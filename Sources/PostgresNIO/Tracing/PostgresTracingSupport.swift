import Atomics
import Logging
import NIOCore
import Tracing

struct PostgresTracingConnectionInfo: Sendable {
    static let dbSystemName = "postgresql"

    let namespace: String?
    let serverAddress: String?
    let serverPort: Int?
    let peerAddress: String?
    let peerPort: Int?

    init(configuration: PostgresConnection.InternalConfiguration) {
        self.namespace = Self.namespace(database: configuration.database, username: configuration.username)

        switch configuration.connection {
        case .resolved(let address):
            let parts = Self.socketAddressComponents(address)
            self.serverAddress = parts.address
            self.serverPort = parts.port
            self.peerAddress = parts.address
            self.peerPort = parts.port

        case .unresolvedTCP(let host, let port):
            self.serverAddress = host
            self.serverPort = port
            self.peerAddress = host
            self.peerPort = port

        case .unresolvedUDS(let path):
            self.serverAddress = path
            self.serverPort = nil
            self.peerAddress = path
            self.peerPort = nil

        case .bootstrapped(let channel):
            let parts = channel.remoteAddress.map(Self.socketAddressComponents)
            self.serverAddress = parts?.address
            self.serverPort = parts?.port
            self.peerAddress = parts?.address
            self.peerPort = parts?.port
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    init(configuration: PostgresClient.Configuration) {
        self.namespace = Self.namespace(database: configuration.database, username: configuration.username)

        switch configuration.endpointInfo {
        case .connectTCP(let host, let port):
            self.serverAddress = host
            self.serverPort = port
            self.peerAddress = host
            self.peerPort = port

        case .bindUnixDomainSocket(let path):
            self.serverAddress = path
            self.serverPort = nil
            self.peerAddress = path
            self.peerPort = nil
        }
    }

    private static func namespace(database: String?, username: String?) -> String? {
        if let database, !database.isEmpty {
            return database
        }
        return username
    }

    private static func socketAddressComponents(_ address: SocketAddress) -> (address: String, port: Int?) {
        let raw = String(describing: address)
        if let port = address.port {
            let suffix = ":\(port)"
            if raw.hasSuffix(suffix) {
                return (String(raw.dropLast(suffix.count)), port)
            }
            return (raw, port)
        }
        return (raw, nil)
    }
}

enum PostgresTraceOperation: Sendable {
    case query(PostgresQuery, safeQueryText: String?, exactSummary: PostgresTraceSummary?)
    case prepare(sql: String)
    case executePrepared(sql: String, bindCount: Int)
    case deallocate(statementName: String)
    case transaction

    var isTransaction: Bool {
        if case .transaction = self {
            return true
        } else {
            return false
        }
    }
}

extension PostgresTraceOperation {
    static func userQuery(_ query: PostgresQuery) -> Self {
        .query(query, safeQueryText: nil, exactSummary: nil)
    }

    static func libraryQuery(
        _ query: PostgresQuery,
        safeQueryText: String,
        exactSummary: PostgresTraceSummary? = nil
    ) -> Self {
        .query(query, safeQueryText: safeQueryText, exactSummary: exactSummary)
    }

    static func preparedExecution(sql: String, bindCount: Int) -> Self {
        .executePrepared(sql: sql, bindCount: bindCount)
    }
}

struct PostgresTraceSummary: Sendable {
    let operationName: String?
    let querySummary: String?
    let collectionName: String?
    let storedProcedureName: String?

    static let none = Self(
        operationName: nil,
        querySummary: nil,
        collectionName: nil,
        storedProcedureName: nil
    )

    static let prepare = Self(
        operationName: "PREPARE",
        querySummary: "PREPARE",
        collectionName: nil,
        storedProcedureName: nil
    )

    static let deallocate = Self(
        operationName: "DEALLOCATE",
        querySummary: "DEALLOCATE",
        collectionName: nil,
        storedProcedureName: nil
    )

    static func make(
        for operation: PostgresTraceOperation,
        policy: PostgresTracingConfiguration.StatementMetadataPolicy
    ) -> Self {
        switch policy {
        case .exact:
            return self.makeExact(for: operation)
        case .inferred:
            return self.makeInferred(for: operation)
        case .disabled:
            return .none
        }
    }

    private static func makeExact(for operation: PostgresTraceOperation) -> Self {
        switch operation {
        case .query(_, _, let exactSummary):
            return exactSummary ?? .none

        case .executePrepared:
            return .none

        case .prepare:
            return .prepare

        case .deallocate:
            return .deallocate

        case .transaction:
            return .none
        }
    }

    private static func makeInferred(for operation: PostgresTraceOperation) -> Self {
        switch operation {
        case .query(let query, _, let exactSummary):
            return exactSummary ?? Self.inferredFromSQL(query.sql)

        case .executePrepared(let sql, _):
            return Self.inferredFromSQL(sql)

        case .prepare:
            return .prepare

        case .deallocate:
            return .deallocate

        case .transaction:
            return .none
        }
    }

    private static func inferredFromSQL(_ sql: String) -> Self {
        let operationName = sql
            .drop(while: \.isWhitespace)
            .prefix(while: \.isLetter)
            .uppercased()
        guard !operationName.isEmpty else {
            return .none
        }
        return .init(operationName: operationName, querySummary: nil, collectionName: nil, storedProcedureName: nil)
    }
}

struct PostgresTraceMetadata: Sendable {
    let spanName: String
    let spanKind: SpanKind
    let attributes: SpanAttributes

    init(
        operation: PostgresTraceOperation,
        configuration: PostgresTracingConfiguration,
        connectionInfo: PostgresTracingConnectionInfo
    ) {
        let summary = PostgresTraceSummary.make(
            for: operation,
            policy: configuration.statementMetadataPolicy
        )
        var attributes: SpanAttributes = [:]
        attributes["db.system.name"] = PostgresTracingConnectionInfo.dbSystemName
        // Also emit the legacy experimental attribute so backends like Datadog that have
        // not yet migrated to the stable db.system.name attribute can still classify the
        // span as a database span and show the database icon.
        attributes["db.system"] = PostgresTracingConnectionInfo.dbSystemName

        if let namespace = connectionInfo.namespace {
            attributes["db.namespace"] = namespace
        }
        if let serverAddress = connectionInfo.serverAddress {
            attributes["server.address"] = serverAddress
        }
        if let serverPort = connectionInfo.serverPort {
            attributes["server.port"] = serverPort
        }
        if let peerAddress = connectionInfo.peerAddress {
            attributes["network.peer.address"] = peerAddress
        }
        if let peerPort = connectionInfo.peerPort {
            attributes["network.peer.port"] = peerPort
        }
        if let operationName = summary.operationName {
            attributes["db.operation.name"] = operationName
        }
        if let querySummary = summary.querySummary, !operation.isTransaction {
            attributes["db.query.summary"] = querySummary
        }
        if let collectionName = summary.collectionName {
            attributes["db.collection.name"] = collectionName
        }
        if let storedProcedureName = summary.storedProcedureName {
            attributes["db.stored_procedure.name"] = storedProcedureName
        }
        if let queryText = Self.queryText(for: operation, policy: configuration.queryTextPolicy) {
            attributes["db.query.text"] = queryText
        }

        switch operation {
        case .transaction:
            self.spanName = "postgres.transaction"
            self.spanKind = .internal
        default:
            self.spanName = Self.spanName(summary: summary, connectionInfo: connectionInfo)
            self.spanKind = .client
        }
        self.attributes = attributes
    }

    private static func spanName(
        summary: PostgresTraceSummary,
        connectionInfo: PostgresTracingConnectionInfo
    ) -> String {
        if let querySummary = summary.querySummary {
            return querySummary
        }

        let target = Self.target(summary: summary, connectionInfo: connectionInfo)
        if let operationName = summary.operationName {
            if let target {
                return "\(operationName) \(target)"
            }
            return operationName
        }

        return target ?? PostgresTracingConnectionInfo.dbSystemName
    }

    private static func target(
        summary: PostgresTraceSummary,
        connectionInfo: PostgresTracingConnectionInfo
    ) -> String? {
        if let collectionName = summary.collectionName {
            return collectionName
        }
        if let storedProcedureName = summary.storedProcedureName {
            return storedProcedureName
        }
        if let namespace = connectionInfo.namespace {
            return namespace
        }
        if let serverAddress = connectionInfo.serverAddress {
            if let serverPort = connectionInfo.serverPort {
                return "\(serverAddress):\(serverPort)"
            }
            return serverAddress
        }
        return nil
    }

    private static func queryText(
        for operation: PostgresTraceOperation,
        policy: PostgresTracingConfiguration.QueryTextPolicy
    ) -> String? {
        let sql: String?
        let safeQueryText: String?
        let isParameterized: Bool

        switch operation {
        case .query(let query, let sanitizedQueryText, _):
            sql = query.sql
            safeQueryText = sanitizedQueryText
            isParameterized = query.binds.count > 0
        case .prepare(let statement):
            sql = statement
            safeQueryText = nil
            isParameterized = false
        case .executePrepared(let statement, let bindCount):
            sql = statement
            safeQueryText = nil
            isParameterized = bindCount > 0
        case .deallocate, .transaction:
            sql = nil
            safeQueryText = nil
            isParameterized = false
        }

        guard let sql else {
            return nil
        }

        switch policy {
        case .safe:
            if isParameterized {
                return sql
            }
            return safeQueryText
        case .recordAll:
            return sql
        }
    }
}

final class PostgresTraceSpan: @unchecked Sendable {
    private let span: any Span
    private let errorDetailsPolicy: PostgresTracingConfiguration.ErrorDetailsPolicy
    private let isFinished = ManagedAtomic(false)

    init(
        span: any Span,
        errorDetailsPolicy: PostgresTracingConfiguration.ErrorDetailsPolicy
    ) {
        self.span = span
        self.errorDetailsPolicy = errorDetailsPolicy
    }

    var context: ServiceContext {
        self.span.context
    }

    func withContext<T>(_ body: () throws -> T) rethrows -> T {
        try ServiceContext.$current.withValue(self.span.context) {
            try body()
        }
    }

    func succeed() {
        guard self.finishIfNeeded() else {
            return
        }
        self.span.end()
    }

    func fail(_ error: any Error) {
        guard self.finishIfNeeded() else {
            return
        }
        self.span.updateAttributes { attributes in
            if let psqlError = error as? PSQLError,
               let sqlState = psqlError.serverInfo?[.sqlState] {
                attributes["db.response.status_code"] = sqlState
                attributes["error.type"] = sqlState
            } else {
                attributes["error.type"] = Self.errorType(for: error)
            }
        }
        self.span.recordError(
            error,
            attributes: Self.errorAttributes(for: error, policy: self.errorDetailsPolicy)
        )
        self.span.setStatus(.init(code: .error))
        self.span.end()
    }

    private func finishIfNeeded() -> Bool {
        self.isFinished.compareExchange(
            expected: false,
            desired: true,
            ordering: .relaxed
        ).exchanged
    }

    private static func errorType(for error: any Error) -> String {
        if let postgresError = error as? PSQLError {
            return postgresError.code.description
        }
        return String(reflecting: type(of: error))
    }

    private static func errorAttributes(
        for error: any Error,
        policy: PostgresTracingConfiguration.ErrorDetailsPolicy
    ) -> SpanAttributes {
        guard let exceptionMessage = self.exceptionMessage(for: error, policy: policy) else {
            return [:]
        }
        return ["exception.message": .string(exceptionMessage)]
    }

    private static func exceptionMessage(
        for error: any Error,
        policy: PostgresTracingConfiguration.ErrorDetailsPolicy
    ) -> String? {
        switch policy {
        case .safe:
            return nil

        case .message:
            if let psqlError = error as? PSQLError,
               let message = psqlError.serverInfo?[.message],
               !message.isEmpty {
                return message
            }
            return nil

        case .debugDescription:
            return String(reflecting: error)
        }
    }
}

func enrichTracingError(
    _ error: any Error,
    query: PostgresQuery? = nil,
    file: String,
    line: Int
) -> any Error {
    guard var psqlError = error as? PSQLError else {
        return error
    }

    psqlError.file = file
    psqlError.line = line
    if let query {
        psqlError.query = query
    }
    return psqlError
}

private extension Span {
    func updateAttributes(_ update: (inout SpanAttributes) -> Void) {
        var attributes = self.attributes
        update(&attributes)
        self.attributes = attributes
    }
}

extension PostgresTraceOperation {
    func makeSpan(
        tracer: any Tracer,
        configuration: PostgresTracingConfiguration,
        connectionInfo: PostgresTracingConnectionInfo,
        parentContext: ServiceContext?,
        function: String = #function,
        file fileID: String = #fileID,
        line: UInt = #line
    ) -> PostgresTraceSpan {
        let metadata = PostgresTraceMetadata(
            operation: self,
            configuration: configuration,
            connectionInfo: connectionInfo
        )
        let span = tracer.startSpan(
            metadata.spanName,
            context: parentContext ?? .topLevel,
            ofKind: metadata.spanKind,
            function: function,
            file: fileID,
            line: line
        )
        span.attributes = metadata.attributes
        return PostgresTraceSpan(
            span: span,
            errorDetailsPolicy: configuration.errorDetailsPolicy
        )
    }

    func makeSpan(
        configuration: PostgresTracingConfiguration?,
        connectionInfo: PostgresTracingConnectionInfo,
        parentContext: ServiceContext?,
        function: String = #function,
        file fileID: String = #fileID,
        line: UInt = #line
    ) -> PostgresTraceSpan? {
        guard let configuration,
              let tracer = configuration.resolvedTracer
        else {
            return nil
        }

        return self.makeSpan(
            tracer: tracer,
            configuration: configuration,
            connectionInfo: connectionInfo,
            parentContext: parentContext,
            function: function,
            file: fileID,
            line: line
        )
    }
}
