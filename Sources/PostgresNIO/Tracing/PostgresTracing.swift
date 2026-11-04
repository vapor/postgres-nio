import NIOCore
import Tracing

@usableFromInline
struct PostgresTracing: Sendable {
    private let tracer: any Tracer
    private let recordsQueryText: Bool
    private let recordsReturnedRows: Bool
    private let names: PostgresTracingConfiguration.AttributeNames
    /// `db.system.name`, `db.namespace`, `server.address` and `server.port`, which the transaction span reports.
    private let scopeAttributes: SpanAttributes
    /// ``scopeAttributes`` plus `network.peer.address` and `network.peer.port`, which every other span reports.
    private let baseAttributes: SpanAttributes
    private let target: String

    /// Creates the state a connection traces with, or `nil` when tracing is disabled.
    init?(configuration: PostgresConnection.InternalConfiguration, remoteAddress: SocketAddress?) {
        let tracing = configuration.options.tracing
        guard let tracer = tracing.tracer, !(tracer is NoOpTracer) else { return nil }
        self.tracer = tracer
        self.recordsQueryText = tracing.recordsQueryText
        self.recordsReturnedRows = tracing.recordsReturnedRows
        let names = tracing.attributeNames
        self.names = names

        var attributes = SpanAttributes()
        attributes[names.databaseSystemName] = "postgresql"
        attributes[names.databaseNamespace] = configuration.database

        // `server.*` describes what the user asked to connect to, `network.peer.*` what we ended up
        // talking to
        var endpoint: String?
        func appendServer(address: String, port: Int?) {
            attributes[names.serverAddress] = address
            // The conventions only ask for the port when it isn't the default one.
            if let port, port != 5432 {
                attributes[names.serverPort] = port
            }
            endpoint = port.map { "\(address):\($0)" } ?? address
        }
        switch configuration.connection {
        case .unresolvedTCP(let host, let port):
            appendServer(address: host, port: port)
        case .unresolvedUDS(let path):
            appendServer(address: path, port: nil)
        case .resolved(let socketAddress):
            if let address = socketAddress.ipAddress ?? socketAddress.pathname {
                appendServer(address: address, port: socketAddress.port)
            }
        case .bootstrapped:
            if let remoteAddress, let address = remoteAddress.ipAddress ?? remoteAddress.pathname {
                appendServer(address: address, port: remoteAddress.port)
            }
        }
        self.scopeAttributes = attributes

        if let remoteAddress, let address = remoteAddress.ipAddress ?? remoteAddress.pathname {
            attributes[names.networkPeerAddress] = address
            attributes[names.networkPeerPort] = remoteAddress.port
        }
        self.baseAttributes = attributes
        self.target = configuration.database ?? endpoint ?? "postgresql"
    }

    /// Computes the span name following the OTel database span naming fallback chain:
    ///
    /// 1. `{db.query.summary}`, if the caller supplied one
    /// 2. `{db.operation.name} {target}`, if a low-cardinality operation name is known
    /// 3. `{target}`, being the first available of `db.collection.name`, `db.namespace`
    ///    or `server.address:server.port`
    /// 4. `{db.system.name}` (`postgresql`)
    ///
    /// The query text is never inspected, as mandated by the spec.
    private func spanName(summary: String?, operation: String?, collection: String?) -> String {
        if let summary {
            return summary
        }
        let target = collection ?? self.target
        if let operation {
            return "\(operation) \(target)"
        }
        return target
    }

    @usableFromInline
    func startSpan(
        queryText: String,
        summary: String? = nil,
        operation: String? = nil,
        collection: String? = nil
    ) -> any Span {
        let span = self.tracer.startSpan(
            self.spanName(summary: summary, operation: operation, collection: collection),
            ofKind: .client
        )
        span.updateAttributes { attributes in
            attributes.merge(self.baseAttributes)
            attributes[self.names.databaseQuerySummary] = summary
            attributes[self.names.databaseOperationName] = operation
            attributes[self.names.databaseCollectionName] = collection
            if self.recordsQueryText {
                attributes[self.names.databaseQueryText] = queryText
            }
        }
        return span
    }

    func startTransactionSpan() -> any Span {
        let span = self.tracer.startSpan("TRANSACTION", ofKind: .internal)
        span.attributes.merge(self.scopeAttributes)
        return span
    }

    /// Records `error.type`, `db.response.status_code` and the span status for a failed operation.
    ///
    /// The error itself is not recorded on the span: Postgres interpolates user data into the error.
    @usableFromInline
    func recordError(_ error: any Error, in span: (any Span)?) {
        guard let span else { return }
        let psqlError = error as? PSQLError
        span.updateAttributes { attributes in
            attributes[self.names.errorType] = psqlError?.tracingErrorType ?? "_OTHER"
            attributes[self.names.databaseResponseStatusCode] = psqlError?.tracingStatusCode
        }
        span.setStatus(.init(code: .error))
    }

    @usableFromInline
    func recordReturnedRows(of rows: PostgresRowSequence, in span: (any Span)?) {
        guard self.recordsReturnedRows else { return }
        span?.attributes[self.names.databaseResponseReturnedRows] = rows.stream.receivedRowCount.load(ordering: .relaxed)
    }
}

extension PSQLError {
    /// The value to report as `error.type`: `cancelled` for cancelled queries, otherwise the
    /// ``tracingStatusCode``. `nil` when the error carries no recognisable code.
    ///
    /// A cancelled query is not a server error, so a user may want to filter these out.
    var tracingErrorType: String? {
        self.code == .queryCancelled ? "cancelled" : self.tracingStatusCode
    }

    /// The SQLSTATE to report as `db.response.status_code`, or `nil` when the error didn't come from the server.
    ///
    /// A SQLSTATE is accepted only when it looks like one, five upper-case ASCII letters or digits such as
    /// `42P01`, so that a server returning unusual text cannot inflate the cardinality of the attribute.
    var tracingStatusCode: String? {
        guard self.code == .server, let sqlState = self.serverInfo?[.sqlState] else {
            return nil
        }
        guard sqlState.count == 5, sqlState.allSatisfy({ $0.isASCII && ($0.isUppercase || $0.isNumber) }) else {
            return nil
        }
        return sqlState
    }
}
