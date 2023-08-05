import NIOCore

/// An error that is thrown from the PostgresClient.
public struct PSQLError: Error {

    public struct Code: Sendable, Hashable, CustomStringConvertible {
        enum Base: Sendable, Hashable {
            case sslUnsupported
            case failedToAddSSLHandler
            case receivedUnencryptedDataAfterSSLRequest
            case server
            case messageDecodingFailure
            case unexpectedBackendMessage
            case unsupportedAuthMechanism
            case authMechanismRequiresPassword
            case saslError
            case invalidCommandTag

            case queryCancelled
            case tooManyParameters
            case connectionQuiescing
            case connectionClosed
            case connectionError
            case uncleanShutdown

            case listenFailed
            case unlistenFailed
        }

        internal var base: Base

        private init(_ base: Base) {
            self.base = base
        }

        public static let sslUnsupported = Self.init(.sslUnsupported)
        public static let failedToAddSSLHandler = Self(.failedToAddSSLHandler)
        public static let receivedUnencryptedDataAfterSSLRequest = Self(.receivedUnencryptedDataAfterSSLRequest)
        public static let server = Self(.server)
        public static let messageDecodingFailure = Self(.messageDecodingFailure)
        public static let unexpectedBackendMessage = Self(.unexpectedBackendMessage)
        public static let unsupportedAuthMechanism = Self(.unsupportedAuthMechanism)
        public static let authMechanismRequiresPassword = Self(.authMechanismRequiresPassword)
        public static let saslError = Self.init(.saslError)
        public static let invalidCommandTag = Self(.invalidCommandTag)
        public static let queryCancelled = Self(.queryCancelled)
        public static let tooManyParameters = Self(.tooManyParameters)
        public static let connectionQuiescing = Self(.connectionQuiescing)
        public static let connectionClosed = Self(.connectionClosed)
        public static let connectionError = Self(.connectionError)
        public static let uncleanShutdown = Self.init(.uncleanShutdown)
        public static let listenFailed = Self.init(.listenFailed)
        public static let unlistenFailed = Self.init(.unlistenFailed)

        public var description: String {
            switch self.base {
            case .sslUnsupported:
                return "sslUnsupported"
            case .failedToAddSSLHandler:
                return "failedToAddSSLHandler"
            case .receivedUnencryptedDataAfterSSLRequest:
                return "receivedUnencryptedDataAfterSSLRequest"
            case .server:
                return "server"
            case .messageDecodingFailure:
                return "messageDecodingFailure"
            case .unexpectedBackendMessage:
                return "unexpectedBackendMessage"
            case .unsupportedAuthMechanism:
                return "unsupportedAuthMechanism"
            case .authMechanismRequiresPassword:
                return "authMechanismRequiresPassword"
            case .saslError:
                return "saslError"
            case .invalidCommandTag:
                return "invalidCommandTag"
            case .queryCancelled:
                return "queryCancelled"
            case .tooManyParameters:
                return "tooManyParameters"
            case .connectionQuiescing:
                return "connectionQuiescing"
            case .connectionClosed:
                return "connectionClosed"
            case .connectionError:
                return "connectionError"
            case .uncleanShutdown:
                return "uncleanShutdown"
            case .listenFailed:
                return "listenFailed"
            case .unlistenFailed:
                return "unlistenFailed"
            }
        }
    }

    private var backing: Backing

    private mutating func copyBackingStoriageIfNecessary() {
        if !isKnownUniquelyReferenced(&self.backing) {
            self.backing = self.backing.copy()
        }
    }

    /// The ``PSQLError/Code-swift.struct`` code
    public internal(set) var code: Code {
        get { self.backing.code }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.code = newValue
        }
    }

    /// The info that was received from the server
    public internal(set) var serverInfo: ServerInfo? {
        get { self.backing.serverInfo }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.serverInfo = newValue
        }
    }

    /// The underlying error
    public internal(set) var underlying: Error? {
        get { self.backing.underlying }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.underlying = newValue
        }
    }

    /// The file in which the Postgres operation was triggered that failed
    public internal(set) var file: String? {
        get { self.backing.file }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.file = newValue
        }
    }

    /// The line in which the Postgres operation was triggered that failed
    public internal(set) var line: Int? {
        get { self.backing.line }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.line = newValue
        }
    }

    /// The query that failed
    public internal(set) var query: PostgresQuery? {
        get { self.backing.query }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.query = newValue
        }
    }

    /// the backend message... we should keep this internal but we can use it to print more
    /// advanced debug reasons.
    var backendMessage: PostgresBackendMessage? {
        get { self.backing.backendMessage }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.backendMessage = newValue
        }
    }

    /// the unsupported auth scheme... we should keep this internal but we can use it to print more
    /// advanced debug reasons.
    var unsupportedAuthScheme: UnsupportedAuthScheme? {
        get { self.backing.unsupportedAuthScheme }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.unsupportedAuthScheme = newValue
        }
    }

    /// the invalid command tag... we should keep this internal but we can use it to print more
    /// advanced debug reasons.
    var invalidCommandTag: String? {
        get { self.backing.invalidCommandTag }
        set {
            self.copyBackingStoriageIfNecessary()
            self.backing.invalidCommandTag = newValue
        }
    }

    init(code: Code, query: PostgresQuery, file: String? = nil, line: Int? = nil) {
        self.backing = .init(code: code)
        self.query = query
        self.file = file
        self.line = line
    }

    init(code: Code) {
        self.backing = .init(code: code)
    }

    private final class Backing {
        fileprivate var code: Code
        fileprivate var serverInfo: ServerInfo?
        fileprivate var underlying: Error?
        fileprivate var file: String?
        fileprivate var line: Int?
        fileprivate var query: PostgresQuery?
        fileprivate var backendMessage: PostgresBackendMessage?
        fileprivate var unsupportedAuthScheme: UnsupportedAuthScheme?
        fileprivate var invalidCommandTag: String?

        init(code: Code) {
            self.code = code
        }

        func copy() -> Self {
            let new = Self.init(code: self.code)
            new.serverInfo = self.serverInfo
            new.underlying = self.underlying
            new.file = self.file
            new.line = self.line
            new.query = self.query
            new.backendMessage = self.backendMessage
            return new
        }
    }

    public struct ServerInfo {
        public struct Field: Hashable, Sendable, CustomStringConvertible {
            fileprivate let backing: PostgresBackendMessage.Field

            fileprivate init(_ backing: PostgresBackendMessage.Field) {
                self.backing = backing
            }

            /// Severity: the field contents are ERROR, FATAL, or PANIC (in an error message),
            /// or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message), or a
            /// localized translation of one of these. Always present.
            public static let localizedSeverity = Self(.localizedSeverity)

            /// Severity: the field contents are ERROR, FATAL, or PANIC (in an error message),
            /// or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message).
            /// This is identical to the S field except that the contents are never localized.
            /// This is present only in messages generated by PostgreSQL versions 9.6 and later.
            public static let severity = Self(.severity)

            /// Code: the SQLSTATE code for the error (see Appendix A). Not localizable. Always present.
            public static let sqlState = Self(.sqlState)

            /// Message: the primary human-readable error message. This should be accurate but terse (typically one line).
            /// Always present.
            public static let message = Self(.message)

            /// Detail: an optional secondary error message carrying more detail about the problem.
            /// Might run to multiple lines.
            public static let detail = Self(.detail)

            /// Hint: an optional suggestion what to do about the problem.
            /// This is intended to differ from Detail in that it offers advice (potentially inappropriate)
            /// rather than hard facts. Might run to multiple lines.
            public static let hint = Self(.hint)

            /// Position: the field value is a decimal ASCII integer, indicating an error cursor
            /// position as an index into the original query string. The first character has index 1,
            /// and positions are measured in characters not bytes.
            public static let position = Self(.position)

            /// Internal position: this is defined the same as the P field, but it is used when the
            /// cursor position refers to an internally generated command rather than the one submitted by the client.
            /// The q field will always appear when this field appears.
            public static let internalPosition = Self(.internalPosition)

            /// Internal query: the text of a failed internally-generated command.
            /// This could be, for example, a SQL query issued by a PL/pgSQL function.
            public static let internalQuery = Self(.internalQuery)

            /// Where: an indication of the context in which the error occurred.
            /// Presently this includes a call stack traceback of active procedural language functions and
            /// internally-generated queries. The trace is one entry per line, most recent first.
            public static let locationContext = Self(.locationContext)

            /// Schema name: if the error was associated with a specific database object, the name of
            /// the schema containing that object, if any.
            public static let schemaName = Self(.schemaName)

            /// Table name: if the error was associated with a specific table, the name of the table.
            /// (Refer to the schema name field for the name of the table's schema.)
            public static let tableName = Self(.tableName)

            /// Column name: if the error was associated with a specific table column, the name of the column.
            /// (Refer to the schema and table name fields to identify the table.)
            public static let columnName = Self(.columnName)

            /// Data type name: if the error was associated with a specific data type, the name of the data type.
            /// (Refer to the schema name field for the name of the data type's schema.)
            public static let dataTypeName = Self(.dataTypeName)

            /// Constraint name: if the error was associated with a specific constraint, the name of the constraint.
            /// Refer to fields listed above for the associated table or domain. (For this purpose, indexes are
            /// treated as constraints, even if they weren't created with constraint syntax.)
            public static let constraintName = Self(.constraintName)

            /// File: the file name of the source-code location where the error was reported.
            public static let file = Self(.file)

            /// Line: the line number of the source-code location where the error was reported.
            public static let line = Self(.line)

            /// Routine: the name of the source-code routine reporting the error.
            public static let routine = Self(.routine)

            public var description: String {
                switch self.backing {
                case .localizedSeverity:
                    return "localizedSeverity"
                case .severity:
                    return "severity"
                case .sqlState:
                    return "sqlState"
                case .message:
                    return "message"
                case .detail:
                    return "detail"
                case .hint:
                    return "hint"
                case .position:
                    return "position"
                case .internalPosition:
                    return "internalPosition"
                case .internalQuery:
                    return "internalQuery"
                case .locationContext:
                    return "locationContext"
                case .schemaName:
                    return "schemaName"
                case .tableName:
                    return "tableName"
                case .columnName:
                    return "columnName"
                case .dataTypeName:
                    return "dataTypeName"
                case .constraintName:
                    return "constraintName"
                case .file:
                    return "file"
                case .line:
                    return "line"
                case .routine:
                    return "routine"
                }
            }
        }

        let underlying: PostgresBackendMessage.ErrorResponse

        fileprivate init(_ underlying: PostgresBackendMessage.ErrorResponse) {
            self.underlying = underlying
        }

        /// The detailed server error information. This field is set if the ``PSQLError/code-swift.property`` is
        /// ``PSQLError/Code-swift.struct/server``.
        public subscript(field: Field) -> String? {
            self.underlying.fields[field.backing]
        }
    }

    // MARK: - Internal convenience factory methods -

    static func unexpectedBackendMessage(_ message: PostgresBackendMessage) -> Self {
        var new = Self(code: .unexpectedBackendMessage)
        new.backendMessage = message
        return new
    }

    static func messageDecodingFailure(_ error: PostgresMessageDecodingError) -> Self {
        var new = Self(code: .messageDecodingFailure)
        new.underlying = error
        return new
    }

    static var connectionQuiescing: PSQLError { PSQLError(code: .connectionQuiescing) }

    static var connectionClosed: PSQLError { PSQLError(code: .connectionClosed) }

    static var authMechanismRequiresPassword: PSQLError { PSQLError(code: .authMechanismRequiresPassword) }

    static var sslUnsupported: PSQLError { PSQLError(code: .sslUnsupported) }

    static var queryCancelled: PSQLError { PSQLError(code: .queryCancelled) }

    static var uncleanShutdown: PSQLError { PSQLError(code: .uncleanShutdown) }

    static var receivedUnencryptedDataAfterSSLRequest: PSQLError { PSQLError(code: .receivedUnencryptedDataAfterSSLRequest) }

    static func server(_ response: PostgresBackendMessage.ErrorResponse) -> PSQLError {
        var error = PSQLError(code: .server)
        error.serverInfo = .init(response)
        return error
    }

    static func sasl(underlying: Error) -> PSQLError {
        var error = PSQLError(code: .saslError)
        error.underlying = underlying
        return error
    }

    static func failedToAddSSLHandler(underlying: Error) -> PSQLError {
        var error = PSQLError(code: .failedToAddSSLHandler)
        error.underlying = underlying
        return error
    }

    static func connectionError(underlying: Error) -> PSQLError {
        var error = PSQLError(code: .connectionError)
        error.underlying = underlying
        return error
    }

    static func unsupportedAuthMechanism(_ authScheme: UnsupportedAuthScheme) -> PSQLError {
        var error = PSQLError(code: .unsupportedAuthMechanism)
        error.unsupportedAuthScheme = authScheme
        return error
    }

    static func invalidCommandTag(_ value: String) -> PSQLError {
        var error = PSQLError(code: .invalidCommandTag)
        error.invalidCommandTag = value
        return error
    }

    static func unlistenError(underlying: Error) -> PSQLError {
        var error = PSQLError(code: .unlistenFailed)
        error.underlying = underlying
        return error
    }

    enum UnsupportedAuthScheme {
        case none
        case kerberosV5
        case md5
        case plaintext
        case scmCredential
        case gss
        case sspi
        case sasl(mechanisms: [String])
    }
}

extension PSQLError: CustomStringConvertible {
    public var description: String {
        // This may seem very odd... But we are afraid that users might accidentally send the
        // unfiltered errors out to end-users. This may leak security relevant information. For this
        // reason we overwrite the error description by default to this generic "Database error"
        """
        PSQLError – Generic description to prevent accidental leakage of sensitive data. For debugging details, use `String(reflecting: error)`.
        """
    }
}

extension PSQLError: CustomDebugStringConvertible {
    public var debugDescription: String {
        var result = #"PSQLError(code: \#(self.code)"#

        if let serverInfo = self.serverInfo?.underlying {
            result.append(", serverInfo: [")
            result.append(
                serverInfo.fields
                    .sorted(by: { $0.key.rawValue < $1.key.rawValue })
                    .map { "\(PSQLError.ServerInfo.Field($0.0)): \($0.1)" }
                    .joined(separator: ", ")
            )
            result.append("]")
        }

        if let backendMessage = self.backendMessage {
            result.append(", backendMessage: \(String(reflecting: backendMessage))")
        }

        if let unsupportedAuthScheme = self.unsupportedAuthScheme {
            result.append(", unsupportedAuthScheme: \(unsupportedAuthScheme)")
        }

        if let invalidCommandTag = self.invalidCommandTag {
            result.append(", invalidCommandTag: \(invalidCommandTag)")
        }

        if let underlying = self.underlying {
            result.append(", underlying: \(String(reflecting: underlying))")
        }

        if let file = self.file {
            result.append(", triggeredFromRequestInFile: \(file)")
            if let line = self.line {
                result.append(", line: \(line)")
            }
        }

        if let query = self.query {
            result.append(", query: \(String(reflecting: query))")
        }

        result.append(")")

        return result
    }
}

/// An error that may happen when a ``PostgresRow`` or ``PostgresCell`` is decoded to native Swift types.
public struct PostgresDecodingError: Error, Equatable {
    public struct Code: Hashable, Error, CustomStringConvertible {
        enum Base {
            case missingData
            case typeMismatch
            case failure
        }

        var base: Base

        init(_ base: Base) {
            self.base = base
        }

        public static let missingData = Self.init(.missingData)
        public static let typeMismatch = Self.init(.typeMismatch)
        public static let failure = Self.init(.failure)
        
        public var description: String {
            switch self.base {
            case .missingData:
                return "missingData"
            case .typeMismatch:
                return "typeMismatch"
            case .failure:
                return "failure"
            }
        }
    }

    /// The decoding error code
    public let code: Code

    /// The cell's column name for which the decoding failed
    public let columnName: String
    /// The cell's column index for which the decoding failed
    public let columnIndex: Int
    /// The swift type the cell should have been decoded into
    public let targetType: Any.Type
    /// The cell's postgres data type for which the decoding failed
    public let postgresType: PostgresDataType
    /// The cell's postgres format for which the decoding failed
    public let postgresFormat: PostgresFormat
    /// A copy of the cell data which was attempted to be decoded
    public let postgresData: ByteBuffer?

    /// The file the decoding was attempted in
    public let file: String
    /// The line the decoding was attempted in
    public let line: Int

    @usableFromInline
    init(
        code: Code,
        columnName: String,
        columnIndex: Int,
        targetType: Any.Type,
        postgresType: PostgresDataType,
        postgresFormat: PostgresFormat,
        postgresData: ByteBuffer?,
        file: String,
        line: Int
    ) {
        self.code = code
        self.columnName = columnName
        self.columnIndex = columnIndex
        self.targetType = targetType
        self.postgresType = postgresType
        self.postgresFormat = postgresFormat
        self.postgresData = postgresData
        self.file = file
        self.line = line
    }

    public static func ==(lhs: PostgresDecodingError, rhs: PostgresDecodingError) -> Bool {
        return lhs.code == rhs.code
            && lhs.columnName == rhs.columnName
            && lhs.columnIndex == rhs.columnIndex
            && lhs.targetType == rhs.targetType
            && lhs.postgresType == rhs.postgresType
            && lhs.postgresFormat == rhs.postgresFormat
            && lhs.postgresData == rhs.postgresData
            && lhs.file == rhs.file
            && lhs.line == rhs.line
    }
}

extension PostgresDecodingError: CustomStringConvertible {
    public var description: String {
        // This may seem very odd... But we are afraid that users might accidentally send the
        // unfiltered errors out to end-users. This may leak security relevant information. For this
        // reason we overwrite the error description by default to this generic "Database error"
        """
        PostgresDecodingError – Generic description to prevent accidental leakage of sensitive data. For debugging details, use `String(reflecting: error)`.
        """
    }
}

extension PostgresDecodingError: CustomDebugStringConvertible {
    public var debugDescription: String {
        var result = #"PostgresDecodingError(code: \#(self.code)"#
        
        result.append(#", columnName: \#(String(reflecting: self.columnName))"#)
        result.append(#", columnIndex: \#(self.columnIndex)"#)
        result.append(#", targetType: \#(String(reflecting: self.targetType))"#)
        result.append(#", postgresType: \#(self.postgresType)"#)
        result.append(#", postgresFormat: \#(self.postgresFormat)"#)
        if let postgresData = self.postgresData {
            result.append(#", postgresData: \#(String(reflecting: postgresData))"#)
        }
        result.append(#", file: \#(self.file)"#)
        result.append(#", line: \#(self.line)"#)
        result.append(")")

        return result
    }
}

