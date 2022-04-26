import NIOCore

struct PSQLError: Error {

    enum Base {
        case sslUnsupported
        case failedToAddSSLHandler(underlying: Error)
        case server(PostgresBackendMessage.ErrorResponse)
        case decoding(PSQLDecodingError)
        case unexpectedBackendMessage(PostgresBackendMessage)
        case unsupportedAuthMechanism(PSQLAuthScheme)
        case authMechanismRequiresPassword
        case saslError(underlyingError: Error)
        case invalidCommandTag(String)

        case queryCancelled
        case tooManyParameters
        case connectionQuiescing
        case connectionClosed
        case connectionError(underlying: Error)
        case uncleanShutdown

        case casting(PostgresDecodingError)
    }

    internal var base: Base

    private init(_ base: Base) {
        self.base = base
    }

    static var sslUnsupported: PSQLError {
        Self.init(.sslUnsupported)
    }

    static func failedToAddSSLHandler(underlying error: Error) -> PSQLError {
        Self.init(.failedToAddSSLHandler(underlying: error))
    }

    static func server(_ message: PostgresBackendMessage.ErrorResponse) -> PSQLError {
        Self.init(.server(message))
    }

    static func decoding(_ error: PSQLDecodingError) -> PSQLError {
        Self.init(.decoding(error))
    }

    static func unexpectedBackendMessage(_ message: PostgresBackendMessage) -> PSQLError {
        Self.init(.unexpectedBackendMessage(message))
    }

    static func unsupportedAuthMechanism(_ authScheme: PSQLAuthScheme) -> PSQLError {
        Self.init(.unsupportedAuthMechanism(authScheme))
    }

    static var authMechanismRequiresPassword: PSQLError {
        Self.init(.authMechanismRequiresPassword)
    }

    static func sasl(underlying: Error) -> PSQLError {
        Self.init(.saslError(underlyingError: underlying))
    }

    static func invalidCommandTag(_ value: String) -> PSQLError {
        Self.init(.invalidCommandTag(value))
    }

    static var queryCancelled: PSQLError {
        Self.init(.queryCancelled)
    }

    static var tooManyParameters: PSQLError {
        Self.init(.tooManyParameters)
    }

    static var connectionQuiescing: PSQLError {
        Self.init(.connectionQuiescing)
    }

    static var connectionClosed: PSQLError {
        Self.init(.connectionClosed)
    }

    static func channel(underlying: Error) -> PSQLError {
        Self.init(.connectionError(underlying: underlying))
    }

    static var uncleanShutdown: PSQLError {
        Self.init(.uncleanShutdown)
    }
}

/// An error that may happen when a ``PostgresRow`` or ``PostgresCell`` is decoded to native Swift types.
struct PostgresDecodingError: Error, Equatable {
    public struct Code: Hashable, Error {
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
    }

    /// The casting error code
    public let code: Code

    /// The cell's column name for which the casting failed
    public let columnName: String
    /// The cell's column index for which the casting failed
    public let columnIndex: Int
    /// The swift type the cell should have been casted into
    public let targetType: Any.Type
    /// The cell's postgres data type for which the casting failed
    public let postgresType: PostgresDataType
    /// The cell's postgres format for which the casting failed
    public let postgresFormat: PostgresFormat
    /// A copy of the cell data which was attempted to be casted
    public let postgresData: ByteBuffer?

    /// The file the casting/decoding was attempted in
    public let file: String
    /// The line the casting/decoding was attempted in
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

    @usableFromInline
    static func ==(lhs: PostgresDecodingError, rhs: PostgresDecodingError) -> Bool {
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
  var description: String {
    // This may seem very odd... But we are afraid that users might accidentally send the
    // unfiltered errors out to end-users. This may leak security relevant information. For this
    // reason we overwrite the error description by default to this generic "Database error"
    "Database error"
  }
}
enum PSQLAuthScheme {
    case none
    case kerberosV5
    case md5
    case plaintext
    case scmCredential
    case gss
    case sspi
    case sasl(mechanisms: [String])
}
