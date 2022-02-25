import NIOCore

struct PSQLError: Error {
    
    enum Base {
        case sslUnsupported
        case failedToAddSSLHandler(underlying: Error)
        case server(PSQLBackendMessage.ErrorResponse)
        case decoding(PSQLDecodingError)
        case unexpectedBackendMessage(PSQLBackendMessage)
        case unsupportedAuthMechanism(PSQLAuthScheme)
        case authMechanismRequiresPassword
        case saslError(underlyingError: Error)
        
        case tooManyParameters
        case connectionQuiescing
        case connectionClosed
        case connectionError(underlying: Error)
        case uncleanShutdown
        
        case casting(PostgresCastingError)
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
    
    static func server(_ message: PSQLBackendMessage.ErrorResponse) -> PSQLError {
        Self.init(.server(message))
    }
    
    static func decoding(_ error: PSQLDecodingError) -> PSQLError {
        Self.init(.decoding(error))
    }
    
    static func unexpectedBackendMessage(_ message: PSQLBackendMessage) -> PSQLError {
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
struct PostgresCastingError: Error, Equatable {
    struct Code: Hashable, Error {
        enum Base {
            case missingData
            case typeMismatch
            case failure
        }

        var base: Base

        init(_ base: Base) {
            self.base = base
        }

        static let missingData = Self.init(.missingData)
        static let typeMismatch = Self.init(.typeMismatch)
        static let failure = Self.init(.failure)
    }

    /// The casting error code
    let code: Code

    /// The cell's column name for which the casting failed
    let columnName: String
    /// The cell's column index for which the casting failed
    let columnIndex: Int
    /// The swift type the cell should have been casted into
    let targetType: Any.Type
    /// The cell's postgres data type for which the casting failed
    let postgresType: PostgresDataType
    /// The cell's postgres format for which the casting failed
    let postgresFormat: PostgresFormat
    /// A copy of the cell data which was attempted to be casted
    let postgresData: ByteBuffer?

    /// The file the casting/decoding was attempted in
    let file: String
    /// The line the casting/decoding was attempted in
    let line: Int
    
    var description: String {
        // This may seem very odd... But we are afraid that users might accidentally send the
        // unfiltered errors out to end-users. This may leak security relevant information. For this
        // reason we overwrite the error description by default to this generic "Database error"
        "Database error"
    }
    
    static func ==(lhs: PostgresCastingError, rhs: PostgresCastingError) -> Bool {
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
