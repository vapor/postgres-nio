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

    var code: Code
    
    let columnName: String
    let columnIndex: Int
    let targetType: Any.Type
    let postgresType: PostgresDataType
    let postgresData: ByteBuffer?
    
    var description: String {
        switch self.code.base {
        case .missingData:
            return """
                Failed to cast Postgres data type \(self.postgresType.description) to Swift type \(self.targetType) \
                because of missing data.
                """

        case .typeMismatch:
            preconditionFailure()

        case .failure:
            return """
                Failed to cast Postgres data type \(self.postgresType.description) to Swift type \(self.targetType).
                """
        }

    }
    
    static func ==(lhs: PostgresCastingError, rhs: PostgresCastingError) -> Bool {
        lhs.targetType == rhs.targetType
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
