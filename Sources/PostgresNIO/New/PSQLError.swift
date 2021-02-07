import struct Foundation.Data

struct PSQLError: Error {
    
    enum Underlying {
        case sslUnsupported
        case failedToAddSSLHandler(underlying: Error)
        case server(PSQLBackendMessage.ErrorResponse)
        case decoding(PSQLBackendMessage.DecodingError)
        case unexpectedBackendMessage(PSQLBackendMessage)
        case unsupportedAuthMechanism(PSQLAuthScheme)
        
        case tooManyParameters
        case connectionQuiescing
        case connectionClosed
        case connectionError(underlying: Error)
        
        case casting(PSQLCastingError)
    }
    
    internal var underlying: Underlying
    
    private init(_ underlying: Underlying) {
        self.underlying = underlying
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
    
    static func decoding(_ error: PSQLBackendMessage.DecodingError) -> PSQLError {
        Self.init(.decoding(error))
    }
    
    static func unexpectedBackendMessage(_ message: PSQLBackendMessage) -> PSQLError {
        Self.init(.unexpectedBackendMessage(message))
    }
    
    static func unsupportedAuthMechanism(_ authScheme: PSQLAuthScheme) -> PSQLError {
        Self.init(.unsupportedAuthMechanism(authScheme))
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
    
    static func connection(underlying: Error) -> PSQLError {
        Self.init(.connectionError(underlying: underlying))
    }
}

struct PSQLCastingError: Error {
    
    let columnName: String
    let columnIndex: Int
    
    let file: String
    let line: Int
    
    let targetType: PSQLDecodable.Type
    let postgresType: PSQLDataType
    let postgresData: ByteBuffer?
    
    let description: String
    let underlying: Error?
    
    static func missingData(targetType: PSQLDecodable.Type, type: PSQLDataType, context: PSQLDecodingContext) -> Self {
        PSQLCastingError(
            columnName: context.columnName,
            columnIndex: context.columnIndex,
            file: context.file,
            line: context.line,
            targetType: targetType,
            postgresType: type,
            postgresData: nil,
            description: """
                Failed to cast Postgres data type \(type.description) to Swift type \(targetType) \
                because of missing data in \(context.file) line \(context.line).
                """,
            underlying: nil
        )
    }
    
    static func failure(targetType: PSQLDecodable.Type,
                        type: PSQLDataType,
                        postgresData: ByteBuffer,
                        description: String? = nil,
                        underlying: Error? = nil,
                        context: PSQLDecodingContext) -> Self
    {
        PSQLCastingError(
            columnName: context.columnName,
            columnIndex: context.columnIndex,
            file: context.file,
            line: context.line,
            targetType: targetType,
            postgresType: type,
            postgresData: postgresData,
            description: description ?? """
                Failed to cast Postgres data type \(type.description) to Swift type \(targetType) \
                in \(context.file) line \(context.line)."
                """,
            underlying: underlying
        )
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
    case sasl
}
