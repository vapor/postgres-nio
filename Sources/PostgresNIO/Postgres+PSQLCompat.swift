import NIOCore

extension PSQLError {
    func toPostgresError() -> Error {
        switch self.base {
        case .server(let errorMessage):
            var fields = [PostgresMessage.Error.Field: String]()
            fields.reserveCapacity(errorMessage.fields.count)
            errorMessage.fields.forEach { (key, value) in
                fields[PostgresMessage.Error.Field(rawValue: key.rawValue)!] = value
            }
            return PostgresError.server(PostgresMessage.Error(fields: fields))
        case .sslUnsupported:
            return PostgresError.protocol("Server does not support TLS")
        case .failedToAddSSLHandler(underlying: let underlying):
            return underlying
        case .decoding(let decodingError):
            return PostgresError.protocol("Error decoding message: \(decodingError)")
        case .unexpectedBackendMessage(let message):
            return PostgresError.protocol("Unexpected message: \(message)")
        case .unsupportedAuthMechanism(let authScheme):
            return PostgresError.protocol("Unsupported auth scheme: \(authScheme)")
        case .authMechanismRequiresPassword:
            return PostgresError.protocol("Unable to authenticate without password")
        case .saslError(underlyingError: let underlying):
            return underlying
        case .tooManyParameters, .invalidCommandTag:
            return self
        case .connectionQuiescing:
            return PostgresError.connectionClosed
        case .connectionClosed:
            return PostgresError.connectionClosed
        case .connectionError(underlying: let underlying):
            return underlying
        case .casting(let castingError):
            return castingError
        case .uncleanShutdown:
            return PostgresError.protocol("Unexpected connection close")
        }
    }
}

extension PostgresFormat {
    init(psqlFormatCode: PostgresFormat) {
        switch psqlFormatCode {
        case .binary:
            self = .binary
        case .text:
            self = .text
        }
    }
}

extension Error {
    internal var asAppropriatePostgresError: Error {
        if let psqlError = self as? PSQLError {
            return psqlError.toPostgresError()
        } else {
            return self
        }
    }
}
