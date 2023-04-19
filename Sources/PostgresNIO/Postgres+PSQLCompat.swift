import NIOCore

extension PSQLError {
    func toPostgresError() -> Error {
        switch self.code.base {
        case .queryCancelled:
            return self
        case .server:
            guard let serverInfo = self.serverInfo else {
                return self
            }

            var fields = [PostgresMessage.Error.Field: String]()
            fields.reserveCapacity(serverInfo.underlying.fields.count)
            serverInfo.underlying.fields.forEach { (key, value) in
                fields[PostgresMessage.Error.Field(rawValue: key.rawValue)!] = value
            }
            return PostgresError.server(PostgresMessage.Error(fields: fields))
        case .sslUnsupported:
            return PostgresError.protocol("Server does not support TLS")
        case .failedToAddSSLHandler:
            return self.underlying ?? self
        case .messageDecodingFailure:
            return PostgresError.protocol("Error decoding message: \(String(describing: self.underlying))")
        case .unexpectedBackendMessage:
            return PostgresError.protocol("Unexpected message: \(String(describing: self.backendMessage))")
        case .unsupportedAuthMechanism:
            return PostgresError.protocol("Unsupported auth scheme: \(String(describing: self.unsupportedAuthScheme))")
        case .authMechanismRequiresPassword:
            return PostgresError.protocol("Unable to authenticate without password")
        case .saslError:
            return self.underlying ?? self
        case .tooManyParameters, .invalidCommandTag:
            return self
        case .connectionQuiescing:
            return PostgresError.connectionClosed
        case .connectionClosed:
            return PostgresError.connectionClosed
        case .connectionError:
            return self.underlying ?? self
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
