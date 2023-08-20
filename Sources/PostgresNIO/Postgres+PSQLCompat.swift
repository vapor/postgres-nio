import NIOCore

extension PSQLError {
    func toPostgresError() -> Error {
        switch self.code.base {
        case .queryCancelled:
            return self
        case .server, .listenFailed:
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
            let message = self.underlying != nil ? String(describing: self.underlying!) : "no message"
            return PostgresError.protocol("Error decoding message: \(message)")
        case .unexpectedBackendMessage:
            let message = self.backendMessage != nil ? String(describing: self.backendMessage!) : "no message"
            return PostgresError.protocol("Unexpected message: \(message)")
        case .unsupportedAuthMechanism:
            let message = self.unsupportedAuthScheme != nil ? String(describing: self.unsupportedAuthScheme!) : "no scheme"
            return PostgresError.protocol("Unsupported auth scheme: \(message)")
        case .authMechanismRequiresPassword:
            return PostgresError.protocol("Unable to authenticate without password")
        case .receivedUnencryptedDataAfterSSLRequest:
            return PostgresError.protocol("Received unencrypted data after SSL request")
        case .saslError:
            return self.underlying ?? self
        case .tooManyParameters, .invalidCommandTag:
            return self
        case .clientClosedConnection,
             .serverClosedConnection:
            return PostgresError.connectionClosed
        case .connectionError:
            return self.underlying ?? self
        case .unlistenFailed:
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
