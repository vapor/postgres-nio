extension PostgresConnection {
    public func requestTLS(using tlsConfig: TLSConfiguration) -> EventLoopFuture<Bool> {
        let tls = RequestTLSQuery()
        return self.send(tls).thenThrowing { _ in
            if tls.isSupported {
                let sslContext = try SSLContext(configuration: tlsConfig)
                let handler = try OpenSSLClientHandler(context: sslContext)
                _ = self.channel.pipeline.add(handler: handler, first: true)
            }
            return tls.isSupported
        }
    }
}

// MARK: Private

private final class RequestTLSQuery: PostgresConnectionRequest {
    var isSupported: Bool
    
    init() {
        self.isSupported = false
    }
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        switch message.identifier {
        case .sslSupported:
            self.isSupported = true
            return nil
        case .sslUnsupported:
            self.isSupported = false
            return nil
        default: throw PostgresError(.protocol("Unexpected message during TLS request: \(message)"))
        }
    }
    
    func start() throws -> [PostgresMessage] {
        return try [
            PostgresMessage.SSLRequest().message()
        ]
    }
}

