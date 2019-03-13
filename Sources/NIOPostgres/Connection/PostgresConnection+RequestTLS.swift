import NIOSSL

extension PostgresConnection {
    public func requestTLS(using tlsConfig: TLSConfiguration, serverHostname: String?) -> EventLoopFuture<Bool> {
        let tls = RequestTLSQuery()
        return self.send(tls).flatMapThrowing { _ in
            if tls.isSupported {
                let sslContext = try NIOSSLContext(configuration: tlsConfig)
                let handler = try NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname)
                _ = self.channel.pipeline.addHandler(handler, position: .first)
            }
            return tls.isSupported
        }
    }
}

// MARK: Private

private final class RequestTLSQuery: PostgresRequestHandler {
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
        default: throw PostgresError.protocol("Unexpected message during TLS request: \(message)")
        }
    }
    
    func start() throws -> [PostgresMessage] {
        return try [
            PostgresMessage.SSLRequest().message()
        ]
    }
}

