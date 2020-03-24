import NIOSSL
import Logging

extension PostgresConnection {
    internal func requestTLS(
        using tlsConfig: TLSConfiguration,
        serverHostname: String?,
        logger: Logger?
    ) -> EventLoopFuture<Void> {
        let tls = RequestTLSQuery()
        return self.send(tls, logger: logger).flatMapThrowing { _ in
            guard tls.isSupported else {
                throw PostgresError.protocol("Server does not support TLS")
            }
            let sslContext = try NIOSSLContext(configuration: tlsConfig)
            let handler = try NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname)
            _ = self.channel.pipeline.addHandler(handler, position: .first)
        }
    }
}

// MARK: Private

private final class RequestTLSQuery: PostgresRequest {
    var isSupported: Bool
    
    init() {
        self.isSupported = false
    }
    
    func log(to logger: Logger?) {
        logger?.debug("Requesting TLS")
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

