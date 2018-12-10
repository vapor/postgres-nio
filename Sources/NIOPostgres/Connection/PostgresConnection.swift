import NIO
import NIOOpenSSL

public final class PostgresConnection {
    let handler: InboundHandler
    
    #warning("publicize these values?")
    public var status: [String: String]
    var processID: Int32?
    var secretKey: Int32?
    
    var tableNames: TableNames?
    
    public var eventLoop: EventLoop {
        return self.handler.channel.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return handler.channel.closeFuture
    }
    
    init(_ handler: InboundHandler) {
        self.handler = handler
        self.status = [:]
    }
    
    public func close() -> EventLoopFuture<Void> {
        return handler.channel.close(mode: .all)
    }
}

extension PostgresConnection {
    public func requestTLS(using tlsConfiguration: TLSConfiguration) -> EventLoopFuture<Bool> {
        var sslResponse: PostgresMessage.SSLResponse?
        return self.handler.send([.sslRequest(.init())]) { message in
            switch message {
            case .sslResponse(let res):
                sslResponse = res
                return true
            default: fatalError("Unexpected message during TLS request: \(message)")
            }
        }.then {
            guard let res = sslResponse else {
                fatalError("SSL response should not be nil")
            }
            switch res {
            case .supported:
                let sslContext = try! SSLContext(configuration: tlsConfiguration)
                let handler = try! OpenSSLClientHandler(context: sslContext)
                return self.handler.channel.pipeline.add(handler: handler, first: true).map { true }
            case .unsupported:
                return self.eventLoop.makeSucceededFuture(result: false)
            }
        }
    }
}
