extension PostgresConnection {
    public func requestTLS(using tlsConfig: TLSConfiguration) -> EventLoopFuture<Bool> {
        let promise = self.channel.eventLoop.makePromise(of: Bool.self)
        let handler = RequestTLSHandler(tlsConfig: tlsConfig, promise: promise)
        return self.channel.pipeline.add(handler: handler).then {
            return promise.futureResult
        }
    }
    
    // MARK: Private
    
    private final class RequestTLSHandler: PostgresConnectionHandler {
        let tlsConfig: TLSConfiguration
        var promise: EventLoopPromise<Bool>
        
        init(tlsConfig: TLSConfiguration, promise: EventLoopPromise<Bool>) {
            self.tlsConfig = tlsConfig
            self.promise = promise
        }
        
        func read(message: inout PostgresMessage, ctx: ChannelHandlerContext) throws {
            switch message.identifier {
            case .sslSupported:
                let sslContext = try SSLContext(configuration: self.tlsConfig)
                let handler = try OpenSSLClientHandler(context: sslContext)
                _ = ctx.channel.pipeline.add(handler: handler, first: true)
                self.promise.succeed(result: true)
            case .sslUnsupported:
                self.promise.succeed(result: false)
            default: throw PostgresError(.protocol("Unexpected message during TLS request: \(message)"))
            }
            ctx.channel.pipeline.remove(handler: self, promise: nil)
        }
        
        func errorCaught(ctx: ChannelHandlerContext, error: Error) {
            ctx.close(mode: .all, promise: nil)
            self.promise.fail(error: error)
        }
        
        func handlerAdded(ctx: ChannelHandlerContext) {
            ctx.write(message: PostgresMessage.SSLRequest(), promise: nil)
            ctx.flush()
        }
    }
}

