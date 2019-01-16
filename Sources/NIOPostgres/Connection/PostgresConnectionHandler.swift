extension PostgresConnection {
    public func send(_ request: PostgresConnectionRequest) -> EventLoopFuture<Void> {
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let request = PostgresConnectionRequestContext(delegate: request, promise: promise)
        self.channel.write(request).cascadeFailure(promise: promise)
        self.channel.flush()
        return promise.futureResult
    }
}

public protocol PostgresConnectionRequest {
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]?
    func start() throws -> [PostgresMessage]
}

// MARK: Private

final class PostgresConnectionRequestContext {
    let delegate: PostgresConnectionRequest
    let promise: EventLoopPromise<Void>
    var error: Error?
    
    init(delegate: PostgresConnectionRequest, promise: EventLoopPromise<Void>) {
        self.delegate = delegate
        self.promise = promise
    }
}

final class PostgresConnectionHandler: ChannelDuplexHandler {
    typealias InboundIn = PostgresMessage
    typealias OutboundIn = PostgresConnectionRequestContext
    typealias OutboundOut = PostgresMessage
    
    private var queue: [PostgresConnectionRequestContext]
    
    public init() {
        self.queue = []
    }
    
    private func _channelRead(ctx: ChannelHandlerContext, data: NIOAny) throws {
        let message = self.unwrapInboundIn(data)
        guard self.queue.count > 0 else {
            assertionFailure("PostgresRequest queue empty, discarded: \(message)")
            return
        }
        let request = self.queue[0]
        
        switch message.identifier {
        case .error:
            let error = try PostgresMessage.Error(message: message)
            request.error = PostgresError(.server(error))
        case .notice:
            let notice = try PostgresMessage.Error(message: message)
            print("[NIOPostgres] [NOTICE] \(notice)")
        default:
            if let responses = try request.delegate.respond(to: message) {
                for response in responses {
                    ctx.write(self.wrapOutboundOut(response), promise: nil)
                }
                ctx.flush()
            } else {
                self.queue.removeFirst()
                if let error = request.error {
                    request.promise.fail(error: error)
                } else {
                    request.promise.succeed(result: ())
                }
            }
        }
    }
    
    private func _write(ctx: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) throws {
        let request = self.unwrapOutboundIn(data)
        self.queue.append(request)
        let messages = try request.delegate.start()
        for message in messages {
            ctx.write(self.wrapOutboundOut(message), promise: nil)
        }
        ctx.flush()
    }
    
    func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
        do {
            try self._channelRead(ctx: ctx, data: data)
        } catch {
            self.errorCaught(ctx: ctx, error: error)
        }
    }
    
    func write(ctx: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        do {
            try self._write(ctx: ctx, data: data, promise: promise)
        } catch {
            self.errorCaught(ctx: ctx, error: error)
        }
    }
    
    func errorCaught(ctx: ChannelHandlerContext, error: Error) {
        guard self.queue.count > 0 else {
            assertionFailure("PostgresRequest queue empty, discarded: \(error)")
            return
        }
        self.queue[0].promise.fail(error: error)
        ctx.close(mode: .all, promise: nil)
    }
}
