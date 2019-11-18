import Logging

extension PostgresConnection: PostgresDatabase {
    public func send(
        _ request: PostgresRequest,
        logger: Logger
    ) -> EventLoopFuture<Void> {
        request.log(to: logger)
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let request = PostgresRequestContext(delegate: request, promise: promise)
        self.channel.write(request).cascadeFailure(to: promise)
        self.channel.flush()
        return promise.futureResult
    }
    
    public func withConnection<T>(_ closure: (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        closure(self)
    }
}

public protocol PostgresRequest {
    // return nil to end request
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]?
    func start() throws -> [PostgresMessage]
    func log(to logger: Logger)
}

final class PostgresRequestContext {
    let delegate: PostgresRequest
    let promise: EventLoopPromise<Void>
    var lastError: Error?
    
    init(delegate: PostgresRequest, promise: EventLoopPromise<Void>) {
        self.delegate = delegate
        self.promise = promise
    }
}

final class PostgresRequestHandler: ChannelDuplexHandler {
    typealias InboundIn = PostgresMessage
    typealias OutboundIn = PostgresRequestContext
    typealias OutboundOut = PostgresMessage
    
    private var queue: [PostgresRequestContext]
    let logger: Logger
    
    public init(logger: Logger) {
        self.queue = []
        self.logger = logger
    }
    
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        let message = self.unwrapInboundIn(data)
        guard self.queue.count > 0 else {
            // discard packet
            return
        }
        let request = self.queue[0]
        
        switch message.identifier {
        case .error:
            let error = try PostgresMessage.Error(message: message)
            self.logger.error("\(error)")
            request.lastError = PostgresError.server(error)
        case .notice:
            let notice = try PostgresMessage.Error(message: message)
            self.logger.notice("\(notice)")
        default: break
        }
        
        if let responses = try request.delegate.respond(to: message) {
            for response in responses {
                context.write(self.wrapOutboundOut(response), promise: nil)
            }
            context.flush()
        } else {
            self.queue.removeFirst()
            if let error = request.lastError {
                request.promise.fail(error)
            } else {
                request.promise.succeed(())
            }
        }
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            try self._channelRead(context: context, data: data)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = self.unwrapOutboundIn(data)
        self.queue.append(request)
        do {
            let messages = try request.delegate.start()
            self.write(context: context, items: messages, promise: promise)
            context.flush()
        } catch {
            promise?.fail(error)
            self.errorCaught(context: context, error: error)
        }
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        for current in self.queue {
            current.promise.fail(PostgresError.connectionClosed)
        }
        self.queue = []
        context.close(mode: mode, promise: promise)
    }
}


extension ChannelInboundHandler {
    func write(context: ChannelHandlerContext, items: [OutboundOut], promise: EventLoopPromise<Void>?) {
        var items = items
        if let last = items.popLast() {
            for item in items {
                context.write(self.wrapOutboundOut(item), promise: nil)
            }
            context.write(self.wrapOutboundOut(last), promise: promise)
        } else {
            promise?.succeed(())
        }
    }
}
