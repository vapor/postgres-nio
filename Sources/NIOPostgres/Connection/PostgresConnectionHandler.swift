import Logging

extension PostgresConnection {
    public func send(_ request: PostgresRequestHandler) -> EventLoopFuture<Void> {
        request.log(to: self.logger)
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let request = PostgresRequest(delegate: request, promise: promise)
        self.channel.write(request).cascadeFailure(to: promise)
        self.channel.flush()
        return promise.futureResult
    }
}

public protocol PostgresRequestHandler {
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]?
    func start() throws -> [PostgresMessage]
    func log(to logger: Logger)

    #warning("TODO: Workaround for Authentication see #14")
    var errorMessageIsFinal: Bool { get }
}


extension PostgresRequestHandler {
    var errorMessageIsFinal: Bool {
        return false
    }
}

// MARK: Private

final class PostgresRequest {
    let delegate: PostgresRequestHandler
    let promise: EventLoopPromise<Void>
    var error: Error?
    
    init(delegate: PostgresRequestHandler, promise: EventLoopPromise<Void>) {
        self.delegate = delegate
        self.promise = promise
    }
}

final class PostgresConnectionHandler: ChannelDuplexHandler {
    typealias InboundIn = PostgresMessage
    typealias OutboundIn = PostgresRequest
    typealias OutboundOut = PostgresMessage
    
    private var queue: [PostgresRequest]
    
    public init() {
        self.queue = []
    }
    
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        let message = self.unwrapInboundIn(data)
        guard self.queue.count > 0 else {
            assertionFailure("PostgresRequest queue empty, discarded: \(message)")
            return
        }
        let request = self.queue[0]
        
        switch message.identifier {
        case .error:
            let error = try PostgresMessage.Error(message: message)
            let postgresError = PostgresError.server(error)
            if request.delegate.errorMessageIsFinal {
                request.promise.fail(postgresError)
                self.queue.removeFirst()
            }
            request.error = postgresError
        case .notice:
            let notice = try PostgresMessage.Error(message: message)
            print("[NIOPostgres] [NOTICE] \(notice)")
        default:
            if let responses = try request.delegate.respond(to: message) {
                for response in responses {
                    context.write(self.wrapOutboundOut(response), promise: nil)
                }
                context.flush()
            } else {
                self.queue.removeFirst()
                if let error = request.error {
                    request.promise.fail(error)
                } else {
                    request.promise.succeed(())
                }
            }
        }
    }
    
    private func _write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) throws {
        let request = self.unwrapOutboundIn(data)
        self.queue.append(request)
        let messages = try request.delegate.start()
        for message in messages {
            context.write(self.wrapOutboundOut(message), promise: nil)
        }
        context.flush()
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            try self._channelRead(context: context, data: data)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        do {
            try self._write(context: context, data: data, promise: promise)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        guard self.queue.count > 0 else {
            assertionFailure("PostgresRequest queue empty, discarded: \(error)")
            return
        }
        self.queue[0].promise.fail(error)
        context.close(mode: .all, promise: nil)
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        for current in self.queue {
            current.promise.fail(PostgresError.connectionClosed)
        }
        self.queue = []
        context.close(mode: mode, promise: promise)
    }
}
