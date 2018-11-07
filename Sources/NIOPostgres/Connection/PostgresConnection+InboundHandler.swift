import NIO

extension PostgresConnection {
    final class InboundHandler: ChannelInboundHandler {
        typealias InboundIn = PostgresMessage
        typealias OutboundOut = PostgresMessage
        
        let channel: Channel
        
        struct Request {
            let promise: EventLoopPromise<Void>
            let callback: (PostgresMessage) throws -> Bool
        }
        
        var waiters: [Request]
        
        init(_ channel: Channel) {
            self.channel = channel
            self.waiters = .init()
        }
        
        func send(_ messages: [PostgresMessage], _ callback: @escaping (PostgresMessage) throws -> Bool) -> EventLoopFuture<Void> {
            // print("PostgresConnection.ChannelHandler.send(\(messages))")
            let promise: EventLoopPromise<Void> = channel.eventLoop.newPromise()
            waiters.append(Request(promise: promise, callback: callback))
            messages.forEach { channel.write(wrapOutboundOut($0)).cascadeFailure(promise: promise) }
            channel.flush()
            return promise.futureResult
        }
        
        func errorCaught(ctx: ChannelHandlerContext, error: Error) {
            // print("PostgresConnection.ChannelHandler.errorCaught(\(error))")
            switch waiters.count {
            case 0:
                print("Discarding \(error)")
            default:
                // fail the current waiter
                let request = waiters.removeFirst()
                request.promise.fail(error: error)
            }
        }
        
        func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
            let message = unwrapInboundIn(data)
            // print("PostgresConnection.ChannelHandler.channelRead(\(message))")
            switch message {
            case .error(let error):
                errorCaught(ctx: ctx, error: PostgresError(.server(error)))
            default:
                switch waiters.count {
                case 0:
                    print("Discarding \(message)")
                    break
                default:
                    let request = waiters[0]
                    do {
                        if try request.callback(message) {
                            _ = waiters.removeFirst()
                            request.promise.succeed(result: ())
                        }
                    } catch {
                        request.promise.fail(error: error)
                    }
                }
            }
        }
    }
}
