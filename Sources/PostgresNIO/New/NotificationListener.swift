import NIOCore

// This object is @unchecked Sendable, since we syncronize state on the EL
final class NotificationListener: @unchecked Sendable {
    let eventLoop: EventLoop

    let channel: String
    let id: Int

    private var state: State

    enum State {
        case streamInitialized(CheckedContinuation<PostgresNotificationSequence, Error>)
        case streamListening(AsyncThrowingStream<PostgresNotification, Error>.Continuation)

        case closure(PostgresListenContext, (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void)
        case done
    }

    init(
        channel: String,
        id: Int,
        eventLoop: EventLoop,
        checkedContinuation: CheckedContinuation<PostgresNotificationSequence, Error>
    ) {
        self.channel = channel
        self.id = id
        self.eventLoop = eventLoop
        self.state = .streamInitialized(checkedContinuation)
    }

    init(
        channel: String,
        id: Int,
        eventLoop: EventLoop,
        context: PostgresListenContext,
        closure: @Sendable @escaping (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void
    ) {
        self.channel = channel
        self.id = id
        self.eventLoop = eventLoop
        self.state = .closure(context, closure)
    }

    func startListeningSucceeded(handler: PostgresChannelHandler) {
        self.eventLoop.preconditionInEventLoop()
        let handlerLoopBound = NIOLoopBound(handler, eventLoop: self.eventLoop)

        switch self.state {
        case .streamInitialized(let checkedContinuation):
            let (stream, continuation) = AsyncThrowingStream.makeStream(of: PostgresNotification.self)
            let eventLoop = self.eventLoop
            let channel = self.channel
            let listenerID = self.id
            continuation.onTermination = { reason in
                switch reason {
                case .cancelled:
                    eventLoop.execute {
                        handlerLoopBound.value.cancelNotificationListener(channel: channel, id: listenerID)
                    }

                case .finished:
                    break

                @unknown default:
                    break
                }
            }
            self.state = .streamListening(continuation)

            let notificationSequence = PostgresNotificationSequence(base: stream)
            checkedContinuation.resume(returning: notificationSequence)

        case .streamListening, .done:
            fatalError("Invalid state: \(self.state)")

        case .closure:
            break // ignore
        }
    }

    func notificationReceived(_ backendMessage: PostgresBackendMessage.NotificationResponse) {
        self.eventLoop.preconditionInEventLoop()

        switch self.state {
        case .streamInitialized, .done:
            fatalError("Invalid state: \(self.state)")
        case .streamListening(let continuation):
            continuation.yield(.init(payload: backendMessage.payload))

        case .closure(let postgresListenContext, let closure):
            let message = PostgresMessage.NotificationResponse(
                backendPID: backendMessage.backendPID,
                channel: backendMessage.channel,
                payload: backendMessage.payload
            )
            closure(postgresListenContext, message)
        }
    }

    func failed(_ error: Error) {
        self.eventLoop.preconditionInEventLoop()

        switch self.state {
        case .streamInitialized(let checkedContinuation):
            self.state = .done
            checkedContinuation.resume(throwing: error)

        case .streamListening(let continuation):
            self.state = .done
            continuation.finish(throwing: error)

        case .closure(let postgresListenContext, _):
            self.state = .done
            postgresListenContext.cancel()

        case .done:
            break // ignore
        }
    }

    func cancelled() {
        self.eventLoop.preconditionInEventLoop()

        switch self.state {
        case .streamInitialized(let checkedContinuation):
            self.state = .done
            checkedContinuation.resume(throwing: PSQLError(code: .queryCancelled))

        case .streamListening(let continuation):
            self.state = .done
            continuation.finish()

        case .closure(let postgresListenContext, _):
            self.state = .done
            postgresListenContext.cancel()

        case .done:
            break // ignore
        }
    }
}


#if compiler(<5.9)
// Async stream API backfill
extension AsyncThrowingStream {
    static func makeStream(
         of elementType: Element.Type = Element.self,
         throwing failureType: Failure.Type = Failure.self,
         bufferingPolicy limit: Continuation.BufferingPolicy = .unbounded
     ) -> (stream: AsyncThrowingStream<Element, Failure>, continuation: AsyncThrowingStream<Element, Failure>.Continuation) where Failure == Error {
         var continuation: AsyncThrowingStream<Element, Failure>.Continuation!
         let stream = AsyncThrowingStream<Element, Failure>(bufferingPolicy: limit) { continuation = $0 }
         return (stream: stream, continuation: continuation!)
     }
 }
 #endif
