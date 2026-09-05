import NIOCore

// Thread safety is guaranteed in the NotificationListener by dispatching all access to the shared
// state onto the underlying EventLoop.
final class NotificationListener: Sendable {
    let eventLoop: any EventLoop

    let channel: String
    let id: Int

    private let stateBox: NIOLoopBoundBox<State>

    enum State {
        case streamInitialized(CheckedContinuation<PostgresNotificationSequence, any Error>)
        case streamListening(AsyncThrowingStream<PostgresNotification, any Error>.Continuation)

        case closure(PostgresListenContext, @Sendable (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void)
        case done
    }

    deinit {
        // The state may only be inspected on the EventLoop. If we are deinitialized somewhere else,
        // we can't validate that the listener has been used correctly.
        guard self.eventLoop.inEventLoop else { return }

        switch self.stateBox.value {
        case .streamInitialized:
            preconditionFailure("Notification continuation had not been used")
        case .closure:
            preconditionFailure("Notification closure had not been used")
        case .streamListening, .done:
            break
        }
    }

    init(
        channel: String,
        id: Int,
        eventLoop: any EventLoop,
        checkedContinuation: CheckedContinuation<PostgresNotificationSequence, any Error>
    ) {
        self.channel = channel
        self.id = id
        self.eventLoop = eventLoop
        self.stateBox = NIOLoopBoundBox.makeBoxSendingValue(
            .streamInitialized(checkedContinuation),
            eventLoop: eventLoop
        )
    }

    init(
        channel: String,
        id: Int,
        eventLoop: any EventLoop,
        context: PostgresListenContext,
        closure: @Sendable @escaping (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void
    ) {
        self.channel = channel
        self.id = id
        self.eventLoop = eventLoop
        self.stateBox = NIOLoopBoundBox.makeBoxSendingValue(
            .closure(context, closure),
            eventLoop: eventLoop
        )
    }

    // Every state modification below returns the side effects it wants to have run as an action. The
    // actions are then run *after* the modifying closure has returned. This is important, since all
    // side effects (resuming continuations, yielding into the stream, invoking the user supplied
    // closure, ...) may reenter this type. Running them while we hold exclusive access to the state
    // would violate the exclusivity of that access.

    private enum StartListeningSucceededAction {
        case none
        case resumeContinuation(
            CheckedContinuation<PostgresNotificationSequence, any Error>,
            PostgresNotificationSequence
        )
    }

    func startListeningSucceeded(handler: PostgresChannelHandler) {
        self.eventLoop.preconditionInEventLoop()
        let handlerLoopBound = NIOLoopBound(handler, eventLoop: self.eventLoop)
        let eventLoop = self.eventLoop
        let channel = self.channel
        let listenerID = self.id

        let action = self.stateBox.withValue { state -> StartListeningSucceededAction in
            switch state {
            case .streamInitialized(let checkedContinuation):
                let (stream, continuation) = AsyncThrowingStream.makeStream(of: PostgresNotification.self)
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
                state = .streamListening(continuation)

                return .resumeContinuation(checkedContinuation, PostgresNotificationSequence(base: stream))

            case .streamListening, .done:
                fatalError("Invalid state: \(state)")

            case .closure:
                return .none // ignore
            }
        }

        switch action {
        case .none:
            break
        case .resumeContinuation(let checkedContinuation, let notificationSequence):
            checkedContinuation.resume(returning: notificationSequence)
        }
    }

    private enum NotificationReceivedAction {
        case yield(AsyncThrowingStream<PostgresNotification, any Error>.Continuation, PostgresNotification)
        case invokeClosure(
            PostgresListenContext,
            @Sendable (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void,
            PostgresMessage.NotificationResponse
        )
    }

    func notificationReceived(_ backendMessage: PostgresBackendMessage.NotificationResponse) {
        self.eventLoop.preconditionInEventLoop()

        let action = self.stateBox.withValue { state -> NotificationReceivedAction in
            switch state {
            case .streamInitialized, .done:
                fatalError("Invalid state: \(state)")

            case .streamListening(let continuation):
                return .yield(continuation, .init(payload: backendMessage.payload))

            case .closure(let postgresListenContext, let closure):
                let message = PostgresMessage.NotificationResponse(
                    backendPID: backendMessage.backendPID,
                    channel: backendMessage.channel,
                    payload: backendMessage.payload
                )
                return .invokeClosure(postgresListenContext, closure, message)
            }
        }

        switch action {
        case .yield(let continuation, let notification):
            continuation.yield(notification)
        case .invokeClosure(let postgresListenContext, let closure, let message):
            closure(postgresListenContext, message)
        }
    }

    private enum EndAction {
        case none
        case failContinuation(CheckedContinuation<PostgresNotificationSequence, any Error>, any Error)
        case finishStream(AsyncThrowingStream<PostgresNotification, any Error>.Continuation, (any Error)?)
        case cancelListenContext(PostgresListenContext)
    }

    func failed(_ error: any Error) {
        self.eventLoop.preconditionInEventLoop()

        let action = self.stateBox.withValue { state -> EndAction in
            switch state {
            case .streamInitialized(let checkedContinuation):
                state = .done
                return .failContinuation(checkedContinuation, error)

            case .streamListening(let continuation):
                state = .done
                return .finishStream(continuation, error)

            case .closure(let postgresListenContext, _):
                state = .done
                return .cancelListenContext(postgresListenContext)

            case .done:
                return .none // ignore
            }
        }

        self.run(action)
    }

    func cancelled() {
        self.eventLoop.preconditionInEventLoop()

        let action = self.stateBox.withValue { state -> EndAction in
            switch state {
            case .streamInitialized(let checkedContinuation):
                state = .done
                return .failContinuation(checkedContinuation, PSQLError(code: .queryCancelled))

            case .streamListening(let continuation):
                state = .done
                return .finishStream(continuation, nil)

            case .closure(let postgresListenContext, _):
                state = .done
                return .cancelListenContext(postgresListenContext)

            case .done:
                return .none // ignore
            }
        }

        self.run(action)
    }

    private func run(_ action: EndAction) {
        switch action {
        case .none:
            break
        case .failContinuation(let checkedContinuation, let error):
            checkedContinuation.resume(throwing: error)
        case .finishStream(let continuation, let error):
            continuation.finish(throwing: error)
        case .cancelListenContext(let postgresListenContext):
            postgresListenContext.cancel()
        }
    }
}
