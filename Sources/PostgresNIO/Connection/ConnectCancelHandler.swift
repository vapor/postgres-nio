import NIOConcurrencyHelpers
import NIOCore

final class ConnectCancelHandler: Sendable {
    private enum State {
        case waiting
        case cancelledBeforeConnect
        case tcpConnected(any Channel)
        case done
    }

    private let state: NIOLockedValueBox<State>

    init() {
        self.state = NIOLockedValueBox(.waiting)
    }

    enum ChannelConnectedAction {
        case none
        case close(any Channel)
    }

    func channelConnected(_ channel: any Channel) -> EventLoopFuture<Void>? {
        let action = self.state.withLockedValue { state -> ChannelConnectedAction in
            switch state {
            case .waiting:
                state = .tcpConnected(channel)
                return .none
            case .cancelledBeforeConnect:
                state = .done
                return .close(channel)
            case .tcpConnected, .done:
                preconditionFailure("channelConnected called in invalid state")
            }
        }

        switch action {
        case .none:
            return nil
        case .close(let channel):
            channel.close(mode: .all, promise: nil)
            return channel.closeFuture
        }
    }

    enum CancelAction {
        case none
        case close(any Channel)
    }

    func cancel() -> EventLoopFuture<Void>? {
        let action = self.state.withLockedValue { state -> CancelAction in
            switch state {
            case .waiting:
                state = .cancelledBeforeConnect
                return .none
            case .tcpConnected(let channel):
                state = .done
                return .close(channel)
            case .cancelledBeforeConnect, .done:
                return .none
            }
        }

        switch action {
        case .none:
            return nil
        case .close(let channel):
            channel.close(mode: .all, promise: nil)
            return channel.closeFuture
        }
    }

    func postgresHandshakeDone() {
        self.state.withLockedValue { state in
            state = .done
        }
    }
}
