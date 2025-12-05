import NIOCore

struct ListenStateMachine {
    var channels: [String: ChannelState]

    init() {
        self.channels = [:]
    }

    enum StartListeningAction {
        case none
        case startListening(String)
        case succeedListenStart(NotificationListener)
    }

    mutating func startListening(_ new: NotificationListener) -> StartListeningAction {
        return self.channels[new.channel, default: .init()].start(new)
    }

    enum StartListeningSuccessAction {
        case stopListening
        case activateListeners(Dictionary<Int, NotificationListener>.Values)
    }

    mutating func startListeningSucceeded(channel: String) -> StartListeningSuccessAction {
        return self.channels[channel]!.startListeningSucceeded()
    }

    mutating func startListeningFailed(channel: String, error: Error) -> Dictionary<Int, NotificationListener>.Values {
        return self.channels[channel]!.startListeningFailed(error)
    }

    enum StopListeningSuccessAction {
        case startListening
        case none
    }

    mutating func stopListeningSucceeded(channel: String) -> StopListeningSuccessAction {
        switch self.channels[channel]!.stopListeningSucceeded() {
        case .none:
            self.channels.removeValue(forKey: channel)
            return .none

        case .startListening:
            return .startListening
        }
    }

    enum CancelAction {
        case stopListening(String, cancelListener: NotificationListener)
        case cancelListener(NotificationListener)
        case none
    }

    mutating func cancelNotificationListener(channel: String, id: Int) -> CancelAction {
        return self.channels[channel]?.cancelListening(id: id) ?? .none
    }

    mutating func fail(_ error: Error) -> [NotificationListener] {
        var result = [NotificationListener]()
        while var (_, channel) = self.channels.popFirst() {
            switch channel.fail(error) {
            case .none:
                continue

            case .failListeners(let listeners):
                result.append(contentsOf: listeners)
            }
        }
        return result
    }

    enum ReceivedAction {
        case none
        case notify(Dictionary<Int, NotificationListener>.Values)
    }

    func notificationReceived(channel: String) -> ReceivedAction {
        // TODO: Do we want to close the connection, if we receive a notification on a channel that we don't listen to?
        //       We can only change this with the next major release, as it would break current functionality.
        return self.channels[channel]?.notificationReceived() ?? .none
    }
}

extension ListenStateMachine {
    struct ChannelState {
        enum State {
            case initialized
            case starting([Int: NotificationListener])
            case listening([Int: NotificationListener])
            case stopping([Int: NotificationListener])
            case failed(Error)
        }
        
        private var state: State
        
        init() {
            self.state = .initialized
        }
        
        mutating func start(_ new: NotificationListener) -> StartListeningAction {
            switch self.state {
            case .initialized:
                self.state = .starting([new.id: new])
                return .startListening(new.channel)

            case .starting(var listeners):
                listeners[new.id] = new
                self.state = .starting(listeners)
                return .none

            case .listening(var listeners):
                listeners[new.id] = new
                self.state = .listening(listeners)
                return .succeedListenStart(new)
                
            case .stopping(var listeners):
                listeners[new.id] = new
                self.state = .stopping(listeners)
                return .none

            case .failed:
                fatalError("Invalid state: \(self.state)")
            }
        }

        mutating func startListeningSucceeded() -> StartListeningSuccessAction {
            switch self.state {
            case .initialized, .listening, .stopping:
                fatalError("Invalid state: \(self.state)")
                
            case .starting(let listeners):
                if listeners.isEmpty {
                    self.state = .stopping(listeners)
                    return .stopListening
                } else {
                    self.state = .listening(listeners)
                    return .activateListeners(listeners.values)
                }
                
            case .failed:
                fatalError("Invalid state: \(self.state)")
            }
        }
        
        mutating func startListeningFailed(_ error: Error) -> Dictionary<Int, NotificationListener>.Values {
            switch self.state {
            case .initialized, .listening, .stopping:
                fatalError("Invalid state: \(self.state)")
                
            case .starting(let listeners):
                self.state = .initialized
                return listeners.values
                
            case .failed:
                fatalError("Invalid state: \(self.state)")
            }
        }
        
        mutating func stopListeningSucceeded() -> StopListeningSuccessAction {
            switch self.state {
            case .initialized, .listening, .starting:
                fatalError("Invalid state: \(self.state)")
                
            case .stopping(let listeners):
                if listeners.isEmpty {
                    self.state = .initialized
                    return .none
                } else {
                    self.state = .starting(listeners)
                    return .startListening
                }
                
            case .failed:
                return .none
            }
        }
        
        mutating func cancelListening(id: Int) -> CancelAction {
            switch self.state {
            case .initialized:
                fatalError("Invalid state: \(self.state)")
                
            case .starting(var listeners):
                let removed = listeners.removeValue(forKey: id)
                self.state = .starting(listeners)
                if let removed = removed {
                    return .cancelListener(removed)
                }
                return .none

            case .listening(var listeners):
                precondition(!listeners.isEmpty)
                let maybeLast = listeners.removeValue(forKey: id)
                if let last = maybeLast, listeners.isEmpty {
                    self.state = .stopping(listeners)
                    return .stopListening(last.channel, cancelListener: last)
                } else {
                    self.state = .listening(listeners)
                    if let notLast = maybeLast {
                        return .cancelListener(notLast)
                    }
                    return .none
                }
                
            case .stopping(var listeners):
                let removed = listeners.removeValue(forKey: id)
                self.state = .stopping(listeners)
                if let removed = removed {
                    return .cancelListener(removed)
                }
                return .none

            case .failed:
                return .none
            }
        }
        
        enum FailAction {
            case failListeners(Dictionary<Int, NotificationListener>.Values)
            case none
        }
        
        mutating func fail(_ error: Error) -> FailAction {
            switch self.state {
            case .initialized:
                return .none
                
            case .starting(let listeners), .listening(let listeners), .stopping(let listeners):
                self.state = .failed(error)
                return .failListeners(listeners.values)
                
            case .failed:
                return .none
            }
        }
        
        func notificationReceived() -> ReceivedAction {
            switch self.state {
            case .initialized, .starting:
                fatalError("Invalid state: \(self.state)")
                
            case .listening(let listeners):
                return .notify(listeners.values)
                
            case .stopping:
                return .none
                
            default:
                preconditionFailure("TODO: Implemented")
            }
        }
    }
}
