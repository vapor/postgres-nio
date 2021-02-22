
struct CloseStateMachine {
    
    enum State {
        case initialized(CloseCommandContext)
        case closeSyncSent(CloseCommandContext)
        case closeCompleteReceived
        
        case error(PSQLError)
    }
    
    enum Action {
        case sendCloseSync(CloseTarget)
        case succeedClose(CloseCommandContext)
        case failClose(CloseCommandContext, with: PSQLError)

        case read
        case wait
    }
    
    var state: State
    
    init(closeContext: CloseCommandContext) {
        self.state = .initialized(closeContext)
    }
    
    mutating func start() -> Action {
        guard case .initialized(let closeContext) = self.state else {
            preconditionFailure("Start should only be called, if the query has been initialized")
        }
        
        self.state = .closeSyncSent(closeContext)
        
        return .sendCloseSync(closeContext.target)
    }
    
    mutating func closeCompletedReceived() -> Action {
        guard case .closeSyncSent(let closeContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.closeComplete))
        }
        
        self.state = .closeCompleteReceived
        return .succeedClose(closeContext)
    }
    
    mutating func errorReceived(_ errorMessage: PSQLBackendMessage.ErrorResponse) -> Action {
        let error = PSQLError.server(errorMessage)
        switch self.state {
        case .initialized:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
            
        case .closeSyncSent:
            return self.setAndFireError(error)
            
        case .closeCompleteReceived:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
            
        case .error:
            // don't override the first error
            return .wait
        }
    }
    
    mutating func errorHappened(_ error: PSQLError) -> Action {
        return self.setAndFireError(error)
    }

    // MARK: Channel actions
    
    mutating func readEventCaught() -> Action {
        return .read
    }
    
    var isComplete: Bool {
        switch self.state {
        case .closeCompleteReceived, .error:
            return true
        case .initialized, .closeSyncSent:
            return false
        }
    }
    
    // MARK: Private Methods

    private mutating func setAndFireError(_ error: PSQLError) -> Action {
        switch self.state {
        case .closeSyncSent(let closeContext):
            self.state = .error(error)
            return .failClose(closeContext, with: error)
        case .initialized, .closeCompleteReceived, .error:
            preconditionFailure("invalid state")
        }
    }
}
