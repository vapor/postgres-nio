
struct PrepareStatementStateMachine {
    
    enum State {
        case initialized(CreatePreparedStatementContext)
        case parseDescribeSent(CreatePreparedStatementContext)
    
        case parseCompleteReceived(CreatePreparedStatementContext)
        case parameterDescriptionReceived(CreatePreparedStatementContext)
        case rowDescriptionReceived
        case noDataMessageReceived
        
        case error(PSQLError)
    }
    
    enum Action {
        case sendParseDescribeSync(name: String, query: String)
        case succeedPreparedStatementCreation(CreatePreparedStatementContext, with: PSQLBackendMessage.RowDescription?)
        case failPreparedStatementCreation(CreatePreparedStatementContext, with: PSQLError)

        case read
        case wait
    }
    
    var state: State
    
    init(createContext: CreatePreparedStatementContext) {
        self.state = .initialized(createContext)
    }
    
    mutating func start() -> Action {
        guard case .initialized(let createContext) = self.state else {
            preconditionFailure("Start should only be called, if the query has been initialized")
        }
        
        self.state = .parseDescribeSent(createContext)
        
        return .sendParseDescribeSync(name: createContext.name, query: createContext.query)
    }
    
    mutating func parseCompletedReceived() -> Action {
        guard case .parseDescribeSent(let createContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.parseComplete))
        }
        
        self.state = .parseCompleteReceived(createContext)
        return .wait
    }
    
    mutating func parameterDescriptionReceived(_ parameterDescription: PSQLBackendMessage.ParameterDescription) -> Action {
        guard case .parseCompleteReceived(let createContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.parameterDescription(parameterDescription)))
        }
        
        self.state = .parameterDescriptionReceived(createContext)
        return .wait
    }
    
    mutating func noDataReceived() -> Action {
        guard case .parameterDescriptionReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.noData))
        }
        
        self.state = .noDataMessageReceived
        return .succeedPreparedStatementCreation(queryContext, with: nil)
    }
    
    mutating func rowDescriptionReceived(_ rowDescription: PSQLBackendMessage.RowDescription) -> Action {
        guard case .parameterDescriptionReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.rowDescription(rowDescription)))
        }
        
        self.state = .rowDescriptionReceived
        return .succeedPreparedStatementCreation(queryContext, with: rowDescription)
    }
    
    mutating func errorReceived(_ errorMessage: PSQLBackendMessage.ErrorResponse) -> Action {
        let error = PSQLError.server(errorMessage)
        switch self.state {
        case .initialized:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
            
        case .parseDescribeSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived:
            return self.setAndFireError(error)
            
        case .rowDescriptionReceived,
             .noDataMessageReceived:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
            
        case .error:
            // don't override the first error
            return .wait
        }
    }
    
    private mutating func setAndFireError(_ error: PSQLError) -> Action {
        switch self.state {
        case .initialized(let context),
             .parseDescribeSent(let context),
             .parseCompleteReceived(let context),
             .parameterDescriptionReceived(let context):
            self.state = .error(error)
            return .failPreparedStatementCreation(context, with: error)
        case .rowDescriptionReceived,
             .noDataMessageReceived,
             .error:
            #warning("This must be implemented")
            preconditionFailure("unimplementd")
        }
    }
    
    // MARK: Channel actions
    
    mutating func readEventCatched() -> Action {
        return .read
    }
    
    var isComplete: Bool {
        switch self.state {
        case .rowDescriptionReceived,
             .noDataMessageReceived,
             .error:
            return true
        case .initialized,
             .parseDescribeSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived:
            return false
        }
    }
    
    // MARK: Private Methods
    
}
