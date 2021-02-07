
struct ConnectionStateMachine {
    
    typealias TransactionState = PSQLBackendMessage.TransactionState
    
    struct ConnectionContext {
        let processID: Int32
        let secretKey: Int32
        
        var parameters: [String: String]
        var transactionState: TransactionState
    }
    
    struct BackendKeyData {
        let processID: Int32
        let secretKey: Int32
    }
    
    enum State {
        case initialized
        case connected
        case sslRequestSent
        case sslNegotiated
        case sslHandlerAdded
        case waitingToStartAuthentication
        case authenticating(AuthenticationStateMachine)
        case authenticated(BackendKeyData?, [String: String])
        
        case readyForQuery(ConnectionContext)
        case extendedQuery(ExtendedQueryStateMachine, ConnectionContext)
        case prepareStatement(PrepareStatementStateMachine, ConnectionContext)
        case closeCommand(CloseStateMachine, ConnectionContext)
        
        case error(PSQLError)
        case closing
        case closed
        
        case modifying
    }
    
    enum QuiescingState {
        case notQuiescing
        case quiescing(closePromise: EventLoopPromise<Void>?)
    }
    
    enum ConnectionAction {
        
        struct Parse: Equatable {
            var statementName: String
            
            /// The query string to be parsed.
            var query: String
            
            /// The number of parameter data types specified (can be zero).
            /// Note that this is not an indication of the number of parameters that might appear in the
            /// query string, only the number that the frontend wants to prespecify types for.
            /// Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
            var parameterTypes: [PSQLDataType]
        }
        
        struct CleanUpContext {
            
            /// Tasks to fail with the error
            let tasks: [PSQLTask]
            
        }
        
        case read
        case wait
        case sendSSLRequest
        case establishSSLConnection
        case fireErrorAndCloseConnetion(PSQLError)
        case closeConnection(EventLoopPromise<Void>?)
        case provideAuthenticationContext
        case fireEventReadyForQuery
        case forwardNotificationToListeners(PSQLBackendMessage.NotificationResponse)
        
        // Auth Actions
        case sendStartupMessage(AuthContext)
        case sendPasswordMessage(PasswordAuthencationMode, AuthContext)
        
        // Connection Actions
        
        // --- general actions
        case sendParseDescribeBindExecuteSync(query: String, binds: [PSQLEncodable])
        case sendBindExecuteSync(statementName: String, binds: [PSQLEncodable])
        case failQuery(ExecuteExtendedQueryContext, with: PSQLError)
        case succeedQuery(ExecuteExtendedQueryContext, columns: [PSQLBackendMessage.RowDescription.Column])
        case succeedQueryNoRowsComming(ExecuteExtendedQueryContext, commandTag: String)
        
        // --- streaming actions
        // actions if query has requested next row but we are waiting for backend
        case forwardRow([PSQLData], to: EventLoopPromise<StateMachineStreamNextResult>)
        case forwardCommandComplete(CircularBuffer<[PSQLData]>, commandTag: String, to: EventLoopPromise<StateMachineStreamNextResult>)
        case forwardStreamError(PSQLError, to: EventLoopPromise<StateMachineStreamNextResult>)
        // actions if query has not asked for next row but are pushing the final bytes to it
        case forwardStreamErrorToCurrentQuery(PSQLError, read: Bool)
        case forwardStreamCompletedToCurrentQuery(CircularBuffer<[PSQLData]>, commandTag: String, read: Bool)
        
        // Prepare statement actions
        case sendParseDescribeSync(name: String, query: String)
        case succeedPreparedStatementCreation(CreatePreparedStatementContext, with: PSQLBackendMessage.RowDescription?)
        case failPreparedStatementCreation(CreatePreparedStatementContext, with: PSQLError)
        
        // Close actions
        case sendCloseSync(CloseTarget)
        case succeedClose(CloseCommandContext)
        case failClose(CloseCommandContext, with: PSQLError)
    }
    
    private var state: State
    private var taskQueue = CircularBuffer<PSQLTask>()
    private var quiescingState: QuiescingState = .notQuiescing
    
    init() {
        self.state = .initialized
    }
    
    #if DEBUG
    /// for testing purposes only
    init(_ state: State) {
        self.state = state
    }
    #endif
    
    mutating func connected(requireTLS: Bool) -> ConnectionAction {
        guard case .initialized = self.state else {
            preconditionFailure("Unexpected state")
        }
        self.state = .connected
        if requireTLS {
            return self.sendSSLRequest()
        } else {
            self.state = .waitingToStartAuthentication
            return .provideAuthenticationContext
        }
    }
    
    mutating func provideAuthenticationContext(_ authContext: AuthContext) -> ConnectionAction {
        self.startAuthentication(authContext)
    }
    
    mutating func close(_ promise: EventLoopPromise<Void>?) -> ConnectionAction {
        switch self.state {
        case .closing, .closed:
            // we are already closed, but sometimes an upstream handler might want to close the
            // connection, though it has already been closed by the remote. Typical race condition.
            return .closeConnection(promise)
        case .readyForQuery:
            precondition(self.taskQueue.isEmpty, """
                The state should only be .readyForQuery if there are no more tasks in the queue
                """)
            self.state = .closing
            return .closeConnection(promise)
        default:
            switch self.quiescingState {
            case .notQuiescing:
                self.quiescingState = .quiescing(closePromise: promise)
            case .quiescing(.some(let closePromise)):
                closePromise.futureResult.cascade(to: promise)
            case .quiescing(.none):
                self.quiescingState = .quiescing(closePromise: promise)
            }
            return .wait
        }
    }
    
    mutating func closed() -> ConnectionAction {
        switch self.state {
        case .readyForQuery:
            guard case .notQuiescing = self.quiescingState else {
                preconditionFailure("A connection can never be quiescing and readyForQuery at the same time")
            }
            
            self.state = .closed
            return .wait
        case .error, .closing:
            self.state = .closed
            self.quiescingState = .notQuiescing
            return .wait
            
        case .authenticated,
             .initialized,
             .connected,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .extendedQuery,
             .prepareStatement,
             .closeCommand,
             .closed:
            preconditionFailure("The connection can only be closed, if we are ready for next request or failed")
            
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func sslSupportedReceived() -> ConnectionAction {
        switch self.state {
        case .sslRequestSent:
            self.state = .sslNegotiated
            return .establishSSLConnection
        default:
            return self.setAndFireError(.unexpectedBackendMessage(.sslSupported))
        }
    }
    
    mutating func sslUnsupportedReceived() -> ConnectionAction {
        switch self.state {
        case .sslRequestSent:
            return self.setAndFireError(.sslUnsupported)
        default:
            return self.setAndFireError(.unexpectedBackendMessage(.sslSupported))
        }
    }
    
    mutating func sslHandlerAdded() -> ConnectionAction {
        guard case .sslNegotiated = self.state else {
            preconditionFailure("Can only add a ssl handler after negotiation")
        }
        
        self.state = .sslHandlerAdded
        return .wait
    }
    
    mutating func sslEstablished() -> ConnectionAction {
        guard case .sslHandlerAdded = self.state else {
            preconditionFailure("Can only establish a ssl connection after adding a ssl handler")
        }
        
        self.state = .waitingToStartAuthentication
        return .provideAuthenticationContext
    }
    
    mutating func authenticationMessageReceived(_ message: PSQLBackendMessage.Authentication) -> ConnectionAction {
        guard case .authenticating(var authState) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.authentication(message)))
        }
        
        return self.avoidingStateMachineCoW { state in
            let action = authState.authenticationMessageReceived(message)
            state = .authenticating(authState)
            return state.modify(with: action)
        }
    }
    
    mutating func backendKeyDataReceived(_ keyData: PSQLBackendMessage.BackendKeyData) -> ConnectionAction {
        guard case .authenticated(_, let parameters) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.backendKeyData(keyData)))
        }
        
        let keyData = BackendKeyData(
            processID: keyData.processID,
            secretKey: keyData.secretKey)
        
        self.state = .authenticated(keyData, parameters)
        return .wait
    }
    
    mutating func parameterStatusReceived(_ status: PSQLBackendMessage.ParameterStatus) -> ConnectionAction {
        switch self.state {
        case .connected,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .closing:
            self.state = .error(.unexpectedBackendMessage(.parameterStatus(status)))
            return .wait
        case .authenticated(let keyData, var parameters):
            return self.avoidingStateMachineCoW { state in
                parameters[status.parameter] = status.value
                state = .authenticated(keyData, parameters)
                return .wait
            }
        case .readyForQuery(var connectionContext):
            return self.avoidingStateMachineCoW { state in
                connectionContext.parameters[status.parameter] = status.value
                state = .readyForQuery(connectionContext)
                return .wait
            }
        case .extendedQuery(let query, var connectionContext):
            return self.avoidingStateMachineCoW { state in
                connectionContext.parameters[status.parameter] = status.value
                state = .extendedQuery(query, connectionContext)
                return .wait
            }
        case .prepareStatement(let prepareState, var connectionContext):
            return self.avoidingStateMachineCoW { state in
                connectionContext.parameters[status.parameter] = status.value
                state = .prepareStatement(prepareState, connectionContext)
                return .wait
            }
        case .closeCommand(let closeState, var connectionContext):
            return self.avoidingStateMachineCoW { state in
                connectionContext.parameters[status.parameter] = status.value
                state = .closeCommand(closeState, connectionContext)
                return .wait
            }
        case .error(_):
            return .wait
        case .initialized,
             .closed:
            preconditionFailure("We shouldn't receive messages if we are not connected")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func errorReceived(_ errorMessage: PSQLBackendMessage.ErrorResponse) -> ConnectionAction {
        switch self.state {
        case .authenticating(var authState):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = authState.errorReceived(errorMessage)
                state = .authenticating(authState)
                return state.modify(with: action)
            }
        case .extendedQuery(var extendedQueryState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = extendedQueryState.errorReceived(errorMessage)
                state = .extendedQuery(extendedQueryState, connectionContext)
                return state.modify(with: action)
            }
        case .closeCommand(var closeStateMachine, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = closeStateMachine.errorReceived(errorMessage)
                state = .closeCommand(closeStateMachine, connectionContext)
                return state.modify(with: action)
            }
        default:
            return self.setAndFireError(.server(errorMessage))
        }
    }
    
    mutating func errorHappened(_ error: PSQLError) -> ConnectionAction {
        return self.setAndFireError(error)
    }
    
    mutating func noticeReceived(_ notice: PSQLBackendMessage.NoticeResponse) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var extendedQuery, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = extendedQuery.noticeReceived(notice)
                state = .extendedQuery(extendedQuery, connectionContext)
                return state.modify(with: action)
            }
        default:
            return .wait
        }
    }
    
    mutating func notificationReceived(_ notification: PSQLBackendMessage.NotificationResponse) -> ConnectionAction {
        return .forwardNotificationToListeners(notification)
    }
    
    mutating func readyForQueryReceived(_ transactionState: PSQLBackendMessage.TransactionState) -> ConnectionAction {
        switch self.state {
        case .authenticated(let backendKeyData, let parameters):
            guard let keyData = backendKeyData else {
                preconditionFailure()
            }
            
            let connectionContext = ConnectionContext(
                processID: keyData.processID,
                secretKey: keyData.secretKey,
                parameters: parameters,
                transactionState: transactionState)
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        case .extendedQuery(let extendedQuery, var connectionContext):
            guard extendedQuery.isComplete else {
                assertionFailure("A ready for query has been received, but our ExecuteQueryStateMachine has not reached a finish point. Something must be wrong")
                return self.setAndFireError(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        case .prepareStatement(let preparedStateMachine, var connectionContext):
            guard preparedStateMachine.isComplete else {
                assertionFailure("A ready for query has been received, but our PrepareStatementStateMachine has not reached a finish point. Something must be wrong")
                return self.setAndFireError(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        
        case .closeCommand(let closeStateMachine, var connectionContext):
            guard closeStateMachine.isComplete else {
                assertionFailure("A ready for query has been received, but our CloseCommandStateMachine has not reached a finish point. Something must be wrong")
                return self.setAndFireError(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
            
        default:
            return self.setAndFireError(.unexpectedBackendMessage(.readyForQuery(transactionState)))
        }
    }
    
    mutating func enqueue(task: PSQLTask) -> ConnectionAction {
        // check if we are quiescing. if so fail task immidiatly
        if case .quiescing = self.quiescingState {
            switch task {
            case .extendedQuery(let queryContext):
                return .failQuery(queryContext, with: .connectionQuiescing)
            case .preparedStatement(let prepareContext):
                return .failPreparedStatementCreation(prepareContext, with: .connectionQuiescing)
            case .closeCommand(let closeContext):
                return .failClose(closeContext, with: .connectionQuiescing)
            }
        }

        switch self.state {
        case .readyForQuery:
            return self.executeTask(task)
        case .closed:
            switch task {
            case .extendedQuery(let queryContext):
                return .failQuery(queryContext, with: .connectionClosed)
            case .preparedStatement(let prepareContext):
                return .failPreparedStatementCreation(prepareContext, with: .connectionClosed)
            case .closeCommand(let closeContext):
                return .failClose(closeContext, with: .connectionClosed)
            }
        default:
            self.taskQueue.append(task)
            return .wait
        }
    }
    
    mutating func readEventCatched() -> ConnectionAction {
        switch self.state {
        case .initialized:
            preconditionFailure("How can we receive a read, if the connection isn't active.")
        case .connected:
            return .read
        case .sslRequestSent:
            return .read
        case .sslNegotiated:
            return .read
        case .sslHandlerAdded:
            return .read
        case .waitingToStartAuthentication:
            return .read
        case .authenticating:
            return .read
        case .authenticated:
            return .read
        case .readyForQuery:
            return .read
        case .extendedQuery(var extendedQuery, let connectionContext):
            return self.avoidingStateMachineCoW { state in
                let action = extendedQuery.readEventCatched()
                state = .extendedQuery(extendedQuery, connectionContext)
                return state.modify(with: action)
            }
        case .prepareStatement(var preparedStatement, let connectionContext):
            return self.avoidingStateMachineCoW { state in
                let action = preparedStatement.readEventCatched()
                state = .prepareStatement(preparedStatement, connectionContext)
                return state.modify(with: action)
            }
        case .closeCommand(var closeState, let connectionContext):
            return self.avoidingStateMachineCoW { state in
                let action = closeState.readEventCatched()
                state = .closeCommand(closeState, connectionContext)
                return state.modify(with: action)
            }
        case .error:
            return .read
        case .closing:
            return .read
        case .closed:
            preconditionFailure("How can we receive a read, if the connection is closed")
        case .modifying:
            preconditionFailure("Invalid state")

        }
    }
    
    // MARK: - Running Queries -
    
    // MARK: Connection
    
    mutating func parseCompleteReceived() -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = queryState.parseCompletedReceived()
                state = .extendedQuery(queryState, connectionContext)
                return state.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = preparedState.parseCompletedReceived()
                state = .prepareStatement(preparedState, connectionContext)
                return state.modify(with: action)
            }
        default:
            return self.setAndFireError(.unexpectedBackendMessage(.parseComplete))
        }
    }
    
    mutating func bindCompleteReceived() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.bindComplete))
        }
        
        return self.avoidingStateMachineCoW { state -> ConnectionAction in
            let action = queryState.bindCompleteReceived()
            state = .extendedQuery(queryState, connectionContext)
            return state.modify(with: action)
        }
    }
    
    mutating func parameterDescriptionReceived(_ description: PSQLBackendMessage.ParameterDescription) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = queryState.parameterDescriptionReceived(description)
                state = .extendedQuery(queryState, connectionContext)
                return state.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = preparedState.parameterDescriptionReceived(description)
                state = .prepareStatement(preparedState, connectionContext)
                return state.modify(with: action)
            }
        default:
            return self.setAndFireError(.unexpectedBackendMessage(.parameterDescription(description)))
        }
    }
    
    mutating func rowDescriptionReceived(_ description: PSQLBackendMessage.RowDescription) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = queryState.rowDescriptionReceived(description)
                state = .extendedQuery(queryState, connectionContext)
                return state.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = preparedState.rowDescriptionReceived(description)
                state = .prepareStatement(preparedState, connectionContext)
                return state.modify(with: action)
            }
        default:
            return self.setAndFireError(.unexpectedBackendMessage(.rowDescription(description)))
        }
    }
    
    mutating func noDataReceived() -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = queryState.noDataReceived()
                state = .extendedQuery(queryState, connectionContext)
                return state.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                let action = preparedState.noDataReceived()
                state = .prepareStatement(preparedState, connectionContext)
                return state.modify(with: action)
            }
        default:
            return self.setAndFireError(.unexpectedBackendMessage(.noData))
        }
        
    }

    mutating func portalSuspendedReceived() -> ConnectionAction {
        self.setAndFireError(.unexpectedBackendMessage(.portalSuspended))
    }
    
    mutating func closeCompletedReceived() -> ConnectionAction {
        guard case .closeCommand(var closeState, let connectionContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.closeComplete))
        }
        
        return self.avoidingStateMachineCoW { state -> ConnectionAction in
            let action = closeState.closeCompletedReceived()
            state = .closeCommand(closeState, connectionContext)
            return state.modify(with: action)
        }
    }
    
    mutating func commandCompletedReceived(_ commandTag: String) -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.commandComplete(commandTag)))
        }
        
        return self.avoidingStateMachineCoW { state -> ConnectionAction in
            let action = queryState.commandCompletedReceived(commandTag)
            state = .extendedQuery(queryState, connectionContext)
            return state.modify(with: action)
        }
    }
    
    mutating func emptyQueryResponseReceived() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.emptyQueryResponse))
        }
        
        return self.avoidingStateMachineCoW { state -> ConnectionAction in
            let action = queryState.emptyQueryResponseReceived()
            state = .extendedQuery(queryState, connectionContext)
            return state.modify(with: action)
        }
    }
    
    mutating func dataRowReceived(_ dataRow: PSQLBackendMessage.DataRow) -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.dataRow(dataRow)))
        }
        
        return self.avoidingStateMachineCoW { state -> ConnectionAction in
            let action = queryState.dataRowReceived(dataRow)
            state = .extendedQuery(queryState, connectionContext)
            return state.modify(with: action)
        }
    }
    
    // MARK: Consumer
    
    mutating func cancelQueryStream() -> ConnectionAction {
        preconditionFailure("Unimplemented")
    }
    
    mutating func consumeNextQueryRow(promise: EventLoopPromise<StateMachineStreamNextResult>) -> ConnectionAction {
        guard case .extendedQuery(var extendedQuery, let connectionContext) = self.state else {
            preconditionFailure("Tried to consume next row, without active query")
        }
        
        return self.avoidingStateMachineCoW { state -> ConnectionAction in
            let action = extendedQuery.consumeNextRow(promise: promise)
            state = .extendedQuery(extendedQuery, connectionContext)
            return state.modify(with: action)
        }
    }
    
    // MARK: - Private Methods -
    
    private mutating func startAuthentication(_ authContext: AuthContext) -> ConnectionAction {
        guard case .waitingToStartAuthentication = self.state else {
            preconditionFailure("Can only start authentication after connect or ssl establish")
        }
        
        return self.avoidingStateMachineCoW { state in
            var authState = AuthenticationStateMachine(authContext: authContext)
            let action = authState.start()
            state = .authenticating(authState)
            return state.modify(with: action)
        }
    }
    
    private mutating func sendSSLRequest() -> ConnectionAction {
        guard case .connected = self.state else {
            preconditionFailure("Can only send the SSL request directly after connect.")
        }
        
        self.state = .sslRequestSent
        return .sendSSLRequest
    }
    
    private mutating func setAndFireError(_ error: PSQLError) -> ConnectionAction {
        self.avoidingStateMachineCoW { state -> ConnectionAction in
            state = .error(error)
            return .fireErrorAndCloseConnetion(error)
        }
    }
    
    private mutating func executeNextQueryFromQueue() -> ConnectionAction {
        guard case .readyForQuery = self.state else {
            preconditionFailure("Only expected to be invoked, if we are readyToQuery")
        }
        
        if let task = self.taskQueue.popFirst() {
            return self.executeTask(task)
        }
        
        // if we don't have anything left to do and we are quiescing, next we should close
        if case .quiescing(let promise) = self.quiescingState {
            self.state = .closed
            return .closeConnection(promise)
        }
        
        return .fireEventReadyForQuery
    }
    
    private mutating func executeTask(_ task: PSQLTask) -> ConnectionAction {
        guard case .readyForQuery(let connectionContext) = self.state else {
            preconditionFailure("Only expected to be invoked, if we are readyToQuery")
        }
        
        switch task {
        case .extendedQuery(let queryContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                var extendedQuery = ExtendedQueryStateMachine(queryContext: queryContext)
                let action = extendedQuery.start()
                state = .extendedQuery(extendedQuery, connectionContext)
                return state.modify(with: action)
            }
        case .preparedStatement(let prepareContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                var prepareStatement = PrepareStatementStateMachine(createContext: prepareContext)
                let action = prepareStatement.start()
                state = .prepareStatement(prepareStatement, connectionContext)
                return state.modify(with: action)
            }
        case .closeCommand(let closeContext):
            return self.avoidingStateMachineCoW { state -> ConnectionAction in
                var closeStateMachine = CloseStateMachine(closeContext: closeContext)
                let action = closeStateMachine.start()
                state = .closeCommand(closeStateMachine, connectionContext)
                return state.modify(with: action)
            }
        }
    }
    
    struct Configuration {
        let requireTLS: Bool
    }
}

// MARK: CoW helpers

extension ConnectionStateMachine {
    @inline(__always)
    private mutating func avoidingStateMachineCoW<ReturnType>(_ body: (inout State) -> ReturnType) -> ReturnType {
        self.state = .modifying
        defer {
            assert(!self.isModifying)
        }

        return body(&self.state)
    }

    private var isModifying: Bool {
        if case .modifying = self.state {
            return true
        } else {
            return false
        }
    }
}

extension ConnectionStateMachine.State {
    func modify(with action: ExtendedQueryStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendParseDescribeBindExecuteSync(let query, let binds):
            return .sendParseDescribeBindExecuteSync(query: query, binds: binds)
        case .sendBindExecuteSync(let statementName, let binds):
            return .sendBindExecuteSync(statementName: statementName, binds: binds)
        case .failQuery(let requestContext, with: let error):
            return .failQuery(requestContext, with: error)
        case .succeedQuery(let requestContext, columns: let columns):
            return .succeedQuery(requestContext, columns: columns)
        case .succeedQueryNoRowsComming(let requestContext, let commandTag):
            return .succeedQueryNoRowsComming(requestContext, commandTag: commandTag)
        case .forwardRow(let data, to: let promise):
            return .forwardRow(data, to: promise)
        case .forwardCommandComplete(let buffer, let commandTag, to: let promise):
            return .forwardCommandComplete(buffer, commandTag: commandTag, to: promise)
        case .forwardStreamError(let error, to: let promise):
            return .forwardStreamError(error, to: promise)
        case .forwardStreamErrorToCurrentQuery(let error, let read):
            return .forwardStreamErrorToCurrentQuery(error, read: read)
        case .forwardStreamCompletedToCurrentQuery(let buffer, let commandTag, let read):
            return .forwardStreamCompletedToCurrentQuery(buffer, commandTag: commandTag, read: read)
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }
}

extension ConnectionStateMachine.State {
    mutating func modify(with action: PrepareStatementStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendParseDescribeSync(let name, let query):
            return .sendParseDescribeSync(name: name, query: query)
        case .succeedPreparedStatementCreation(let prepareContext, with: let rowDescription):
            return .succeedPreparedStatementCreation(prepareContext, with: rowDescription)
        case .failPreparedStatementCreation(let prepareContext, with: let error):
            return .failPreparedStatementCreation(prepareContext, with: error)
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }
}

extension ConnectionStateMachine.State {
    mutating func modify(with action: AuthenticationStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendStartupMessage(let authContext):
            return .sendStartupMessage(authContext)
        case .sendPassword(let mode, let authContext):
            return .sendPasswordMessage(mode, authContext)
        case .sendSaslInitialResponse:
            preconditionFailure("unimplemented")
        case .sendSaslResponse:
            preconditionFailure("unimplemented")
        case .authenticated:
            self = .authenticated(nil, [:])
            return .wait
        case .reportAuthenticationError(let error):
            self = .error(error)
            return .fireErrorAndCloseConnetion(error)
        }
    }
}

extension ConnectionStateMachine.State {
    mutating func modify(with action: CloseStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendCloseSync(let sendClose):
            return .sendCloseSync(sendClose)
        case .succeedClose(let closeContext):
            return .succeedClose(closeContext)
        case .failClose(let closeContext, with: let error):
            return .failClose(closeContext, with: error)
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }
}

enum StateMachineStreamNextResult {
    /// the next row
    case row([PSQLData])
    
    /// the query has completed, all remaining rows and the command completion tag
    case complete(CircularBuffer<[PSQLData]>, commandTag: String)
}

struct SendPrepareStatement {
    let name: String
    let query: String
}

struct AuthContext: Equatable, CustomDebugStringConvertible {
    let username: String
    let password: String?
    let database: String?
    
    var debugDescription: String {
        """
        (username: \(String(reflecting: self.username)), \
        password: \(self.password != nil ? String(reflecting: self.password!) : "nil"), \
        database: \(self.database != nil ? String(reflecting: self.database!) : "nil"))
        """
    }
}

enum PasswordAuthencationMode: Equatable {
    case cleartext
    case md5(salt: (UInt8, UInt8, UInt8, UInt8))
    
    static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.cleartext, .cleartext):
            return true
        case (.md5(let lhs), .md5(let rhs)):
            return lhs == rhs
        default:
            return false
        }
    }
}

extension ConnectionStateMachine.State: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .initialized:
            return ".initialized"
        case .connected:
            return ".connected"
        case .sslRequestSent:
            return ".sslRequestSent"
        case .sslNegotiated:
            return ".sslNegotiated"
        case .sslHandlerAdded:
            return ".sslHandlerAdded"
        case .waitingToStartAuthentication:
            return ".waitingToStartAuthentication"
        case .authenticating(let authStateMachine):
            return ".authenticating(\(String(reflecting: authStateMachine)))"
        case .authenticated(let backendKeyData, let parameters):
            return ".authenticated(\(String(reflecting: backendKeyData)), \(String(reflecting: parameters)))"
        case .readyForQuery(let connectionContext):
            return ".readyForQuery(connectionContext: \(String(reflecting: connectionContext)))"
        case .extendedQuery(let subStateMachine, let connectionContext):
            return ".extendedQuery(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .prepareStatement(let subStateMachine, let connectionContext):
            return ".prepareStatement(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .closeCommand(let subStateMachine, let connectionContext):
            return ".closeCommand(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .error(let error):
            return ".error(\(String(reflecting: error)))"
        case .closing:
            return ".closing"
        case .closed:
            return ".closed"
        case .modifying:
            return ".modifying"
        }
    }
}

extension ConnectionStateMachine.ConnectionContext: CustomDebugStringConvertible {
    var debugDescription: String {
        """
        (processID: \(self.processID), \
        secretKey: \(self.secretKey), \
        parameters: \(String(reflecting: self.parameters)))
        """
    }
}
