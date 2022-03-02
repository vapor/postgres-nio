import NIOCore
import NIOTLS
import Crypto
import Logging

protocol PSQLChannelHandlerNotificationDelegate: AnyObject {
    func notificationReceived(_: PostgresBackendMessage.NotificationResponse)
}

final class PSQLChannelHandler: ChannelDuplexHandler {
    typealias OutboundIn = PSQLTask
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let logger: Logger
    private var state: ConnectionStateMachine {
        didSet {
            self.logger.trace("Connection state changed", metadata: [.connectionState: "\(self.state)"])
        }
    }
    
    /// A `ChannelHandlerContext` to be used for non channel related events. (for example: More rows needed).
    ///
    /// The context is captured in `handlerAdded` and released` in `handlerRemoved`
    private var handlerContext: ChannelHandlerContext!
    private var rowStream: PSQLRowStream?
    private var decoder: NIOSingleStepByteToMessageProcessor<PSQLBackendMessageDecoder>
    private var encoder: BufferedMessageEncoder!
    private let configuration: PSQLConnection.Configuration
    private let configureSSLCallback: ((Channel) throws -> Void)?
    
    /// this delegate should only be accessed on the connections `EventLoop`
    weak var notificationDelegate: PSQLChannelHandlerNotificationDelegate?
    
    init(configuration: PSQLConnection.Configuration,
         logger: Logger,
         configureSSLCallback: ((Channel) throws -> Void)?)
    {
        self.state = ConnectionStateMachine()
        self.configuration = configuration
        self.configureSSLCallback = configureSSLCallback
        self.logger = logger
        self.decoder = NIOSingleStepByteToMessageProcessor(PSQLBackendMessageDecoder())
    }
    
    #if DEBUG
    /// for testing purposes only
    init(configuration: PSQLConnection.Configuration,
         state: ConnectionStateMachine = .init(.initialized),
         logger: Logger = .psqlNoOpLogger,
         configureSSLCallback: ((Channel) throws -> Void)?)
    {
        self.state = state
        self.configuration = configuration
        self.configureSSLCallback = configureSSLCallback
        self.logger = logger
        self.decoder = NIOSingleStepByteToMessageProcessor(PSQLBackendMessageDecoder())
    }
    #endif

    // MARK: Handler lifecycle
    
    func handlerAdded(context: ChannelHandlerContext) {
        self.handlerContext = context
        self.encoder = BufferedMessageEncoder(
            buffer: context.channel.allocator.buffer(capacity: 256),
            encoder: PSQLFrontendMessageEncoder()
        )
        
        if context.channel.isActive {
            self.connected(context: context)
        }
    }
    
    func handlerRemoved(context: ChannelHandlerContext) {
        self.handlerContext = nil
    }
    
    // MARK: Channel handler incoming
    
    func channelActive(context: ChannelHandlerContext) {
        // `fireChannelActive` needs to be called BEFORE we set the state machine to connected,
        // since we want to make sure that upstream handlers know about the active connection before
        // it receives a 
        context.fireChannelActive()
        
        self.connected(context: context)
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        self.logger.trace("Channel inactive.")
        let action = self.state.closed()
        self.run(action, with: context)
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.debug("Channel error caught.", metadata: [.error: "\(error)"])
        let action = self.state.errorHappened(.channel(underlying: error))
        self.run(action, with: context)
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        
        do {
            try self.decoder.process(buffer: buffer) { message in
                self.logger.trace("Backend message received", metadata: [.message: "\(message)"])
                let action: ConnectionStateMachine.ConnectionAction
                
                switch message {
                case .authentication(let authentication):
                    action = self.state.authenticationMessageReceived(authentication)
                case .backendKeyData(let keyData):
                    action = self.state.backendKeyDataReceived(keyData)
                case .bindComplete:
                    action = self.state.bindCompleteReceived()
                case .closeComplete:
                    action = self.state.closeCompletedReceived()
                case .commandComplete(let commandTag):
                    action = self.state.commandCompletedReceived(commandTag)
                case .dataRow(let dataRow):
                    action = self.state.dataRowReceived(dataRow)
                case .emptyQueryResponse:
                    action = self.state.emptyQueryResponseReceived()
                case .error(let errorResponse):
                    action = self.state.errorReceived(errorResponse)
                case .noData:
                    action = self.state.noDataReceived()
                case .notice(let noticeResponse):
                    action = self.state.noticeReceived(noticeResponse)
                case .notification(let notification):
                    action = self.state.notificationReceived(notification)
                case .parameterDescription(let parameterDescription):
                    action = self.state.parameterDescriptionReceived(parameterDescription)
                case .parameterStatus(let parameterStatus):
                    action = self.state.parameterStatusReceived(parameterStatus)
                case .parseComplete:
                    action = self.state.parseCompleteReceived()
                case .portalSuspended:
                    action = self.state.portalSuspendedReceived()
                case .readyForQuery(let transactionState):
                    action = self.state.readyForQueryReceived(transactionState)
                case .rowDescription(let rowDescription):
                    action = self.state.rowDescriptionReceived(rowDescription)
                case .sslSupported:
                    action = self.state.sslSupportedReceived()
                case .sslUnsupported:
                    action = self.state.sslUnsupportedReceived()
                }
                
                self.run(action, with: context)
            }
        } catch let error as PSQLDecodingError {
            let action = self.state.errorHappened(.decoding(error))
            self.run(action, with: context)
        } catch {
            preconditionFailure("Expected to only get PSQLDecodingErrors from the PSQLBackendMessageDecoder.")
        }
    }
    
    func channelReadComplete(context: ChannelHandlerContext) {
        let action = self.state.channelReadComplete()
        self.run(action, with: context)
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        self.logger.trace("User inbound event received", metadata: [
            .userEvent: "\(event)"
        ])
        
        switch event {
        case TLSUserEvent.handshakeCompleted:
            let action = self.state.sslEstablished()
            self.run(action, with: context)
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }
    
    // MARK: Channel handler outgoing
    
    func read(context: ChannelHandlerContext) {
        self.logger.trace("Channel read event received")
        let action = self.state.readEventCaught()
        self.run(action, with: context)
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let task = self.unwrapOutboundIn(data)
        let action = self.state.enqueue(task: task)
        self.run(action, with: context)
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.logger.trace("Close triggered by upstream.")
        guard mode == .all else {
            // TODO: Support also other modes ?
            promise?.fail(ChannelError.operationUnsupported)
            return
        }

        let action = self.state.close(promise)
        self.run(action, with: context)
    }
    
    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        self.logger.trace("User outbound event received", metadata: [.userEvent: "\(event)"])
        
        switch event {
        case PSQLOutgoingEvent.authenticate(let authContext):
            let action = self.state.provideAuthenticationContext(authContext)
            self.run(action, with: context)
        default:
            context.triggerUserOutboundEvent(event, promise: promise)
        }
    }

    // MARK: Channel handler actions
    
    func run(_ action: ConnectionStateMachine.ConnectionAction, with context: ChannelHandlerContext) {
        self.logger.trace("Run action", metadata: [.connectionAction: "\(action)"])
        
        switch action {
        case .establishSSLConnection:
            self.establishSSLConnection(context: context)
        case .read:
            context.read()
        case .wait:
            break
        case .sendStartupMessage(let authContext):
            self.encoder.encode(.startup(.versionThree(parameters: authContext.toStartupParameters())))
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .sendSSLRequest:
            self.encoder.encode(.sslRequest(.init()))
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .sendPasswordMessage(let mode, let authContext):
            self.sendPasswordMessage(mode: mode, authContext: authContext, context: context)
        case .sendSaslInitialResponse(let name, let initialResponse):
            self.encoder.encode(.saslInitialResponse(.init(saslMechanism: name, initialData: initialResponse)))
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .sendSaslResponse(let bytes):
            self.encoder.encode(.saslResponse(.init(data: bytes)))
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .closeConnectionAndCleanup(let cleanupContext):
            self.closeConnectionAndCleanup(cleanupContext, context: context)
        case .fireChannelInactive:
            context.fireChannelInactive()
        case .sendParseDescribeSync(let name, let query):
            self.sendParseDecribeAndSyncMessage(statementName: name, query: query, context: context)
        case .sendBindExecuteSync(let executeStatement):
            self.sendBindExecuteAndSyncMessage(executeStatement: executeStatement, context: context)
        case .sendParseDescribeBindExecuteSync(let query):
            self.sendParseDescribeBindExecuteAndSyncMessage(query: query, context: context)
        case .succeedQuery(let queryContext, columns: let columns):
            self.succeedQueryWithRowStream(queryContext, columns: columns, context: context)
        case .succeedQueryNoRowsComming(let queryContext, let commandTag):
            self.succeedQueryWithoutRowStream(queryContext, commandTag: commandTag, context: context)
        case .failQuery(let queryContext, with: let error, let cleanupContext):
            queryContext.promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        
        case .forwardRows(let rows):
            self.rowStream!.receive(rows)
            
        case .forwardStreamComplete(let buffer, let commandTag):
            guard let rowStream = self.rowStream else {
                preconditionFailure("Expected to have a row stream here.")
            }
            self.rowStream = nil
            if buffer.count > 0 {
                rowStream.receive(buffer)
            }
            rowStream.receive(completion: .success(commandTag))
            
            
        case .forwardStreamError(let error, let read, let cleanupContext):
            self.rowStream!.receive(completion: .failure(error))
            self.rowStream = nil
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            } else if read {
                context.read()
            }
            
        case .provideAuthenticationContext:
            context.fireUserInboundEventTriggered(PSQLEvent.readyForStartup)
            
            if let authentication = self.configuration.authentication {
                let authContext = AuthContext(
                    username: authentication.username,
                    password: authentication.password,
                    database: authentication.database
                )
                let action = self.state.provideAuthenticationContext(authContext)
                return self.run(action, with: context)
            }
        case .fireEventReadyForQuery:
            context.fireUserInboundEventTriggered(PSQLEvent.readyForQuery)
        case .closeConnection(let promise):
            if context.channel.isActive {
                // The normal, graceful termination procedure is that the frontend sends a Terminate
                // message and immediately closes the connection. On receipt of this message, the
                // backend closes the connection and terminates.
                self.encoder.encode(.terminate)
                context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
            }
            context.close(mode: .all, promise: promise)
        case .succeedPreparedStatementCreation(let preparedContext, with: let rowDescription):
            preparedContext.promise.succeed(rowDescription)
        case .failPreparedStatementCreation(let preparedContext, with: let error, let cleanupContext):
            preparedContext.promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        case .sendCloseSync(let sendClose):
            self.sendCloseAndSyncMessage(sendClose, context: context)
        case .succeedClose(let closeContext):
            closeContext.promise.succeed(Void())
        case .failClose(let closeContext, with: let error, let cleanupContext):
            closeContext.promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        case .forwardNotificationToListeners(let notification):
            self.notificationDelegate?.notificationReceived(notification)
        }
    }
    
    // MARK: - Private Methods -
    
    private func connected(context: ChannelHandlerContext) {

        let action = self.state.connected(tls: .init(self.configuration.tls))
        
        self.run(action, with: context)
    }
    
    private func establishSSLConnection(context: ChannelHandlerContext) {
        // This method must only be called, if we signalized the StateMachine before that we are
        // able to setup a SSL connection.
        do {
            try self.configureSSLCallback!(context.channel)
            let action = self.state.sslHandlerAdded()
            self.run(action, with: context)
        } catch {
            let action = self.state.errorHappened(.failedToAddSSLHandler(underlying: error))
            self.run(action, with: context)
        }
    }
    
    private func sendPasswordMessage(
        mode: PasswordAuthencationMode,
        authContext: AuthContext,
        context: ChannelHandlerContext)
    {
        switch mode {
        case .md5(let salt):
            let hash1 = (authContext.password ?? "") + authContext.username
            let pwdhash = Insecure.MD5.hash(data: [UInt8](hash1.utf8)).asciiHexDigest()

            var hash2 = [UInt8]()
            hash2.reserveCapacity(pwdhash.count + 4)
            hash2.append(contentsOf: pwdhash)
            hash2.append(salt.0)
            hash2.append(salt.1)
            hash2.append(salt.2)
            hash2.append(salt.3)
            let hash = Insecure.MD5.hash(data: hash2).md5PrefixHexdigest()
            
            self.encoder.encode(.password(.init(value: hash)))
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)

        case .cleartext:
            self.encoder.encode(.password(.init(value: authContext.password ?? "")))
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        }
    }
    
    private func sendCloseAndSyncMessage(_ sendClose: CloseTarget, context: ChannelHandlerContext) {
        switch sendClose {
        case .preparedStatement(let name):
            self.encoder.encode(.close(.preparedStatement(name)))
            self.encoder.encode(.sync)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
            
        case .portal(let name):
            self.encoder.encode(.close(.portal(name)))
            self.encoder.encode(.sync)
            context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
        }
    }
    
    private func sendParseDecribeAndSyncMessage(
        statementName: String,
        query: String,
        context: ChannelHandlerContext)
    {
        precondition(self.rowStream == nil, "Expected to not have an open stream at this point")
        let parse = PostgresFrontendMessage.Parse(
            preparedStatementName: statementName,
            query: query,
            parameters: [])

        self.encoder.encode(.parse(parse))
        self.encoder.encode(.describe(.preparedStatement(statementName)))
        self.encoder.encode(.sync)
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
    }
    
    private func sendBindExecuteAndSyncMessage(
        executeStatement: PSQLExecuteStatement,
        context: ChannelHandlerContext
    ) {
        let bind = PostgresFrontendMessage.Bind(
            portalName: "",
            preparedStatementName: executeStatement.name,
            bind: executeStatement.binds)

        self.encoder.encode(.bind(bind))
        self.encoder.encode(.execute(.init(portalName: "")))
        self.encoder.encode(.sync)
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
    }
    
    private func sendParseDescribeBindExecuteAndSyncMessage(
        query: PostgresQuery,
        context: ChannelHandlerContext)
    {
        precondition(self.rowStream == nil, "Expected to not have an open stream at this point")
        let unnamedStatementName = ""
        let parse = PostgresFrontendMessage.Parse(
            preparedStatementName: unnamedStatementName,
            query: query.sql,
            parameters: query.binds.metadata.map(\.dataType))
        let bind = PostgresFrontendMessage.Bind(
            portalName: "",
            preparedStatementName: unnamedStatementName,
            bind: query.binds)

        self.encoder.encode(.parse(parse))
        self.encoder.encode(.describe(.preparedStatement("")))
        self.encoder.encode(.bind(bind))
        self.encoder.encode(.execute(.init(portalName: "")))
        self.encoder.encode(.sync)
        context.writeAndFlush(self.wrapOutboundOut(self.encoder.flush()), promise: nil)
    }
    
    private func succeedQueryWithRowStream(
        _ queryContext: ExtendedQueryContext,
        columns: [RowDescription.Column],
        context: ChannelHandlerContext)
    {
        let rows = PSQLRowStream(
            rowDescription: columns,
            queryContext: queryContext,
            eventLoop: context.channel.eventLoop,
            rowSource: .stream(self))
        
        self.rowStream = rows
        queryContext.promise.succeed(rows)
    }
    
    private func succeedQueryWithoutRowStream(
        _ queryContext: ExtendedQueryContext,
        commandTag: String,
        context: ChannelHandlerContext)
    {
        let rows = PSQLRowStream(
            rowDescription: [],
            queryContext: queryContext,
            eventLoop: context.channel.eventLoop,
            rowSource: .noRows(.success(commandTag))
        )
        queryContext.promise.succeed(rows)
    }
    
    private func closeConnectionAndCleanup(
        _ cleanup: ConnectionStateMachine.ConnectionAction.CleanUpContext,
        context: ChannelHandlerContext)
    {
        self.logger.debug("Cleaning up and closing connection.", metadata: [.error: "\(cleanup.error)"])
        
        // 1. fail all tasks
        cleanup.tasks.forEach { task in
            task.failWithError(cleanup.error)
        }
        
        // 2. fire an error
        context.fireErrorCaught(cleanup.error)
        
        // 3. close the connection or fire channel inactive
        switch cleanup.action {
        case .close:
            context.close(mode: .all, promise: cleanup.closePromise)
        case .fireChannelInactive:
            cleanup.closePromise?.succeed(())
            context.fireChannelInactive()
        }
    }
}

extension PSQLChannelHandler: PSQLRowsDataSource {
    func request(for stream: PSQLRowStream) {
        guard self.rowStream === stream else {
            return
        }
        let action = self.state.requestQueryRows()
        self.run(action, with: self.handlerContext!)
    }
    
    func cancel(for stream: PSQLRowStream) {
        guard self.rowStream === stream else {
            return
        }
        // we ignore this right now :)
    }
}

extension PSQLConnection.Configuration.Authentication {
    func toAuthContext() -> AuthContext {
        AuthContext(
            username: self.username,
            password: self.password,
            database: self.database)
    }
}

extension AuthContext {
    func toStartupParameters() -> PostgresFrontendMessage.Startup.Parameters {
        PostgresFrontendMessage.Startup.Parameters(
            user: self.username,
            database: self.database,
            options: nil,
            replication: .false)
    }
}

private extension Insecure.MD5.Digest {
    
    private static let lowercaseLookup: [UInt8] = [
        UInt8(ascii: "0"), UInt8(ascii: "1"), UInt8(ascii: "2"), UInt8(ascii: "3"),
        UInt8(ascii: "4"), UInt8(ascii: "5"), UInt8(ascii: "6"), UInt8(ascii: "7"),
        UInt8(ascii: "8"), UInt8(ascii: "9"), UInt8(ascii: "a"), UInt8(ascii: "b"),
        UInt8(ascii: "c"), UInt8(ascii: "d"), UInt8(ascii: "e"), UInt8(ascii: "f"),
    ]
    
    func asciiHexDigest() -> [UInt8] {
        var result = [UInt8]()
        result.reserveCapacity(2 * Insecure.MD5Digest.byteCount)
        for byte in self {
            result.append(Self.lowercaseLookup[Int(byte >> 4)])
            result.append(Self.lowercaseLookup[Int(byte & 0x0F)])
        }
        return result
    }
    
    func md5PrefixHexdigest() -> String {
        // TODO: The array should be stack allocated in the best case. But we support down to 5.2.
        //       Given that this method is called only on startup of a new connection, this is an
        //       okay tradeoff for now.
        var result = [UInt8]()
        result.reserveCapacity(3 + 2 * Insecure.MD5Digest.byteCount)
        result.append(UInt8(ascii: "m"))
        result.append(UInt8(ascii: "d"))
        result.append(UInt8(ascii: "5"))
        
        for byte in self {
            result.append(Self.lowercaseLookup[Int(byte >> 4)])
            result.append(Self.lowercaseLookup[Int(byte & 0x0F)])
        }
        return String(decoding: result, as: Unicode.UTF8.self)
    }
}

extension ConnectionStateMachine.TLSConfiguration {
    fileprivate init(_ connection: PSQLConnection.Configuration.TLS) {
        switch connection.base {
        case .disable:
            self = .disable
        case .require:
            self = .require
        case .prefer:
            self = .prefer
        }
    }
}

extension PSQLChannelHandler {
    convenience init(
        configuration: PSQLConnection.Configuration,
        configureSSLCallback: ((Channel) throws -> Void)?)
    {
        self.init(
            configuration: configuration,
            logger: .psqlNoOpLogger,
            configureSSLCallback: configureSSLCallback
        )
    }
}
