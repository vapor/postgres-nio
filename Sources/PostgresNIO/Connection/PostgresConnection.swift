import NIOCore
import NIOConcurrencyHelpers
import NIOSSL
import Logging

public final class PostgresConnection {
    typealias ID = Int

    struct Configuration {
        struct Authentication {
            var username: String
            var database: String? = nil
            var password: String? = nil

            init(username: String, password: String?, database: String?) {
                self.username = username
                self.database = database
                self.password = password
            }
        }

        struct TLS {
            enum Base {
                case disable
                case prefer(NIOSSLContext)
                case require(NIOSSLContext)
            }

            var base: Base

            private init(_ base: Base) {
                self.base = base
            }

            static var disable: Self = Self.init(.disable)

            static func prefer(_ sslContext: NIOSSLContext) -> Self {
                self.init(.prefer(sslContext))
            }

            static func require(_ sslContext: NIOSSLContext) -> Self {
                self.init(.require(sslContext))
            }
        }

        enum Connection {
            case unresolved(host: String, port: Int)
            case resolved(address: SocketAddress, serverName: String?)
        }

        var connection: Connection

        /// The authentication properties to send to the Postgres server during startup auth handshake
        var authentication: Authentication?

        var tls: TLS

        init(host: String,
             port: Int = 5432,
             username: String,
             database: String? = nil,
             password: String? = nil,
             tls: TLS = .disable
        ) {
            self.connection = .unresolved(host: host, port: port)
            self.authentication = Authentication(username: username, password: password, database: database)
            self.tls = tls
        }

        init(connection: Connection,
             authentication: Authentication?,
             tls: TLS
        ) {
            self.connection = connection
            self.authentication = authentication
            self.tls = tls
        }
    }

    /// The connection's underlying channel
    ///
    /// This should be private, but it is needed for `PostgresConnection` compatibility.
    internal let channel: Channel

    /// The underlying `EventLoop` of both the connection and its channel.
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }

    public var closeFuture: EventLoopFuture<Void> {
        return self.channel.closeFuture
    }

    /// A logger to use in case
    public var logger: Logger {
        get {
            self._logger
        }
        set {
            // ignore
        }
    }

    /// A dictionary to store notification callbacks in
    ///
    /// Those are used when `PostgresConnection.addListener` is invoked. This only lives here since properties
    /// can not be added in extensions. All relevant code lives in `PostgresConnection+Notifications`
    var notificationListeners: [String: [(PostgresListenContext, (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void)]] = [:] {
        willSet {
            self.channel.eventLoop.preconditionInEventLoop()
        }
    }

    public var isClosed: Bool {
        return !self.channel.isActive
    }

    let id: ID

    private var _logger: Logger

    init(channel: Channel, connectionID: ID, logger: Logger) {
        self.channel = channel
        self.id = connectionID
        self._logger = logger

        self.channel.pipeline.handler(type: PSQLChannelHandler.self).whenSuccess { handler in
            handler.notificationDelegate = self
        }
    }
    deinit {
        assert(self.isClosed, "PostgresConnection deinitialized before being closed.")
    }

    static func connect(
        connectionID: ID,
        configuration: PostgresConnection.Configuration,
        logger: Logger,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {

        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(connectionID)"

        // Here we dispatch to the `eventLoop` first before we setup the EventLoopFuture chain, to
        // ensure all `flatMap`s are executed on the EventLoop (this means the enqueuing of the
        // callbacks).
        //
        // This saves us a number of context switches between the thread the Connection is created
        // on and the EventLoop. In addition, it eliminates all potential races between the creating
        // thread and the EventLoop.
        return eventLoop.flatSubmit {
            eventLoop.submit { () throws -> SocketAddress in
                switch configuration.connection {
                case .resolved(let address, _):
                    return address
                case .unresolved(let host, let port):
                    return try SocketAddress.makeAddressResolvingHost(host, port: port)
                }
            }.flatMap { address -> EventLoopFuture<Channel> in
                let bootstrap = ClientBootstrap(group: eventLoop)
                    .channelInitializer { channel in
                        var configureSSLCallback: ((Channel) throws -> ())? = nil

                        switch configuration.tls.base {
                        case .disable:
                            break

                        case .prefer(let sslContext), .require(let sslContext):
                            configureSSLCallback = { channel in
                                channel.eventLoop.assertInEventLoop()

                                let sslHandler = try NIOSSLClientHandler(
                                    context: sslContext,
                                    serverHostname: configuration.sslServerHostname
                                )
                                try channel.pipeline.syncOperations.addHandler(sslHandler, position: .first)
                            }
                        }

                        return channel.pipeline.addHandlers([
                            PSQLChannelHandler(
                                configuration: configuration,
                                logger: logger,
                                configureSSLCallback: configureSSLCallback),
                            PSQLEventsHandler(logger: logger)
                        ])
                    }

                return bootstrap.connect(to: address)
            }.flatMap { channel -> EventLoopFuture<Channel> in
                channel.pipeline.handler(type: PSQLEventsHandler.self).flatMap {
                    eventHandler -> EventLoopFuture<Void> in

                    let startupFuture: EventLoopFuture<Void>
                    if configuration.authentication == nil {
                        startupFuture = eventHandler.readyForStartupFuture
                    } else {
                        startupFuture = eventHandler.authenticateFuture
                    }

                    return startupFuture.flatMapError { error in
                        // in case of an startup error, the connection must be closed and after that
                        // the originating error should be surfaced

                        channel.closeFuture.flatMapThrowing { _ in
                            throw error
                        }
                    }
                }.map { _ in channel }
            }.map { channel in
                PostgresConnection(channel: channel, connectionID: connectionID, logger: logger)
            }.flatMapErrorThrowing { error -> PostgresConnection in
                switch error {
                case is PSQLError:
                    throw error
                default:
                    throw PSQLError.channel(underlying: error)
                }
            }
        }
    }

    // MARK: Query

    func query(_ query: PostgresQuery, logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.id)"
        guard query.binds.count <= Int(Int16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError.tooManyParameters)
        }

        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            query: query,
            logger: logger,
            promise: promise)

        self.channel.write(PSQLTask.extendedQuery(context), promise: nil)

        return promise.futureResult
    }

    // MARK: Prepared statements

    func prepareStatement(_ query: String, with name: String, logger: Logger) -> EventLoopFuture<PSQLPreparedStatement> {
        let promise = self.channel.eventLoop.makePromise(of: RowDescription?.self)
        let context = PrepareStatementContext(
            name: name,
            query: query,
            logger: logger,
            promise: promise)

        self.channel.write(PSQLTask.preparedStatement(context), promise: nil)
        return promise.futureResult.map { rowDescription in
            PSQLPreparedStatement(name: name, query: query, connection: self, rowDescription: rowDescription)
        }
    }

    func execute(_ executeStatement: PSQLExecuteStatement, logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        guard executeStatement.binds.count <= Int(Int16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError.tooManyParameters)
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            executeStatement: executeStatement,
            logger: logger,
            promise: promise)

        self.channel.write(PSQLTask.extendedQuery(context), promise: nil)
        return promise.futureResult
    }

    func close(_ target: CloseTarget, logger: Logger) -> EventLoopFuture<Void> {
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let context = CloseCommandContext(target: target, logger: logger, promise: promise)

        self.channel.write(PSQLTask.closeCommand(context), promise: nil)
        return promise.futureResult
    }


    public func close() -> EventLoopFuture<Void> {
        guard !self.isClosed else {
            return self.eventLoop.makeSucceededFuture(())
        }

        self.channel.close(mode: .all, promise: nil)
        return self.closeFuture
    }
}

// MARK: Connect

extension PostgresConnection {
    static let idGenerator = NIOAtomic.makeAtomic(value: 0)

    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        var tlsFuture: EventLoopFuture<PostgresConnection.Configuration.TLS>

        if let tlsConfiguration = tlsConfiguration {
            tlsFuture = eventLoop.makeSucceededVoidFuture().flatMapBlocking(onto: .global(qos: .default)) {
                try PostgresConnection.Configuration.TLS.require(.init(configuration: tlsConfiguration))
            }
        } else {
            tlsFuture = eventLoop.makeSucceededFuture(.disable)
        }

        return tlsFuture.flatMap { tls in
            let configuration = PostgresConnection.Configuration(
                connection: .resolved(address: socketAddress, serverName: serverHostname),
                authentication: nil,
                tls: tls
            )

            return PostgresConnection.connect(
                connectionID: idGenerator.add(1),
                configuration: configuration,
                logger: logger,
                on: eventLoop
            )
        }.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }

    public func authenticate(
        username: String,
        database: String? = nil,
        password: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres")
    ) -> EventLoopFuture<Void> {
        let authContext = AuthContext(
            username: username,
            password: password,
            database: database)
        let outgoing = PSQLOutgoingEvent.authenticate(authContext)
        self.channel.triggerUserOutboundEvent(outgoing, promise: nil)

        return self.channel.pipeline.handler(type: PSQLEventsHandler.self).flatMap { handler in
            handler.authenticateFuture
        }.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }
}

#if swift(>=5.5) && canImport(_Concurrency)
extension PostgresConnection {
    func close() async throws {
        try await self.close().get()
    }

    func query(_ query: PostgresQuery, logger: Logger, file: String = #file, line: UInt = #line) async throws -> PostgresRowSequence {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.id)"

        do {
            guard query.binds.count <= Int(Int16.max) else {
                throw PSQLError.tooManyParameters
            }
            let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
            let context = ExtendedQueryContext(
                query: query,
                logger: logger,
                promise: promise)

            self.channel.write(PSQLTask.extendedQuery(context), promise: nil)

            return try await promise.futureResult.map({ $0.asyncSequence() }).get()
        }
    }
}
#endif

// MARK: PostgresDatabase

extension PostgresConnection: PostgresDatabase {
    public func send(
        _ request: PostgresRequest,
        logger: Logger
    ) -> EventLoopFuture<Void> {
        guard let command = request as? PostgresCommands else {
            preconditionFailure("\(#function) requires an instance of PostgresCommands. This will be a compile-time error in the future.")
        }

        let resultFuture: EventLoopFuture<Void>

        switch command {
        case .query(let query, let onMetadata, let onRow):
            resultFuture = self.query(query, logger: logger).flatMap { stream in
                return stream.onRow(onRow).map { _ in
                    onMetadata(PostgresQueryMetadata(string: stream.commandTag)!)
                }
            }

        case .queryAll(let query, let onResult):
            resultFuture = self.query(query, logger: logger).flatMap { rows in
                return rows.all().map { allrows in
                    onResult(.init(metadata: PostgresQueryMetadata(string: rows.commandTag)!, rows: allrows))
                }
            }

        case .prepareQuery(let request):
            resultFuture = self.prepareStatement(request.query, with: request.name, logger: self.logger).map {
                request.prepared = PreparedQuery(underlying: $0, database: self)
            }

        case .executePreparedStatement(let preparedQuery, let binds, let onRow):
            var bindings = PostgresBindings(capacity: binds.count)
            binds.forEach { bindings.append($0) }

            let statement = PSQLExecuteStatement(
                name: preparedQuery.underlying.name,
                binds: bindings,
                rowDescription: preparedQuery.underlying.rowDescription
            )

            resultFuture = self.execute(statement, logger: logger).flatMap { rows in
                return rows.onRow(onRow)
            }
        }

        return resultFuture.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }

    public func withConnection<T>(_ closure: (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        closure(self)
    }
}

internal enum PostgresCommands: PostgresRequest {
    case query(PostgresQuery,
               onMetadata: (PostgresQueryMetadata) -> () = { _ in },
               onRow: (PostgresRow) throws -> ())
    case queryAll(PostgresQuery, onResult: (PostgresQueryResult) -> ())
    case prepareQuery(request: PrepareQueryRequest)
    case executePreparedStatement(query: PreparedQuery, binds: [PostgresData], onRow: (PostgresRow) throws -> ())

    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        fatalError("This function must not be called")
    }

    func start() throws -> [PostgresMessage] {
        fatalError("This function must not be called")
    }

    func log(to logger: Logger) {
        fatalError("This function must not be called")
    }
}

// MARK: Notifications

/// Context for receiving NotificationResponse messages on a connection, used for PostgreSQL's `LISTEN`/`NOTIFY` support.
public final class PostgresListenContext {
    var stopper: (() -> Void)?

    /// Detach this listener so it no longer receives notifications. Other listeners, including those for the same channel, are unaffected. `UNLISTEN` is not sent; you are responsible for issuing an `UNLISTEN` query yourself if it is appropriate for your application.
    public func stop() {
        stopper?()
        stopper = nil
    }
}

extension PostgresConnection {
    /// Add a handler for NotificationResponse messages on a certain channel. This is used in conjunction with PostgreSQL's `LISTEN`/`NOTIFY` support: to listen on a channel, you add a listener using this method to handle the NotificationResponse messages, then issue a `LISTEN` query to instruct PostgreSQL to begin sending NotificationResponse messages.
    @discardableResult
    public func addListener(channel: String, handler notificationHandler: @escaping (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void) -> PostgresListenContext {

        let listenContext = PostgresListenContext()

        self.channel.pipeline.handler(type: PSQLChannelHandler.self).whenSuccess { handler in
            if self.notificationListeners[channel] != nil {
                self.notificationListeners[channel]!.append((listenContext, notificationHandler))
            }
            else {
                self.notificationListeners[channel] = [(listenContext, notificationHandler)]
            }
        }

        listenContext.stopper = { [weak self, weak listenContext] in
            // self is weak, since the connection can long be gone, when the listeners stop is
            // triggered. listenContext must be weak to prevent a retain cycle

            self?.channel.eventLoop.execute {
                guard
                    let self = self, // the connection is already gone
                    var listeners = self.notificationListeners[channel] // we don't have the listeners for this topic ¯\_(ツ)_/¯
                else {
                    return
                }

                assert(listeners.filter { $0.0 === listenContext }.count <= 1, "Listeners can not appear twice in a channel!")
                listeners.removeAll(where: { $0.0 === listenContext }) // just in case a listener shows up more than once in a release build, remove all, not just first
                self.notificationListeners[channel] = listeners.isEmpty ? nil : listeners
            }
        }

        return listenContext
    }
}

extension PostgresConnection: PSQLChannelHandlerNotificationDelegate {
    func notificationReceived(_ notification: PostgresBackendMessage.NotificationResponse) {
        self.eventLoop.assertInEventLoop()

        guard let listeners = self.notificationListeners[notification.channel] else {
            return
        }

        let postgresNotification = PostgresMessage.NotificationResponse(
            backendPID: notification.backendPID,
            channel: notification.channel,
            payload: notification.payload)

        listeners.forEach { (listenContext, handler) in
            handler(listenContext, postgresNotification)
        }
    }
}

enum CloseTarget {
    case preparedStatement(String)
    case portal(String)
}

extension PostgresConnection.Configuration {
    var sslServerHostname: String? {
        switch self.connection {
        case .unresolved(let host, _):
            guard !host.isIPAddress() else {
                return nil
            }
            return host
        case .resolved(_, let serverName):
            return serverName
        }
    }
}

// copy and pasted from NIOSSL:
private extension String {
    func isIPAddress() -> Bool {
        // We need some scratch space to let inet_pton write into.
        var ipv4Addr = in_addr()
        var ipv6Addr = in6_addr()

        return self.withCString { ptr in
            return inet_pton(AF_INET, ptr, &ipv4Addr) == 1 ||
                   inet_pton(AF_INET6, ptr, &ipv6Addr) == 1
        }
    }
}
