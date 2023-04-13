import Atomics
import NIOCore
#if canImport(Network)
import NIOTransportServices
#endif
import NIOSSL
import Logging
import NIOPosix

/// A Postgres connection. Use it to run queries against a Postgres server.
///
/// Thread safety is achieved by dispatching all access to shared state onto the underlying EventLoop.
public final class PostgresConnection: @unchecked Sendable {
    /// A Postgres connection ID
    public typealias ID = Int

    /// A configuration object for a connection
    public struct Configuration {
        /// A structure to configure the connection's authentication properties
        public struct Authentication {
            /// The username to connect with.
            ///
            /// - Default: postgres
            public var username: String

            /// The database to open on the server
            ///
            /// - Default: `nil`
            public var database: Optional<String>

            /// The database user's password.
            ///
            /// - Default: `nil`
            public var password: Optional<String>

            public init(username: String, database: String?, password: String?) {
                self.username = username
                self.database = database
                self.password = password
            }
        }

        public struct TLS {
            enum Base {
                case disable
                case prefer(NIOSSLContext)
                case require(NIOSSLContext)
            }

            var base: Base

            private init(_ base: Base) {
                self.base = base
            }

            /// Do not try to create a TLS connection to the server.
            public static var disable: Self = Self.init(.disable)

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, create an insecure connection.
            public static func prefer(_ sslContext: NIOSSLContext) -> Self {
                self.init(.prefer(sslContext))
            }

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, fail the connection creation.
            public static func require(_ sslContext: NIOSSLContext) -> Self {
                self.init(.require(sslContext))
            }
        }

        public struct Connection {
            // MARK: Guts
            
            enum Base {
                case configureChannel(Channel, serverName: String?)
                case bindUnixDomainSocket(path: String, serverName: String?)
                case connectTCP(host: String, port: Int)
            }
            
            var base: Base // TODO: Make this immutable once the deprecated properties are removed
            var realConnectTimeout: TimeAmount = .seconds(10) // need a separate property so we can deprecate the public one's setter
            var realRequireBackendKeyData: Bool = true // need a separate property so we can deprecate the public one's setter
            
            private init(base: Base, connectTimeout: TimeAmount, requireBackendKeyData: Bool) {
                self.base = base
                self.realConnectTimeout = connectTimeout
                self.realRequireBackendKeyData = requireBackendKeyData
            }

            // MARK: Initializers

            /// Create a configuration for connecting to a server over TCP.
            ///
            /// - Parameters:
            ///   - host: The hostname to connect to.
            ///   - port: The TCP port to connect to (defaults to 5432).
            ///   - connectTimeout: See ``connectTimeout``.
            ///   - requireBackendKeyData: See ``requireBackendKeyData``.
            public static func tcp(
                host: String,
                port: Int = 5432,
                connectTimeout: TimeAmount = .seconds(10),
                requireBackendKeyData: Bool = true
            ) -> Self {
                .init(
                    base: .connectTCP(host: host, port: port),
                    connectTimeout: connectTimeout,
                    requireBackendKeyData: true
                )
            }
            
            /// Create a configuration for connecting to a server through a UNIX domain socket.
            ///
            /// - Parameters:
            ///   - path: The filesystem path of the socket to connect to.
            ///   - connectTimeout: See ``connectTimeout``.
            ///   - tlsHostname: See ``tlsHostname``.
            ///   - requireBackendKeyData: See ``requireBackendKeyData``.
            public static func unixDomainSocket(
                path: String,
                connectTimeout: TimeAmount = .seconds(10),
                tlsHostname: String? = nil,
                requireBackendKeyData: Bool = true
            ) -> Self {
                .init(
                    base: .bindUnixDomainSocket(path: path, serverName: tlsHostname),
                    connectTimeout: connectTimeout,
                    requireBackendKeyData: requireBackendKeyData
                )
            }
            
            /// Create a configuration for establishing a connection to a Postgres server over a
            /// preestablished ``NIOCore/Channel``.
            ///
            /// This is provided for calling code which wants to manage the underlying connection
            /// transport on its own, such as when tunneling a connection through SSH.
            ///
            /// - Parameters:
            ///   - channel: The ``NIOCore/Channel`` to use. The channel must already be active and
            ///     connected to an endpoint.
            ///   - tlsHostname: See ``tlsHostname``.
            ///   - requireBackendKeyData: See ``requireBackendKeyData``.
            public static func establishedChannel(
                channel: Channel,
                tlsHostname: String? = nil,
                requireBackendKeyData: Bool = true
            ) -> Self {
                .init(
                    base: .configureChannel(channel, serverName: tlsHostname),
                    connectTimeout: .seconds(10),
                    requireBackendKeyData: requireBackendKeyData
                )
            }
            
            // MARK: Getters
            
            /// The hostname to connect to for TCP configurations. Always `nil` for other configurations.
            public var hostname: String? {
                switch self.base {
                case .connectTCP(let host, _): return host
                default: return nil
                }
            }
            
            /// The port to connect to for TCP configurations. Always `nil` for other configurations.
            public var tcpPort: Int? {
                switch self.base {
                case .connectTCP(_, let port): return port
                default: return nil
                }
            }
            
            /// The socket path to connect to for Unix domain socket connections. Always `nil` for other configurations.
            public var unixSocketPath: String? {
                switch self.base {
                case .bindUnixDomainSocket(let path, _): return path
                default: return nil
                }
            }
            
            /// The ``NIOCore/Channel`` to use in existing-channel configurations. Always `nil` for other configurations.
            public var establishedChannel: Channel? {
                switch self.base {
                case .configureChannel(let channel, _): return channel
                default: return nil
                }
            }
            
            /// Specifies a timeout for connection attempts.
            ///
            /// > Default: 10 seconds
            ///
            /// - Note: This setting has no effect for existing-channel configurations.
            ///
            /// - Warning: Mutating this property on an existing configuration is no longer supported. Provide the
            ///    timeout when calling one of the `static` configuration creation methods instead.
            public var connectTimeout: TimeAmount {
                get { self.realConnectTimeout }
                @available(*, deprecated, message: "Provide connection timeout as a parameter when creating the configuration.")
                set { self.realConnectTimeout = newValue }
            }

            /// Whether the connection is required to provide ``BackendKeyData``.
            ///
            /// This property is provided for compatibility with Amazon RDS Proxy, which requires it to be `false`.
            /// If you are not using Amazon RDS Proxy, you probably don't need this.
            ///
            /// - Warning: Mutating this property on an existing configuration is no longer supported. Provide this
            ///   flag when calling one of the `static` configuration creation methods instead.
            public var requireBackendKeyData: Bool {
                get { self.realRequireBackendKeyData }
                @available(*, deprecated, message: "Provide the backend key data flag as a parameter when creating the configuration.")
                set { self.realRequireBackendKeyData = newValue }
            }

            /// The server name to use for SNI when a connection initiates TLS, if one was provided.
            ///
            /// For TCP configurations, this is always the same as ``hostname``.
            ///
            /// - Note: This presence or absence of this value neither indicates nor affects whether
            ///   TLS is disabled, requested, or required for a connection, regardless of type.
            public var tlsHostname: String? {
                switch self.base {
                case .connectTCP(let host, _): return host
                case .bindUnixDomainSocket(_, let serverName): return serverName
                case .configureChannel(_, let serverName): return serverName
                }
            }
            
            // MARK: Deprecated

            /// Create a configuration for connecting to a server over TCP.
            ///
            /// - Warning: This is a legacy initializer provided for compatibility. Use the
            ///   ``tcp(host:port:connectTimeout:requireBackendKeyData:)`` method instead.
            ///
            /// - Parameters:
            ///   - host: The hostname to connect to.
            ///   - port: The TCP port to connect to (defaults to 5432).
            @available(*, deprecated, message: "Use `.tcp(host:port:connectTimeout:requireBackendKeyData:)` instead.")
            public init(host: String, port: Int = 5432) {
                self = .tcp(host: host, port: port)
            }

            /// The server to connect to.
            ///
            /// - Warning: This is a legacy property. To avoid unexpected crashes, the getter will return an
            ///   empty string and the setter will have no effect when used with non-TCP configurations. Use
            ///   the ``hostname`` property instead. (There is no replacement for the setter.)
            public var host: String {
                @available(*, deprecated, message: "Use `hostname` instead.")
                get { self.hostname ?? "" }
                @available(*, deprecated, message: "This structure should be treated as immutable.")
                set {
                    if case .connectTCP(_, let port) = self.base {
                        self.base = .connectTCP(host: newValue, port: port)
                    }
                }
            }

            /// The server port to connect to.
            ///
            /// - Warning: This is a legacy property. To avoid unexpected crashes, the getter will return zero
            ///   and the setter will have no effect when used with non-TCP configurations. Use the ``tcpPort``
            ///   property instead. (There is no replacement for the setter.)
            public var port: Int {
                @available(*, deprecated, message: "Use `tcpPort` instead.")
                get { self.tcpPort ?? 0 }
                @available(*, deprecated, message: "This structure should be treated as immutable.")
                set {
                    if case .connectTCP(let host, _) = self.base {
                        self.base = .connectTCP(host: host, port: newValue)
                    }
                }
            }
        }

        public var connection: Connection

        /// The authentication properties to send to the Postgres server during startup auth handshake
        public var authentication: Authentication

        public var tls: TLS

        public init(
            connection: Connection,
            authentication: Authentication,
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
    }
    deinit {
        assert(self.isClosed, "PostgresConnection deinitialized before being closed.")
    }

    func start(configuration: InternalConfiguration) -> EventLoopFuture<Void> {
        // 1. configure handlers

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

        let channelHandler = PostgresChannelHandler(
            configuration: configuration,
            logger: logger,
            configureSSLCallback: configureSSLCallback
        )
        channelHandler.notificationDelegate = self

        let eventHandler = PSQLEventsHandler(logger: logger)

        // 2. add handlers

        do {
            try self.channel.pipeline.syncOperations.addHandler(eventHandler)
            try self.channel.pipeline.syncOperations.addHandler(channelHandler, position: .before(eventHandler))
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }

        let startupFuture: EventLoopFuture<Void>
        if configuration.authentication == nil {
            startupFuture = eventHandler.readyForStartupFuture
        } else {
            startupFuture = eventHandler.authenticateFuture
        }

        // 3. wait for startup future to succeed.

        return startupFuture.flatMapError { error in
            // in case of an startup error, the connection must be closed and after that
            // the originating error should be surfaced

            self.channel.closeFuture.flatMapThrowing { _ in
                throw error
            }
        }
    }

    /// Create a new connection to a Postgres server
    ///
    /// - Parameters:
    ///   - eventLoop: The `EventLoop` the request shall be created on
    ///   - configuration: A ``Configuration`` that shall be used for the connection
    ///   - connectionID: An `Int` id, used for metadata logging
    ///   - logger: A logger to log background events into
    /// - Returns: A SwiftNIO `EventLoopFuture` that will provide a ``PostgresConnection``
    ///            at a later point in time.
    public static func connect(
        on eventLoop: EventLoop,
        configuration: PostgresConnection.Configuration,
        id connectionID: ID,
        logger: Logger
    ) -> EventLoopFuture<PostgresConnection> {
        self.connect(
            connectionID: connectionID,
            configuration: .init(configuration),
            logger: logger,
            on: eventLoop
        )
    }

    static func connect(
        connectionID: ID,
        configuration: PostgresConnection.InternalConfiguration,
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
        return eventLoop.flatSubmit { () -> EventLoopFuture<PostgresConnection> in
            let connectFuture: EventLoopFuture<Channel>
            let bootstrap = self.makeBootstrap(on: eventLoop, configuration: configuration)

            switch configuration.connection {
            case .resolved(let address, _):
                connectFuture = bootstrap.connect(to: address)
            case .unresolvedTCP(let host, let port):
                connectFuture = bootstrap.connect(host: host, port: port)
            case .unresolvedUDS(let path, _):
                connectFuture = bootstrap.connect(unixDomainSocketPath: path)
            case .bootstrapped(let channel, _):
                guard channel.isActive else {
                    return eventLoop.makeFailedFuture(PSQLError.channel(underlying: ChannelError.alreadyClosed))
                }
                // TODO: Are there drawbacks to creating a bootstrap we don't end up using?
                connectFuture = eventLoop.makeSucceededFuture(channel)
            }

            return connectFuture.flatMap { channel -> EventLoopFuture<PostgresConnection> in
                let connection = PostgresConnection(channel: channel, connectionID: connectionID, logger: logger)
                return connection.start(configuration: configuration).map { _ in connection }
            }.flatMapErrorThrowing { error -> PostgresConnection in
                switch error {
                case is PSQLError:
                    throw error
                default:
                    throw PSQLError.connectionError(underlying: error)
                }
            }
        }
    }

    static func makeBootstrap(
        on eventLoop: EventLoop,
        configuration: PostgresConnection.InternalConfiguration
    ) -> NIOClientTCPBootstrapProtocol {
        #if canImport(Network)
        if let tsBootstrap = NIOTSConnectionBootstrap(validatingGroup: eventLoop) {
            return tsBootstrap.connectTimeout(configuration.connectTimeout)
        }
        #endif

        if let nioBootstrap = ClientBootstrap(validatingGroup: eventLoop) {
            return nioBootstrap.connectTimeout(configuration.connectTimeout)
        }

        fatalError("No matching bootstrap found")
    }

    // MARK: Query

    private func queryStream(_ query: PostgresQuery, logger: Logger) -> EventLoopFuture<PSQLRowStream> {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.id)"
        guard query.binds.count <= Int(UInt16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError(code: .tooManyParameters, query: query))
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
        guard executeStatement.binds.count <= Int(UInt16.max) else {
            return self.channel.eventLoop.makeFailedFuture(PSQLError(code: .tooManyParameters))
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


    /// Closes the connection to the server.
    ///
    /// - Returns: An EventLoopFuture that is succeeded once the connection is closed.
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
    static let idGenerator = ManagedAtomic(0)

    @available(*, deprecated,
        message: "Use the new connect method that allows you to connect and authenticate in a single step",
        renamed: "connect(on:configuration:id:logger:)"
    )
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
            let configuration = PostgresConnection.InternalConfiguration(
                connection: .resolved(address: socketAddress, serverName: serverHostname),
                connectTimeout: .seconds(10),
                authentication: nil,
                tls: tls,
                requireBackendKeyData: true
            )

            return PostgresConnection.connect(
                connectionID: self.idGenerator.wrappingIncrementThenLoad(ordering: .relaxed),
                configuration: configuration,
                logger: logger,
                on: eventLoop
            )
        }.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }

    @available(*, deprecated,
        message: "Use the new connect method that allows you to connect and authenticate in a single step",
        renamed: "connect(on:configuration:id:logger:)"
    )
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

// MARK: Async/Await Interface

extension PostgresConnection {

    /// Creates a new connection to a Postgres server.
    ///
    /// - Parameters:
    ///   - eventLoop: The `EventLoop` the request shall be created on
    ///   - configuration: A ``Configuration`` that shall be used for the connection
    ///   - connectionID: An `Int` id, used for metadata logging
    ///   - logger: A logger to log background events into
    /// - Returns: An established  ``PostgresConnection`` asynchronously that can be used to run queries.
    public static func connect(
        on eventLoop: EventLoop,
        configuration: PostgresConnection.Configuration,
        id connectionID: ID,
        logger: Logger
    ) async throws -> PostgresConnection {
        try await self.connect(
            connectionID: connectionID,
            configuration: .init(configuration),
            logger: logger,
            on: eventLoop
        ).get()
    }

    /// Closes the connection to the server.
    public func close() async throws {
        try await self.close().get()
    }

    /// Run a query on the Postgres server the connection is connected to.
    ///
    /// - Parameters:
    ///   - query: The ``PostgresQuery`` to run
    ///   - logger: The `Logger` to log into for the query
    ///   - file: The file, the query was started in. Used for better error reporting.
    ///   - line: The line, the query was started in. Used for better error reporting.
    /// - Returns: A ``PostgresRowSequence`` containing the rows the server sent as the query result.
    ///            The sequence  be discarded.
    @discardableResult
    public func query(
        _ query: PostgresQuery,
        logger: Logger,
        file: String = #fileID,
        line: Int = #line
    ) async throws -> PostgresRowSequence {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.id)"

        guard query.binds.count <= Int(UInt16.max) else {
            throw PSQLError(code: .tooManyParameters, query: query, file: file, line: line)
        }
        let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
        let context = ExtendedQueryContext(
            query: query,
            logger: logger,
            promise: promise
        )

        self.channel.write(PSQLTask.extendedQuery(context), promise: nil)

        do {
            return try await promise.futureResult.map({ $0.asyncSequence() }).get()
        } catch var error as PSQLError {
            error.file = file
            error.line = line
            error.query = query
            throw error // rethrow with more metadata
        }
    }
}

// MARK: EventLoopFuture interface

extension PostgresConnection {

    /// Run a query on the Postgres server the connection is connected to and collect all rows.
    ///
    /// - Parameters:
    ///   - query: The ``PostgresQuery`` to run
    ///   - logger: The `Logger` to log into for the query
    ///   - file: The file, the query was started in. Used for better error reporting.
    ///   - line: The line, the query was started in. Used for better error reporting.
    /// - Returns: An EventLoopFuture, that allows access to the future ``PostgresQueryResult``.
    public func query(
        _ query: PostgresQuery,
        logger: Logger,
        file: String = #fileID,
        line: Int = #line
    ) -> EventLoopFuture<PostgresQueryResult> {
        self.queryStream(query, logger: logger).flatMap { rowStream in
            rowStream.all().flatMapThrowing { rows -> PostgresQueryResult in
                guard let metadata = PostgresQueryMetadata(string: rowStream.commandTag) else {
                    throw PSQLError.invalidCommandTag(rowStream.commandTag)
                }
                return PostgresQueryResult(metadata: metadata, rows: rows)
            }
        }.enrichPSQLError(query: query, file: file, line: line)
    }

    /// Run a query on the Postgres server the connection is connected to and iterate the rows in a callback.
    ///
    /// - Note: This API does not support back-pressure. If you need back-pressure please use the query
    ///         API, that supports structured concurrency.
    /// - Parameters:
    ///   - query: The ``PostgresQuery`` to run
    ///   - logger: The `Logger` to log into for the query
    ///   - file: The file, the query was started in. Used for better error reporting.
    ///   - line: The line, the query was started in. Used for better error reporting.
    ///   - onRow: A closure that is invoked for every row.
    /// - Returns: An EventLoopFuture, that allows access to the future ``PostgresQueryMetadata``.
    public func query(
        _ query: PostgresQuery,
        logger: Logger,
        file: String = #fileID,
        line: Int = #line,
        _ onRow: @escaping (PostgresRow) throws -> ()
    ) -> EventLoopFuture<PostgresQueryMetadata> {
        self.queryStream(query, logger: logger).flatMap { rowStream in
            rowStream.onRow(onRow).flatMapThrowing { () -> PostgresQueryMetadata in
                guard let metadata = PostgresQueryMetadata(string: rowStream.commandTag) else {
                    throw PSQLError.invalidCommandTag(rowStream.commandTag)
                }
                return metadata
            }
        }.enrichPSQLError(query: query, file: file, line: line)
    }
}

// MARK: PostgresDatabase conformance

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
            resultFuture = self.queryStream(query, logger: logger).flatMap { stream in
                return stream.onRow(onRow).map { _ in
                    onMetadata(PostgresQueryMetadata(string: stream.commandTag)!)
                }
            }

        case .queryAll(let query, let onResult):
            resultFuture = self.queryStream(query, logger: logger).flatMap { rows in
                return rows.all().map { allrows in
                    onResult(.init(metadata: PostgresQueryMetadata(string: rows.commandTag)!, rows: allrows))
                }
            }

        case .prepareQuery(let request):
            resultFuture = self.prepareStatement(request.query, with: request.name, logger: logger).map {
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

        self.channel.pipeline.handler(type: PostgresChannelHandler.self).whenSuccess { handler in
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

extension PostgresConnection.InternalConfiguration {
    var sslServerHostname: String? {
        switch self.connection {
        case .unresolvedTCP(let host, _):
            guard !host.isIPAddress() else {
                // Providing an IP address to SNI is not valid; disable SNI instead.
                return nil
            }
            return host
        case .unresolvedUDS(_, let serverName), .resolved(_, let serverName), .bootstrapped(_, let serverName):
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

extension PostgresConnection {
    /// A configuration object to bring the new ``PostgresConnection.Configuration`` together with
    /// the deprecated configuration.
    ///
    /// TODO: Drop with next major release
    struct InternalConfiguration {
        enum Connection {
            case unresolvedTCP(host: String, port: Int)
            case unresolvedUDS(path: String, serverName: String?)
            case resolved(address: SocketAddress, serverName: String?)
            case bootstrapped(channel: Channel, serverName: String?)
        }

        var connection: Connection
        var connectTimeout: TimeAmount

        var authentication: Configuration.Authentication?

        var tls: Configuration.TLS
        
        var requireBackendKeyData: Bool
    }
}

extension PostgresConnection.InternalConfiguration {
    init(_ config: PostgresConnection.Configuration) {
        self.authentication = config.authentication
        switch config.connection.base {
        case .connectTCP(let host, let port): self.connection = .unresolvedTCP(host: host, port: port)
        case .bindUnixDomainSocket(let path, let serverName): self.connection = .unresolvedUDS(path: path, serverName: serverName)
        case .configureChannel(let channel, let serverName): self.connection = .bootstrapped(channel: channel, serverName: serverName)
        }
        self.connectTimeout = config.connection.connectTimeout
        self.tls = config.tls
        self.requireBackendKeyData = config.connection.requireBackendKeyData
    }
}

extension EventLoopFuture {
    func enrichPSQLError(query: PostgresQuery, file: String, line: Int) -> EventLoopFuture<Value> {
        return self.flatMapErrorThrowing { error in
            if var error = error as? PSQLError {
                error.file = file
                error.line = line
                error.query = query
                throw error
            } else {
                throw error
            }
        }
    }
}
