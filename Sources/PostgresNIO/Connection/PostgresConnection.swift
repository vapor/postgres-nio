import NIOCore
import NIOSSL
import Logging

public final class PostgresConnection {
    let underlying: PSQLConnection
    
    public var eventLoop: EventLoop {
        return self.underlying.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return self.underlying.channel.closeFuture
    }
    
    /// A logger to use in case 
    public var logger: Logger
    
    /// A dictionary to store notification callbacks in
    ///
    /// Those are used when `PostgresConnection.addListener` is invoked. This only lives here since properties
    /// can not be added in extensions. All relevant code lives in `PostgresConnection+Notifications`
    var notificationListeners: [String: [(PostgresListenContext, (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void)]] = [:] {
        willSet {
            self.underlying.channel.eventLoop.preconditionInEventLoop()
        }
    }

    public var isClosed: Bool {
        return !self.underlying.channel.isActive
    }
    
    init(underlying: PSQLConnection, logger: Logger) {
        self.underlying = underlying
        self.logger = logger
        
        self.underlying.channel.pipeline.handler(type: PSQLChannelHandler.self).whenSuccess { handler in
            handler.notificationDelegate = self
        }
    }
    
    public func close() -> EventLoopFuture<Void> {
        return self.underlying.close()
    }
}

// MARK: Connect

extension PostgresConnection {
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        var tlsFuture: EventLoopFuture<PSQLConnection.Configuration.TLS>

        if let tlsConfiguration = tlsConfiguration {
            tlsFuture = eventLoop.makeSucceededVoidFuture().flatMapBlocking(onto: .global(qos: .default)) {
                try PSQLConnection.Configuration.TLS.require(.init(configuration: tlsConfiguration))
            }
        } else {
            tlsFuture = eventLoop.makeSucceededFuture(.disable)
        }

        return tlsFuture.flatMap { tls in
            let configuration = PSQLConnection.Configuration(
                connection: .resolved(address: socketAddress, serverName: serverHostname),
                authentication: nil,
                tls: tls
            )

            return PSQLConnection.connect(
                configuration: configuration,
                logger: logger,
                on: eventLoop
            )
        }.map { connection in
            PostgresConnection(underlying: connection, logger: logger)
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
        self.underlying.channel.triggerUserOutboundEvent(outgoing, promise: nil)

        return self.underlying.channel.pipeline.handler(type: PSQLEventsHandler.self).flatMap { handler in
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
        logger[postgresMetadataKey: .connectionID] = "\(self.underlying.connectionID)"

        do {
            guard query.binds.count <= Int(Int16.max) else {
                throw PSQLError.tooManyParameters
            }
            let promise = self.underlying.channel.eventLoop.makePromise(of: PSQLRowStream.self)
            let context = ExtendedQueryContext(
                query: query,
                logger: logger,
                promise: promise)

            self.underlying.channel.write(PSQLTask.extendedQuery(context), promise: nil)

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
            resultFuture = self.underlying.query(query, logger: logger).flatMap { stream in
                return stream.onRow(onRow).map { _ in
                    onMetadata(PostgresQueryMetadata(string: stream.commandTag)!)
                }
            }

        case .queryAll(let query, let onResult):
            resultFuture = self.underlying.query(query, logger: logger).flatMap { rows in
                return rows.all().map { allrows in
                    onResult(.init(metadata: PostgresQueryMetadata(string: rows.commandTag)!, rows: allrows))
                }
            }

        case .prepareQuery(let request):
            resultFuture = self.underlying.prepareStatement(request.query, with: request.name, logger: self.logger).map {
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

            resultFuture = self.underlying.execute(statement, logger: logger).flatMap { rows in
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

        self.underlying.channel.pipeline.handler(type: PSQLChannelHandler.self).whenSuccess { handler in
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

            self?.underlying.channel.eventLoop.execute {
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
        self.underlying.eventLoop.assertInEventLoop()

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
