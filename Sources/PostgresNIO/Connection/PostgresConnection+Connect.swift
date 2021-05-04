import NIO
import Logging

extension PostgresConnection {
    @available(*, deprecated, message: "Use the new `PostgresConnection.connect(hostname:port:username:password:database:tlsConfiguration:logger:on eventLoop:)` that allows you to specify username, password and database directly when creating the connection.")
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres"),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        
        let coders = PSQLConnection.Configuration.Coders(
            jsonEncoder: PostgresJSONEncoderWrapper(_defaultJSONEncoder),
            jsonDecoder: PostgresJSONDecoderWrapper(_defaultJSONDecoder)
        )
        
        let configuration = PSQLConnection.Configuration(
            connection: .resolved(address: socketAddress, serverName: serverHostname),
            authentication: nil,
            tlsConfiguration: tlsConfiguration,
            coders: coders)
        
        return PSQLConnection.connect(
            configuration: configuration,
            logger: logger,
            on: eventLoop
        ).map { connection in
            PostgresConnection(underlying: connection, logger: logger)
        }.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }
    
    /// Create a new Postgres Connection to a Postgres Server.
    ///
    /// - Parameters:
    ///   - hostname: The server's hostname as domain or IP address.
    ///   - port: The server's port. Defaults to the Postgres default port 5432.
    ///   - username: The username to authenticate with to the remote server.
    ///   - password: An optional password to authenticate with to the remote server.
    ///   - database: The database name to connect to
    ///   - tlsConfiguration: An optional `TLSConfiguration` that configures the TLS connection to the server. If
    ///                       none is provided the connection to the server is created without TLS.
    ///   - logger: A logger to use for connection state changes and action. The logger is only with the `debug` and
    ///             `trace` log level.
    ///   - eventLoop: The SwiftNIO `EventLoop` the connection should be created on.
    /// - Returns: An `EventLoopFuture` which provides the `PSQLConnection` if the connection creation and
    ///            authentification was successful. Otherwise an `Error`.
    public static func connect(
        hostname: String,
        port: Int = 5432,
        username: String,
        password: String?,
        database: String?,
        tlsConfiguration: TLSConfiguration?,
        logger: Logger = Logger(label: "codes.vapor.postgres", factory: { _ in SwiftLogNoOpLogHandler() }),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        
        let coders = PSQLConnection.Configuration.Coders(
            jsonEncoder: PostgresJSONEncoderWrapper(_defaultJSONEncoder),
            jsonDecoder: PostgresJSONDecoderWrapper(_defaultJSONDecoder)
        )
        
        let configuration = PSQLConnection.Configuration(
            connection: .unresolved(host: hostname, port: port),
            authentication: .init(username: username, password: password, database: database),
            tlsConfiguration: tlsConfiguration,
            coders: coders)

        return PSQLConnection.connect(
            configuration: configuration,
            logger: logger,
            on: eventLoop
        ).map { connection in
            PostgresConnection(underlying: connection, logger: logger)
        }.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }
    
    /// Create a new Postgres Connection to a Postgres Server.
    ///
    /// - Parameters:
    ///   - address: The remote server's address as a `SocketAddress`.
    ///   - serverName: The remote server's name to use for domain name certificate verification, if a
    ///                 `TLSConfiguration` is supplied.
    ///   - username: The username to authenticate with to the remote server.
    ///   - password: An optional password to authenticate with to the remote server.
    ///   - database: The database name to connect to
    ///   - tlsConfiguration: An optional `TLSConfiguration` that configures the TLS connection to the server. If
    ///                       none is provided the connection to the server is created without TLS.
    ///   - logger: A logger to use for connection state changes and action. The logger is only with the `debug` and
    ///             `trace` log level.
    ///   - eventLoop: The SwiftNIO `EventLoop` the connection should be created on.
    /// - Returns: An `EventLoopFuture` which provides the `PSQLConnection` if the connection creation and
    ///            authentification was successful. Otherwise an `Error`.
    public static func connect(
        address: SocketAddress,
        serverName: String? = nil,
        username: String,
        password: String?,
        database: String?,
        tlsConfiguration: TLSConfiguration?,
        logger: Logger = Logger(label: "codes.vapor.postgres", factory: { _ in SwiftLogNoOpLogHandler() }),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<PostgresConnection> {
        
        let coders = PSQLConnection.Configuration.Coders(
            jsonEncoder: PostgresJSONEncoderWrapper(_defaultJSONEncoder),
            jsonDecoder: PostgresJSONDecoderWrapper(_defaultJSONDecoder)
        )
        
        let configuration = PSQLConnection.Configuration(
            connection: .resolved(address: address, serverName: serverName),
            authentication: .init(username: username, password: password, database: database),
            tlsConfiguration: tlsConfiguration,
            coders: coders)

        return PSQLConnection.connect(
            configuration: configuration,
            logger: logger,
            on: eventLoop
        ).map { connection in
            PostgresConnection(underlying: connection, logger: logger)
        }.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }
    
}
