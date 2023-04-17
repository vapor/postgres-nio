import NIOCore

extension PostgresConnection.Configuration {
    /// Legacy connection parameters structure. Replaced by ``PostgresConnection/Configuration/Server-swift.struct``.
    @available(*, deprecated, message: "Use `Configuration.Server` instead.")
    public struct Connection {
        /// See ``PostgresConnection/Configuration/Server-swift.struct/host``.
        public var host: String

        /// See ``PostgresConnection/Configuration/Server-swift.struct/port``.
        public var port: Int

        /// See ``PostgresConnection/Configuration/Options-swift.struct/requireBackendKeyData``.
        public var requireBackendKeyData: Bool = true

        /// See ``PostgresConnection/Configuration/Options-swift.struct/connectTimeout``.
        public var connectTimeout: TimeAmount = .seconds(10)

        /// Create a configuration for connecting to a server.
        ///
        /// - Parameters:
        ///   - host: The hostname to connect to.
        ///   - port: The TCP port to connect to (defaults to 5432).
        public init(host: String, port: Int = 5432) {
            self.host = host
            self.port = port
        }
    }
    
    /// Legacy location of ``PostgresConnection/Configuration/Server-swift.struct/TLS-swift.struct``.
    @available(*, deprecated, message: "Use `Configuration.Server.TLS` instead.")
    public typealias TLS = PostgresConnection.Configuration.Server.TLS
    
    /// Accessor for legacy connection parameters. Replaced by ``PostgresConnection/Configuration/server-swift.property``.
    @available(*, deprecated, message: "Use `Configuration.server` instead.")
    public var connection: Connection {
        get {
            switch self.server.base {
            case .connectTCP(let host, let port):
                var conn = Connection(host: host, port: port)
                conn.requireBackendKeyData = self.options.requireBackendKeyData
                conn.connectTimeout = self.options.connectTimeout
                return conn
            case .bindUnixDomainSocket(_), .configureChannel(_):
                return .init(host: "!invalid!", port: 0) // best we can do, really
            }
        }
        set {
            self = .init(
                server: .init(host: newValue.host, port: newValue.port, tls: self.server.tls),
                authentication: self.authentication,
                options: .init(
                    connectTimeout: newValue.connectTimeout,
                    tlsServerName: self.options.tlsServerName,
                    requireBackendKeyData: newValue.requireBackendKeyData
                )
            )
        }
    }
    
    /// Accessor for legacy TLS mode. Replaced by ``PostgresConnection/Configuration/Server-swift.struct/tls-swift.property``.
    @available(*, deprecated, message: "Use `Configuration.server.tls` instead.")
    public var tls: TLS {
        get { self.server.tls }
        set {
            self = .init(
                server: .init(base: self.server.base, tls: newValue),
                authentication: self.authentication, options: self.options
            )
        }
    }

    /// Legacy initializer. Replaced by ``PostgresConnection/Configuration/init(server:authentication:options:)``.
    @available(*, deprecated, message: "Use `init(server:authentication:options:)` instead.")
    public init(connection: Connection, authentication: Authentication, tls: TLS) {
        self.init(
            server: .init(host: connection.host, port: connection.port, tls: tls),
            authentication: authentication,
            options: .init(connectTimeout: connection.connectTimeout, requireBackendKeyData: connection.requireBackendKeyData)
        )
    }
}

extension PostgresConnection.Configuration.Authentication {
    /// Old initializer for the original, less intitive, order of parameters (database and password switched places).
    /// Replaced by ``init(username:password:database:)``.
    @available(*, deprecated, message: "Use ``init(username:password:database:)`` instead.")
    public init(username: String, database: String?, password: String?) {
        self.init(username: username, password: password, database: database)
    }
}
