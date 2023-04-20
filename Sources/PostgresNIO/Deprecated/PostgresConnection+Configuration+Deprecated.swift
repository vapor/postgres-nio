import NIOCore

extension PostgresConnection.Configuration {
    /// Legacy connection parameters structure. Replaced by ``PostgresConnection/Configuration/host`` etc.
    @available(*, deprecated, message: "Use `Configuration.host` etc. instead.")
    public struct Connection {
        /// See ``PostgresConnection/Configuration/host``.
        public var host: String

        /// See ``PostgresConnection/Configuration/port``.
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

    /// Legacy authentication parameters structure. Replaced by ``PostgresConnection/Configuration/username`` etc.
    @available(*, deprecated, message: "Use `Configuration.username` etc. instead.")
    public struct Authentication {
        /// See ``PostgresConnection/Configuration/username``.
        public var username: String

        /// See ``PostgresConnection/Configuration/password``.
        public var password: String?

        /// See ``PostgresConnection/Configuration/database``.
        public var database: String?

        public init(username: String, database: String?, password: String?) {
            self.username = username
            self.database = database
            self.password = password
        }
     }

    /// Accessor for legacy connection parameters. Replaced by ``PostgresConnection/Configuration/host`` etc.
    @available(*, deprecated, message: "Use `Configuration.host` etc. instead.")
    public var connection: Connection {
        get {
            var conn: Connection
            switch self.endpointInfo {
            case .connectTCP(let host, let port):
                conn = .init(host: host, port: port)
            case .bindUnixDomainSocket(_), .configureChannel(_):
                conn = .init(host: "!invalid!", port: 0) // best we can do, really
            }
            conn.requireBackendKeyData = self.options.requireBackendKeyData
            conn.connectTimeout = self.options.connectTimeout
            return conn
        }
        set {
            self.endpointInfo = .connectTCP(host: newValue.host, port: newValue.port)
            self.options.connectTimeout = newValue.connectTimeout
            self.options.requireBackendKeyData = newValue.requireBackendKeyData
        }
    }
    
    @available(*, deprecated, message: "Use `Configuration.username` etc. instead.")
    public var authentication: Authentication {
        get {
            .init(username: self.username, database: self.database, password: self.password)
        }
        set {
            self.username = newValue.username
            self.password = newValue.password
            self.database = newValue.database
        }
    }
    
    /// Legacy initializer.
    /// Replaced by ``PostgresConnection/Configuration/init(host:port:username:password:database:tls:)`` etc.
    @available(*, deprecated, message: "Use `init(host:port:username:password:database:tls:)` instead.")
    public init(connection: Connection, authentication: Authentication, tls: TLS) {
        self.init(
            host: connection.host, port: connection.port,
            username: authentication.username, password: authentication.password, database: authentication.database,
            tls: tls
        )
        self.options.connectTimeout = connection.connectTimeout
        self.options.requireBackendKeyData = connection.requireBackendKeyData
    }
}
