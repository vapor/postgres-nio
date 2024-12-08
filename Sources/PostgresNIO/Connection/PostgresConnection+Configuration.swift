import NIOCore
import NIOPosix // inet_pton() et al.
import NIOSSL

extension PostgresConnection {
    /// A configuration object for a connection
    public struct Configuration: Sendable {

        // MARK: - TLS
        
        /// The possible modes of operation for TLS encapsulation of a connection.
        public struct TLS: Sendable {
            // MARK: Initializers
            
            /// Do not try to create a TLS connection to the server.
            public static var disable: Self { .init(base: .disable) }

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, create an insecure connection.
            public static func prefer(_ sslContext: NIOSSLContext) -> Self {
                self.init(base: .prefer(sslContext))
            }

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, fail the connection creation.
            public static func require(_ sslContext: NIOSSLContext) -> Self {
                self.init(base: .require(sslContext))
            }
            
            // MARK: Accessors
            
            /// Whether TLS will be attempted on the connection (`false` only when mode is ``disable``).
            public var isAllowed: Bool {
                if case .disable = self.base { return false }
                else { return true }
            }
            
            /// Whether TLS will be enforced on the connection (`true` only when mode is ``require(_:)``).
            public var isEnforced: Bool {
                if case .require(_) = self.base { return true }
                else { return false }
            }
            
            /// The `NIOSSLContext` that will be used. `nil` when TLS is disabled.
            public var sslContext: NIOSSLContext? {
                switch self.base {
                case .prefer(let context), .require(let context): return context
                case .disable: return nil
                }
            }

            // MARK: Implementation details
            
            enum Base {
                case disable
                case prefer(NIOSSLContext)
                case require(NIOSSLContext)
            }
            let base: Base
            private init(base: Base) { self.base = base }
        }
        
        // MARK: - Connection options
        
        /// Describes options affecting how the underlying connection is made.
        public struct Options: Sendable {
            /// A timeout for connection attempts. Defaults to ten seconds.
            ///
            /// Ignored when using a preexisting communcation channel. (See
            /// ``PostgresConnection/Configuration/init(establishedChannel:username:password:database:)``.)
            public var connectTimeout: TimeAmount
            
            /// The server name to use for certificate validation and SNI (Server Name Indication) when TLS is enabled.
            /// Defaults to none (but see below).
            ///
            /// > When set to `nil`:
            /// If the connection is made to a server over TCP using
            /// ``PostgresConnection/Configuration/init(host:port:username:password:database:tls:)``, the given `host`
            /// is used, unless it was an IP address string. If it _was_ an IP, or the connection is made by any other
            /// method, SNI is disabled.
            public var tlsServerName: String?
            
            /// Whether the connection is required to provide backend key data (internal Postgres stuff).
            ///
            /// This property is provided for compatibility with Amazon RDS Proxy, which requires it to be `false`.
            /// If you are not using Amazon RDS Proxy, you should leave this set to `true` (the default).
            public var requireBackendKeyData: Bool

            /// Additional parameters to send to the server on startup. The name value pairs are added to the initial
            /// startup message that the client sends to the server.
            public var additionalStartupParameters: [(String, String)]

            /// Create an options structure with default values.
            ///
            /// Most users should not need to adjust the defaults.
            public init() {
                self.connectTimeout = .seconds(10)
                self.tlsServerName = nil
                self.requireBackendKeyData = true
                self.additionalStartupParameters = []
            }
        }
        
        // MARK: - Accessors

        /// The hostname to connect to for TCP configurations.
        ///
        /// Always `nil` for other configurations.
        public var host: String? {
            if case let .connectTCP(host, _) = self.endpointInfo { return host }
            else { return nil }
        }
        
        /// The port to connect to for TCP configurations.
        ///
        /// Always `nil` for other configurations.
        public var port: Int? {
            if case let .connectTCP(_, port) = self.endpointInfo { return port }
            else { return nil }
        }
        
        /// The socket path to connect to for Unix domain socket connections.
        ///
        /// Always `nil` for other configurations.
        public var unixSocketPath: String? {
            if case let .bindUnixDomainSocket(path) = self.endpointInfo { return path }
            else { return nil }
        }
        
        /// The `Channel` to use in existing-channel configurations.
        ///
        /// Always `nil` for other configurations.
        public var establishedChannel: Channel? {
            if case let .configureChannel(channel) = self.endpointInfo { return channel }
            else { return nil }
        }
        
        /// The TLS mode to use for the connection. Valid for all configurations.
        ///
        /// See ``TLS-swift.struct``.
        public var tls: TLS
        
        /// Options for handling the communication channel. Most users don't need to change these.
        ///
        /// See ``Options-swift.struct``.
        public var options: Options = .init()
        
        /// The username to connect with.
        public var username: String

        /// The password, if any, for the user specified by ``username``.
        ///
        /// - Warning: `nil` means "no password provided", whereas `""` (the empty string) is a password of zero
        ///   length; these are not the same thing.
        public var password: String?

        /// The name of the database to open.
        ///
        /// - Note: If set to `nil` or an empty string, the provided ``username`` is used.
        public var database: String?

        // MARK: - Initializers

        /// Create a configuration for connecting to a server with a hostname and optional port.
        ///
        /// This specifies a TCP connection. If you're unsure which kind of connection you want, you almost
        /// definitely want this one.
        ///
        /// - Parameters:
        ///   - host: The hostname to connect to.
        ///   - port: The TCP port to connect to (defaults to 5432).
        ///   - tls: The TLS mode to use.
        public init(host: String, port: Int = 5432, username: String, password: String?, database: String?, tls: TLS) {
            self.init(endpointInfo: .connectTCP(host: host, port: port), tls: tls, username: username, password: password, database: database)
        }
            
        /// Create a configuration for connecting to a server through a UNIX domain socket.
        ///
        /// - Parameters:
        ///   - path: The filesystem path of the socket to connect to.
        ///   - tls: The TLS mode to use. Defaults to ``TLS-swift.struct/disable``.
        public init(unixSocketPath: String, username: String, password: String?, database: String?) {
            self.init(endpointInfo: .bindUnixDomainSocket(path: unixSocketPath), tls: .disable, username: username, password: password, database: database)
        }
            
        /// Create a configuration for establishing a connection to a Postgres server over a preestablished
        /// `NIOCore/Channel`.
        ///
        /// This is provided for calling code which wants to manage the underlying connection transport on its
        /// own, such as when tunneling a connection through SSH.
        ///
        /// - Parameters:
        ///   - channel: The `NIOCore/Channel` to use. The channel must already be active and connected to an
        ///     endpoint (i.e. `NIOCore/Channel/isActive` must be `true`).
        ///   - tls: The TLS mode to use. Defaults to ``TLS-swift.struct/disable``.
        public init(establishedChannel channel: Channel, tls: PostgresConnection.Configuration.TLS = .disable, username: String, password: String?, database: String?) {
            self.init(endpointInfo: .configureChannel(channel), tls: tls, username: username, password: password, database: database)
        }

        // MARK: - Implementation details

        enum EndpointInfo {
            case configureChannel(Channel)
            case bindUnixDomainSocket(path: String)
            case connectTCP(host: String, port: Int)
        }

        var endpointInfo: EndpointInfo

        init(endpointInfo: EndpointInfo, tls: TLS, username: String, password: String?, database: String?) {
            self.endpointInfo = endpointInfo
            self.tls = tls
            self.username = username
            self.password = password
            self.database = database
        }
    }
}

// MARK: - Internal config details

extension PostgresConnection {
    /// A configuration object to bring the new ``PostgresConnection.Configuration`` together with
    /// the deprecated configuration.
    ///
    /// TODO: Drop with next major release
    struct InternalConfiguration: Sendable {
        enum Connection {
            case unresolvedTCP(host: String, port: Int)
            case unresolvedUDS(path: String)
            case resolved(address: SocketAddress)
            case bootstrapped(channel: Channel)
        }

        let connection: InternalConfiguration.Connection
        let username: String?
        let password: String?
        let database: String?
        var tls: Configuration.TLS
        let options: Configuration.Options
    }
}

extension PostgresConnection.InternalConfiguration {
    init(_ config: PostgresConnection.Configuration) {
        switch config.endpointInfo {
        case .connectTCP(let host, let port): self.connection = .unresolvedTCP(host: host, port: port)
        case .bindUnixDomainSocket(let path): self.connection = .unresolvedUDS(path: path)
        case .configureChannel(let channel): self.connection = .bootstrapped(channel: channel)
        }
        self.username = config.username
        self.password = config.password
        self.database = config.database
        self.tls = config.tls
        self.options = config.options
    }
    
    var serverNameForTLS: String? {
        // If a name was explicitly configured, always use it.
        if let tlsServerName = self.options.tlsServerName { return tlsServerName }
        
        // Otherwise, if the connection is TCP and the hostname wasn't an IP (not valid in SNI), use that.
        if case .unresolvedTCP(let host, _) = self.connection, !host.isIPAddress() { return host }
        
        // Otherwise, disable SNI
        return nil
    }
}

// originally taken from NIOSSL
private extension String {
    func isIPAddress() -> Bool {
        // We need some scratch space to let inet_pton write into.
        var ipv4Addr = in_addr(), ipv6Addr = in6_addr() // inet_pton() assumes the provided address buffer is non-NULL
        
        /// N.B.: ``String/withCString(_:)`` is much more efficient than directly passing `self`, especially twice.
        return self.withCString { ptr in
            inet_pton(AF_INET, ptr, &ipv4Addr) == 1 || inet_pton(AF_INET6, ptr, &ipv6Addr) == 1
        }
    }
}
