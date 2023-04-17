import NIOCore
@_implementationOnly import NIOPosix // inet_pton() et al.
import NIOSSL

extension PostgresConnection {
    /// A configuration object for a connection
    public struct Configuration {

        // MARK: - Communication channel

        /// Contains the information necessary to establish the underlying communication channel for a connection.
        public struct Server {
            // MARK: TLS
            
            /// The possible modes of operation for TLS encapsulation of a connection.
            public struct TLS {
                // MARK: Initializers
                
                /// Do not try to create a TLS connection to the server.
                public static var disable: Self = .init(base: .disable)

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
                
                /// The ``NIOSSLContext`` that will be used. `nil` when TLS is disabled.
                public var sslContext: NIOSSLContext? {
                    switch self.base {
                    case .prefer(let context), .require(let context): return context
                    case .disable: return nil
                    }
                }

                // MARK: Implementation details
                
                fileprivate enum Base {
                    case disable
                    case prefer(NIOSSLContext)
                    case require(NIOSSLContext)
                }
                fileprivate let base: Base
                private init(base: Base) { self.base = base }

            }
            
            // MARK: Initializers
            
            /// Create a configuration for connecting to a server with a hostname and optional port.
            ///
            /// This specifies a TCP connection. If you're unsure which kind of connection you want, you almost
            /// definitely want this one.
            ///
            /// - Parameters:
            ///   - host: The hostname to connect to.
            ///   - port: The TCP port to connect to (defaults to 5432).
            ///   - tls: The TLS mode to use.
            public init(host: String, port: Int = 5432, tls: TLS) {
                self.init(base: .connectTCP(host: host, port: port), tls: tls)
            }
            
            /// Create a configuration for connecting to a server through a UNIX domain socket.
            ///
            /// - Parameters:
            ///   - path: The filesystem path of the socket to connect to.
            ///   - tls: The TLS mode to use. Defaults to ``TLS-swift.struct/disable``.
            public static func unixDomainSocket(path: String, tls: TLS = .disable) -> Self {
                .init(base: .bindUnixDomainSocket(path: path), tls: tls)
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
            public static func establishedChannel(_ channel: Channel, tls: TLS = .disable) -> Self {
                .init(base: .configureChannel(channel), tls: tls)
            }
            
            // MARK: Accessors
            
            /// The hostname to connect to for TCP configurations.
            ///
            /// Always `nil` for other configurations.
            public var host: String? {
                if case let .connectTCP(host, _) = self.base { return host }
                else { return nil }
            }
            
            /// The port to connect to for TCP configurations.
            ///
            /// Always `nil` for other configurations.
            public var port: Int? {
                if case let .connectTCP(_, port) = self.base { return port }
                else { return nil }
            }
            
            /// The socket path to connect to for Unix domain socket connections.
            ///
            /// Always `nil` for other configurations.
            public var socketPath: String? {
                if case let .bindUnixDomainSocket(path) = self.base { return path }
                else { return nil }
            }
            
            /// The `NIOCore/Channel` to use in existing-channel configurations.
            ///
            /// Always `nil` for other configurations.
            public var channel: Channel? {
                if case let .configureChannel(channel) = self.base { return channel }
                else { return nil }
            }
            
            /// The TLS mode to use for the connection. Valid for all configurations.
            public let tls: TLS
            
            // MARK: Implementation details
            
            // TODO: Make all of these `fileprivate` once the deprecated stuff is removed
            enum Base {
                case configureChannel(Channel)
                case bindUnixDomainSocket(path: String)
                case connectTCP(host: String, port: Int)
            }
            let base: Base
            init(base: Base, tls: TLS) { (self.base, self.tls) = (base, tls) }
        }
        
        // MARK: - Authentication

        /// Contains authentication information for a connection.
        public struct Authentication {
            // TODO: Make the properties of this structure immutable (removing setters breaks public API).
    
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

            public init(username: String, password: String?, database: String?) {
                self.username = username
                self.password = password
                self.database = database
            }
        }
        
        // MARK: - Connection options
        
        /// Describes options affecting how the underlying connection is made.
        public struct Options {
            /// A timeout for connection attempts.
            ///
            /// Ignored when using a preexisting communcation channel. (See
            /// ``PostgresConnection/Configuration/Server-swift.struct/establishedChannel(_:tls:)``.)
            public let connectTimeout: TimeAmount
            
            /// The name to use for SNI (Server Name Indication) when TLS is enabled.
            ///
            /// > When set to `nil`:
            /// If the connection is made to a server over TCP using
            /// ``PostgresConnection/Configuration/Server-swift.struct/init(host:port:tls:)``, the given `host` is used,
            /// unless it was an IP address string. If it _was_ an IP, or the connection is made by any other method,
            /// SNI is disabled.
            ///
            /// - Warning: This name is not validated in any way; in particular, there is no attempt to check that it
            ///   matches the server's TLS certificate, nor is it in any way impacted by the validation setting in the
            ///   TLS configuration (see ``PostgresConnection/Configuration/Server-swift.struct/TLS-swift.struct``).
            public let tlsServerName: String?
            
            /// Whether the connection is required to provide backend key data (internal Postgres stuff).
            ///
            /// This property is provided for compatibility with Amazon RDS Proxy, which requires it to be `false`.
            /// If you are not using Amazon RDS Proxy, you should probably leave this set to `true` (the default).
            public let requireBackendKeyData: Bool
            
            /// Configure various options for a connection.
            ///
            /// Most users should not need to adjust the defaults.
            ///
            /// - Parameters:
            ///   - connectTimeout: See ``connectTimeout``. Defaults to 10 seconds.
            ///   - tlsServerName: See ``tlsServerName``. Default is `nil`.
            ///   - requireBackendKeyData: See ``requireBackendKeyData``. Default is `true`.
            public init(
                connectTimeout: TimeAmount = .seconds(10),
                tlsServerName: String? = nil,
                requireBackendKeyData: Bool = true
            ) {
                self.connectTimeout = connectTimeout
                self.tlsServerName = tlsServerName
                self.requireBackendKeyData = requireBackendKeyData
            }
        }

        /// Endpoint information for establishing a communication channel.
        ///
        /// See ``Server-swift.struct``.
        public let server: Server
        
        /// Authentication properties to send during the startup auth handshake.
        ///
        /// See ``Authentication-swift.struct``.
        public var authentication: Authentication // TODO: Make this immutable
        
        /// Options for handling the communication channel. Most users don't need to change these.
        ///
        /// See ``Options-swift.struct``.
        public let options: Options
        
        /// Create a complete connection configuration.
        ///
        /// - Parameters:
        ///   - server: See ``Server-swift.struct``.
        ///   - authentication: See ``Authentication-swift.struct``.
        ///   - options: See ``Options-swift.struct``. Most users do not need to adjust any of these.
        public init(
            server: Server,
            authentication: Authentication,
            options: Options = .init()
        ) {
            self.server = server
            self.authentication = authentication
            self.options = options
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
            case unresolvedUDS(path: String)
            case resolved(address: SocketAddress)
            case bootstrapped(channel: Channel)
        }

        let connection: InternalConfiguration.Connection
        let authentication: Configuration.Authentication?
        let tls: Configuration.Server.TLS
        let options: Configuration.Options
    }
}

extension PostgresConnection.InternalConfiguration {
    init(_ config: PostgresConnection.Configuration) {
        switch config.server.base {
        case .connectTCP(let host, let port): self.connection = .unresolvedTCP(host: host, port: port)
        case .bindUnixDomainSocket(let path): self.connection = .unresolvedUDS(path: path)
        case .configureChannel(let channel): self.connection = .bootstrapped(channel: channel)
        }
        self.authentication = config.authentication
        self.tls = config.server.tls
        self.options = config.options
    }
    
    var serverNameForSNI: String? {
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
