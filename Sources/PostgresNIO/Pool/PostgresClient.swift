import NIOCore
import NIOSSL
import Logging
import _ConnectionPoolModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
@_spi(ConnectionPool)
public final class PostgresClient: Sendable {

    public struct Configuration: Sendable {
        public struct Pool: Sendable {
            /// The minimum number of connections to preserve in the pool.
            ///
            /// If the pool is mostly idle and the Postgres servers closes idle connections,
            /// the `PostgresClient` will initiate new outbound connections proactively to avoid
            /// the number of available connections dropping below this number.
            public var minimumConnectionCount: Int = 0

            /// The maximum number of connections to for this pool, to be preserved.
            public var maximumConnectionSoftLimit: Int = 10

            public var maximumConnectionHardLimit: Int = 10

            public var maxConsecutivePicksFromEventLoopQueue: UInt8 = 16

            public var connectionIdleTimeout: Duration = .seconds(60)

            public var keepAliveFrequency: Duration = .seconds(30)

            public var keepAliveQuery: PostgresQuery = "SELECT 1;"

            public init() {}
        }

        public struct Authentication: Sendable {
            /// The username to connect with.
            ///
            /// - Default: postgres
            public var username: String = "postgres"

            /// The database to open on the server
            ///
            /// - Default: `nil`
            public var database: Optional<String> = "postgres"

            /// The database user's password.
            ///
            /// - Default: `nil`
            public var password: Optional<String> = "password"

            public init() {}
        }

        public struct TLS: Sendable {
            enum Base {
                case disable
                case prefer(NIOSSL.TLSConfiguration)
                case require(NIOSSL.TLSConfiguration)
            }

            var base: Base

            private init(_ base: Base) {
                self.base = base
            }

            /// Do not try to create a TLS connection to the server.
            public static var disable: Self = Self.init(.disable)

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, create an insecure connection.
            public static func prefer(_ sslContext: NIOSSL.TLSConfiguration) -> Self {
                self.init(.prefer(sslContext))
            }

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, fail the connection creation.
            public static func require(_ sslContext: NIOSSL.TLSConfiguration) -> Self {
                self.init(.require(sslContext))
            }
        }

        public struct Server: Sendable {
            /// The server to connect to
            ///
            /// - Default: localhost
            public var host: String = "localhost"

            /// The server port to connect to.
            ///
            /// - Default: 5432
            public var port: Int = 5432

            /// Require connection to provide `BackendKeyData`.
            /// For use with Amazon RDS Proxy, this must be set to false.
            ///
            /// - Default: true
            public var requireBackendKeyData: Bool = true

            /// Specifies a timeout to apply to a connection attempt.
            ///
            /// - Default: 10 seconds
            public var connectTimeout: TimeAmount = .seconds(10)
        }

        public var server = Server()
        public var authentication = Authentication()

        public var pool = Pool()
        public var tls = TLS.prefer(.makeClientConfiguration())

        public init() {}
    }

    typealias Pool = ConnectionPool<
        PostgresConnection,
        PostgresConnection.ID,
        ConnectionIDGenerator,
        ConnectionRequest<PostgresConnection>,
        ConnectionRequest.ID,
        PostgresKeepAliveBehavor,
        PostgresClientMetrics,
        ContinuousClock
    >

    let pool: Pool

    public init(configuration: Configuration, eventLoopGroup: EventLoopGroup, backgroundLogger: Logger) throws {
        let connectionConfig = try PostgresConnection.Configuration(configuration)
        self.pool = ConnectionPool(
            configuration: .init(configuration),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<PostgresConnection>.self,
            keepAliveBehavior: .init(configuration, logger: backgroundLogger),
            observabilityDelegate: .init(logger: backgroundLogger),
            clock: ContinuousClock()
        ) { (connectionID, pool) in
            var connectionLogger = backgroundLogger
            connectionLogger[postgresMetadataKey: .connectionID] = "\(connectionID)"

            let connection = try await PostgresConnection.connect(
                on: eventLoopGroup.any(),
                configuration: connectionConfig,
                id: connectionID,
                logger: connectionLogger
            ).get()

            return ConnectionAndMetadata(connection: connection, maximalStreamsOnConnection: 1)
        }
    }

//    public func query<Clock: _Concurrency.Clock>(
//        _ query: PostgresQuery,
//        deadline: Clock.Instant,
//        clock: Clock,
//        logger: Logger,
//        file: String = #file,
//        line: Int = #line
//    ) async throws -> PostgresRowSequence {
//        let connection = try await self.pool.leaseConnection()
//
//        return try await connection.query(query, logger: logger)
//    }

    public func withConnection<Result>(logger: Logger, _ closure: (PostgresConnection) async throws -> Result) async throws -> Result {
        let connection = try await self.pool.leaseConnection()

        defer { self.pool.releaseConnection(connection) }

        return try await closure(connection)
    }

    public func run() async {
        await self.pool.run()
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct PostgresKeepAliveBehavor: ConnectionKeepAliveBehavior {
    var keepAliveFrequency: Duration?
    var query: PostgresQuery
    var logger: Logger

    init(keepAliveFrequency: Duration?, logger: Logger) {
        self.keepAliveFrequency = keepAliveFrequency
        self.query = "SELECT 1;"
        self.logger = logger
    }

    func runKeepAlive(for connection: PostgresConnection) async throws {
        try await connection.query(self.query, logger: self.logger).map { _ in }.get()
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresKeepAliveBehavor {
    init(_ config: PostgresClient.Configuration, logger: Logger) {
        self = .init(keepAliveFrequency: config.pool.keepAliveFrequency, logger: logger)
        self.query = config.pool.keepAliveQuery
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension ConnectionPoolConfiguration {
    init(_ config: PostgresClient.Configuration) {
        self = ConnectionPoolConfiguration()
        self.minimumConnectionCount = config.pool.minimumConnectionCount
        self.maximumConnectionSoftLimit = config.pool.maximumConnectionSoftLimit
        self.maximumConnectionHardLimit = config.pool.maximumConnectionHardLimit
        self.idleTimeout = config.pool.connectionIdleTimeout
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresConnection.Configuration {
    init(_ config: PostgresClient.Configuration) throws {
        try self.init(
            host: config.server.host,
            port: config.server.port,
            username: config.authentication.username,
            password: config.authentication.password,
            database: config.authentication.database,
            tls: .init(config.tls)
        )
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresConnection.Configuration.TLS {
    // TODO: Make async
    init(_ config: PostgresClient.Configuration.TLS) throws {
        switch config.base {
        case .disable:
            self = .disable
        case .prefer(let tlsConfig):
            self = try .prefer(.init(configuration: tlsConfig))
        case .require(let tlsConfig):
            self = try .require(.init(configuration: tlsConfig))
        }
    }
}

@_spi(ConnectionPool)
extension PostgresConnection: PooledConnection {
    public func close() {
        self.channel.close(mode: .all, promise: nil)
    }

    public func onClose(_ closure: @escaping ((any Error)?) -> ()) {
        self.closeFuture.whenComplete { _ in closure(nil) }
    }
}

extension ConnectionPoolError {
    func mapToPSQLError(lastConnectError: Error?) -> Error {
        var psqlError: PSQLError
        switch self {
        case .poolShutdown:
            psqlError = PSQLError.poolClosed
            psqlError.underlying = self

        case .requestCancelled:
            psqlError = PSQLError.queryCancelled
            psqlError.underlying = self

        default:
            return self
        }
        return psqlError
    }
}
