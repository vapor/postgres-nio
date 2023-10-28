@_spi(ConnectionPool) import PostgresNIO
import XCTest
import NIOPosix
import Logging
import Atomics

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PostgresClientTests: XCTestCase {

    func testGetConnection() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()

        var maybeClient: PostgresClient?
        XCTAssertNoThrow(maybeClient = try PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger))
        guard let client = maybeClient else { return XCTFail("Expected to have a client here") }

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            for i in 0..<10000 {
                taskGroup.addTask {
                    try await client.withConnection(logger: logger) { connection in
                        _ = try await connection.query("SELECT 1", logger: logger)
                    }
                    print("done: \(i)")
                }
            }
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresClient.Configuration {
    static func makeTestConfiguration() -> PostgresClient.Configuration {
        var clientConfig = PostgresClient.Configuration()
        clientConfig.pool.minimumConnectionCount = 0
        clientConfig.pool.maximumConnectionSoftLimit = 8*4
        clientConfig.pool.maximumConnectionHardLimit = 12*4
        clientConfig.pool.keepAliveFrequency = .seconds(5)
        clientConfig.pool.connectionIdleTimeout = .seconds(15)

        clientConfig.server.host = env("POSTGRES_HOSTNAME") ?? "localhost"
        clientConfig.server.port = env("POSTGRES_PORT").flatMap({ Int($0) }) ?? 5432
        clientConfig.authentication.username = env("POSTGRES_USER") ?? "test_username"
        clientConfig.authentication.database = env("POSTGRES_DB") ?? "test_database"
        clientConfig.authentication.password = env("POSTGRES_PASSWORD") ?? "test_password"

        clientConfig.tls = .disable

        return clientConfig
    }
}
