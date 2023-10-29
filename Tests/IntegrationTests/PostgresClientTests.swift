@_spi(ConnectionPool) import PostgresNIO
import XCTest
import NIOPosix
import NIOSSL
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
        let client = PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            for i in 0..<10000 {
                taskGroup.addTask {
                    try await client.withConnection() { connection in
                        _ = try await connection.query("SELECT 1", logger: logger)
                    }
                    print("done: \(i)")
                }
            }

            for _ in 0..<10000 {
                _ = await taskGroup.nextResult()!
            }

            taskGroup.cancelAll()
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresClient.Configuration {
    static func makeTestConfiguration() -> PostgresClient.Configuration {
        var tlsConfiguration = TLSConfiguration.makeClientConfiguration()
        tlsConfiguration.certificateVerification = .none
        var clientConfig = PostgresClient.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: env("POSTGRES_PORT").flatMap({ Int($0) }) ?? 5432,
            username: env("POSTGRES_USER") ?? "test_username",
            password: env("POSTGRES_PASSWORD") ?? "test_password",
            database: env("POSTGRES_DB") ?? "test_database",
            tls: .prefer(tlsConfiguration)
        )
        clientConfig.options.minimumConnections = 0
        clientConfig.options.maximumConnections = 12*4
        clientConfig.options.keepAliveBehavior = .init(frequency: .seconds(5))
        clientConfig.options.connectionIdleTimeout = .seconds(15)

        return clientConfig
    }
}
