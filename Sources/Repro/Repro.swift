import PostgresNIO
import NIOCore
import Logging
import Backtrace

@main
@available(macOS 13.0, *)
enum Repro {
    static func main() async throws {
        Backtrace.install()

        var mlogger = Logger(label: "psql")
        mlogger.logLevel = .debug
        let logger = mlogger
        
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? eventLoopGroup.syncShutdownGracefully() }

        do {
            let connection = try await PostgresConnection.connect(
                on: eventLoopGroup.next(),
                configuration: .init(
                    host: "host.docker.internal",
                    username: "test_username",
                    password: "test_password",
                    database: "test_database",
                    tls: .disable
                ),
                id: 1,
                logger: logger
            )

            for i in 0..<1 {
                let rows = try await connection.query("SELECT \(i)", logger: logger)
                for try await row in rows.decode(Int.self) {
                    logger.info("Row received: \(row)")
                }
//                try await Task.sleep(for: .microseconds(50))
            }

            try await connection.close()
        } catch {
            logger.error("Error caught", metadata: ["error": "\(String(reflecting: error))"])
            exit(1)
        }

        logger.info("Bye")

//        try await ContinuousClock().sleep(until: .now + .seconds(120))

//        try await client.shutdown(graceful: false)
    }
}
