//
//  PostgresPerf.swift
//  benchmarks
//
//  Created by Fabian Fett on 12.05.25.
//

import Synchronization
import PostgresNIO
@preconcurrency import PostgresKit
@preconcurrency import AsyncKit

@main
@available(macOS 15.0, *)
enum PostgresPerf {

    static let maxConnections: Int = 50
    static let tasks: Int = 400
    static let iterationsPerTask: Int = 1000
    static let logger = Logger(label: "TestLogger")
    static let clock = ContinuousClock()

    static let eventLoopCount = {
        NIOSingletons.posixEventLoopGroup.makeIterator().reduce(0, { (res, _) in res + 1 })
    }()

    static func main() async throws {
//        if CommandLine.arguments.first == "kit" {
            try await Self.runPostgresKit()
//        } else {
            try await self.runPostgresNIO()
//        }
    }

    static func runPostgresKit() async throws {
        let configuration = SQLPostgresConfiguration(
            hostname: "localhost", port: 5432,
            username: "test_username",
            password: "test_password",
            database: "test_database",
            tls: .disable
        )

        let pools = EventLoopGroupConnectionPool(
            source: PostgresConnectionSource(sqlConfiguration: configuration),
            maxConnectionsPerEventLoop: Self.maxConnections / Self.eventLoopCount,
            on: NIOSingletons.posixEventLoopGroup
        )

        let start = self.clock.now
        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            for _ in 0..<Self.tasks {
                taskGroup.addTask {
                    for _ in 0..<Self.iterationsPerTask {
                        _ = try await pools.withConnection { connection in
                            connection.query("SELECT 1;", logger: Self.logger) { row in
                                let foo = try row.decode(Int.self)
                            }
                        }.get()
                    }
                }
            }

            for _ in 0..<Self.tasks {
                _ = await taskGroup.nextResult()!
            }

            taskGroup.cancelAll()
        }

        try await pools.shutdownAsync()
        let end = self.clock.now

        self.logger.info("PostgresKit completed", metadata: ["Took": "\(end - start)"])
    }

    static func runPostgresNIO() async throws {
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
        clientConfig.options.maximumConnections = Self.maxConnections
        clientConfig.options.keepAliveBehavior = .init(frequency: .seconds(5))
        clientConfig.options.connectionIdleTimeout = .seconds(15)


        let client = PostgresClient(
            configuration: clientConfig,
            eventLoopGroup: NIOSingletons.posixEventLoopGroup
        )

        let start = self.clock.now
        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

//            let atomic = Atomic(0)
            for _ in 0..<Self.tasks {
                taskGroup.addTask {
                    for _ in 0..<Self.iterationsPerTask {
                        let rows = try await client.query("SELECT 1;")
                        for try await row in rows.decode(Int.self) {
//                            atomic.add(row, ordering: .relaxed)
                        }
                    }
                }
            }

            for _ in 0..<Self.tasks {
                _ = await taskGroup.nextResult()!
            }

            taskGroup.cancelAll()
        }
        let end = self.clock.now

        self.logger.info("PostgresNIO completed", metadata: ["Took": "\(end - start)"])
    }
}

func env(_ name: String) -> String? {
    getenv(name).flatMap { String(cString: $0) }
}
