import Logging
import NIOCore
import PostgresNIO
import ServiceLifecycleTestKit
import Testing

@Suite struct PostgresClientTests {

    @Test(.timeLimit(.minutes(1)))
    func clientRunExitsPromptlyOnCancellationWhileConnecting() async throws {
        try await withSilentServer { port in
            var config = PostgresClient.Configuration(
                host: "127.0.0.1",
                port: port,
                username: "postgres",
                password: "irrelevant",
                database: "test",
                tls: .disable
            )
            // Long connect timeout so the hang is obvious if the fix regresses.
            config.options.connectTimeout = .seconds(30)
            config.options.minimumConnections = 1

            let client = PostgresClient(
                configuration: config,
                eventLoopGroup: NIOSingletons.posixEventLoopGroup,
                backgroundLogger: Logger(label: "test")
            )

            await withTaskGroup(of: Void.self) { group in
                group.addTask { await client.run() }

                // Give the pool enough time to start the connection attempt (enter .starting state).
                try? await Task.sleep(for: .milliseconds(200))

                // Cancelling the group must make pool.run() return quickly — not in 30 seconds.
                group.cancelAll()
            }
            // If we reach here the test passed (the .timeLimit above enforces the deadline).
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func gracefulShutdownClosesPoolAfterQueuedQueryIsCancelled() async throws {
        try await withSilentServer { port in
            var config = PostgresClient.Configuration(
                host: "127.0.0.1",
                port: port,
                username: "postgres",
                password: "irrelevant",
                database: "test",
                tls: .disable
            )
            config.options.connectTimeout = .seconds(2)
            config.options.minimumConnections = 0

            let client = PostgresClient(
                configuration: config,
                eventLoopGroup: NIOSingletons.posixEventLoopGroup,
                backgroundLogger: Logger(label: "test")
            )

            let elapsed = await ServiceLifecycleTestKit.testGracefulShutdown { gracefulShutdownTrigger in
                let start = ContinuousClock.now
                await withTaskGroup(of: Void.self) { group in
                    group.addTask { await client.run() }

                    let queryTask = Task {
                        do {
                            _ = try await client.query("SELECT 1")
                            Issue.record("The query must never succeed against a silent server")
                        } catch {
                            // expected: the query is cancelled below while still queued
                        }
                    }

                    try? await Task.sleep(for: .milliseconds(200))
                    gracefulShutdownTrigger.triggerGracefulShutdown()

                    try? await Task.sleep(for: .milliseconds(300))
                    queryTask.cancel()
                    await queryTask.value
                }
                return ContinuousClock.now - start
            }

            #expect(elapsed > .seconds(2))
        }
    }
}
