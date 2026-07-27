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
    func gracefulShutdownWaitsForInFlightRequest() async throws {
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
            config.options.minimumConnections = 1

            let client = PostgresClient(
                configuration: config,
                eventLoopGroup: NIOSingletons.posixEventLoopGroup,
                backgroundLogger: Logger(label: "test")
            )

            let elapsed = await ServiceLifecycleTestKit.testGracefulShutdown { gracefulShutdownTrigger in
                let start = ContinuousClock.now
                await withTaskGroup(of: Void.self) { group in
                    // We don't have a server to connect to, so  we'll reach `connectTimeout` (2s).
                    group.addTask { await client.run() }

                    try? await Task.sleep(for: .milliseconds(200))

                    gracefulShutdownTrigger.triggerGracefulShutdown()
                }
                return ContinuousClock.now - start
            }

            // If this took ~200ms (our sleep time) it would mean the connect timeout wasn't reached,
            // and the client was canceled instead of gracefully shut down.
            // If the test reached the time limit (1m, and would fail), it would mean there's no graceful shutdown hook.
            #expect(elapsed > .seconds(1))
        }
    }
}
