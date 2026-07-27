import Logging
import NIOCore
import PostgresNIO
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
}
