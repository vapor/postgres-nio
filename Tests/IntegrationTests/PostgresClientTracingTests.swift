@_spi(ConnectionPool) import PostgresNIO
import InMemoryTracing
import Logging
import NIOPosix
import Testing
import Tracing

@Suite(.serialized) struct PostgresClientTracingTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testQueryTracingIncludesLeaseWait() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.minimumConnections = 0
            config.options.maximumConnections = 1
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                let firstTask = Task {
                    let rows = try await client.query("SELECT pg_sleep(0.3);", logger: logger)
                    for try await _ in rows {}
                }

                try await Task.sleep(for: .milliseconds(50))

                let secondTask = Task {
                    let rows = try await client.query("SELECT 1;", logger: logger)
                    for try await _ in rows {}
                }

                try await firstTask.value
                try await secondTask.value

                group.cancelAll()
            }
        }

        let sleepSpan = try #require(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "SELECT pg_sleep(0.3);"
        }))
        let secondSpan = try #require(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "SELECT 1;"
        }))

        #expect(sleepSpan.kind == SpanKind.client)
        #expect(secondSpan.kind == SpanKind.client)

        let duration = secondSpan.endInstant.nanosecondsSinceEpoch - secondSpan.startInstant.nanosecondsSinceEpoch
        #expect(duration >= 200_000_000)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTransactionTracingParentsDatabaseSpans() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                try await client.withTransaction(logger: logger) { transaction in
                    let rows = try await transaction.query("SELECT 1;", logger: logger)
                    for try await _ in rows {}
                }

                group.cancelAll()
            }
        }

        let transactionSpan = try #require(tracer.finishedSpans.first(where: { $0.operationName == "postgres.transaction" }))
        let beginSpan = try #require(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "BEGIN;"
        }))
        let selectSpan = try #require(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "SELECT 1;"
        }))
        let commitSpan = try #require(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "COMMIT;"
        }))

        #expect(transactionSpan.kind == .internal)
        #expect(beginSpan.parentSpanID == transactionSpan.spanID)
        #expect(selectSpan.parentSpanID == transactionSpan.spanID)
        #expect(commitSpan.parentSpanID == transactionSpan.spanID)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTransactionTracingFailsWhenLeaseFails() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            await withTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                group.cancelAll()
                try? await Task.sleep(for: .milliseconds(10))

                do {
                    try await client.withTransaction(logger: logger) { _ in
                        ()
                    }
                    Issue.record("Expected `withTransaction` to throw after client shutdown")
                } catch {
                    // expected
                }
            }
        }

        let transactionSpan = try #require(tracer.finishedSpans.first(where: { $0.operationName == "postgres.transaction" }))
        #expect(transactionSpan.kind == .internal)
        #expect(transactionSpan.status?.code == .error)
        #expect(transactionSpan.attributes["error.type"] != nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveQueriesAreNotTraced() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger
        let keepAliveValue = "postgresnio-keepalive-tracing"
        let keepAliveQuery = PostgresQuery(
            unsafeSQL: "SELECT set_config('application_name', 'postgresnio-keepalive-tracing', false);"
        )
        let resetQuery = PostgresQuery(
            unsafeSQL: "SELECT set_config('application_name', 'postgresnio-user-query', false);"
        )
        let verifyQuery = PostgresQuery(
            unsafeSQL: "SELECT current_setting('application_name');"
        )

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.minimumConnections = 1
            config.options.maximumConnections = 1
            config.options.keepAliveBehavior = .init(
                frequency: .milliseconds(100),
                query: keepAliveQuery
            )
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                let resetRows = try await client.query(resetQuery, logger: logger).decode(String.self)
                for try await _ in resetRows {}

                #expect(try await self.waitForFinishedSpan(withQueryText: resetQuery.sql, in: tracer))
                tracer.clearFinishedSpans()

                try await Task.sleep(for: .milliseconds(250))

                let rows = try await client.query(verifyQuery, logger: logger)

                var values = [String]()
                for try await value in rows.decode(String.self) {
                    values.append(value)
                }

                #expect(try await self.waitForFinishedSpan(withQueryText: verifyQuery.sql, in: tracer))

                #expect(values == [keepAliveValue])
                #expect(tracer.finishedSpans.count == 1)
                #expect(tracer.finishedSpans.first?.attributes.stringValue(for: "db.query.text") == verifyQuery.sql)
                #expect(tracer.finishedSpans.contains(where: {
                    $0.attributes.stringValue(for: "db.query.text") == keepAliveQuery.sql
                }) == false)

                group.cancelAll()
            }
        }
    }

    private func verifyDatabaseAccess(on eventLoopGroup: MultiThreadedEventLoopGroup) async throws {
        let connection = try await PostgresConnection.test(on: eventLoopGroup.next()).get()
        try await connection.close()
    }

    private func withEventLoopGroup<T>(
        _ body: (MultiThreadedEventLoopGroup) async throws -> T
    ) async throws -> T {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        do {
            let result = try await body(eventLoopGroup)
            try await eventLoopGroup.shutdownGracefully()
            return result
        } catch {
            try? await eventLoopGroup.shutdownGracefully()
            throw error
        }
    }

    private func waitForFinishedSpan(
        withQueryText queryText: String,
        in tracer: InMemoryTracer
    ) async throws -> Bool {
        for _ in 0..<50 {
            if tracer.finishedSpans.contains(where: {
                $0.attributes.stringValue(for: "db.query.text") == queryText
            }) {
                return true
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        return false
    }
}

private extension SpanAttributes {
    func stringValue(for key: String) -> String? {
        switch self[key]?.toSpanAttribute() {
        case .string(let value):
            return value
        case .stringConvertible(let value):
            return String(describing: value)
        default:
            return nil
        }
    }
}
