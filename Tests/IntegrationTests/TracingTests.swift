import InMemoryTracing
import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import Testing
import Tracing

@Suite struct TracingTests {
    let database = env("POSTGRES_DB") ?? "test_database"

    @Test func queryWithoutSummaryIsNamedAfterTheDatabase() async throws {
        try await self.withTracedConnection(configure: { $0.recordsReturnedRows = true }) { connection, tracer in
            let count = try await connection.query("SELECT generate_series(1, 3)", logger: .psqlTest) { rows in
                var count = 0
                for try await _ in rows { count += 1 }
                return count
            }
            #expect(count == 3)

            let spans = tracer.finishedSpans
            try #require(spans.count == 1)
            let span = spans[0]
            #expect(span.operationName == self.database)
            #expect(span.kind == .client)
            #expect(span.status == nil)
            #expect(span.attributes.get("db.system.name") == .string("postgresql"))
            #expect(span.attributes.get("db.namespace") == .string(self.database))
            #expect(span.attributes.get("db.query.text") == .string("SELECT generate_series(1, 3)"))
            #expect(span.attributes.get("db.response.returned_rows") == .int64(3))
            #expect(span.attributes.get("server.address") == .string(env("POSTGRES_HOSTNAME") ?? "localhost"))
        }
    }

    @Test func summaryNamesTheSpan() async throws {
        try await self.withTracedConnection { connection, tracer in
            try await connection.query("SELECT 1", logger: .psqlTest, summary: "SELECT one") { _ in }

            let span = try #require(tracer.finishedSpans.first)
            #expect(span.operationName == "SELECT one")
            #expect(span.attributes.get("db.query.summary") == .string("SELECT one"))
            #expect(span.attributes.get("db.response.returned_rows") == nil)
        }
    }

    @Test func serverErrorRecordsOnlyItsClassification() async throws {
        try await self.withTracedConnection { connection, tracer in
            await #expect(throws: PSQLError.self) {
                try await connection.query("SELECT * FROM table_that_does_not_exist", logger: .psqlTest) { rows in
                    for try await _ in rows {}
                }
            }

            let span = try #require(tracer.finishedSpans.first)
            #expect(span.status?.code == .error)
            #expect(span.attributes.get("error.type") == .string("42P01"))
            #expect(span.attributes.get("db.response.status_code") == .string("42P01"))
            #expect(span.errors.isEmpty)
        }
    }

    @Test func copyIsNamedAfterItsTable() async throws {
        try await self.withTracedConnection { connection, tracer in
            _ = try await connection.query("CREATE TEMPORARY TABLE traced_copy (id INT)", logger: .psqlTest)
            try await connection.copyFrom(table: "traced_copy", logger: .psqlTest) { writer in
                try await writer.write(ByteBuffer(string: "1\n2\n"))
            }

            let span = try #require(tracer.finishedSpans.first)
            #expect(span.operationName == "COPY traced_copy")
            #expect(span.attributes.get("db.operation.name") == .string("COPY"))
            #expect(span.attributes.get("db.collection.name") == .string("traced_copy"))
            #expect(span.attributes.get("db.query.text") == .string(#"COPY "traced_copy" FROM STDIN WITH (FORMAT text)"#))
        }
    }

    @Test func transactionScopesItsOwnStatements() async throws {
        try await self.withTracedConnection { connection, tracer in
            try await connection.withTransaction(logger: .psqlTest) { connection in
                try await connection.query("SELECT 1", logger: .psqlTest, summary: "SELECT one") { _ in }
            }

            let spans = tracer.finishedSpans
            #expect(spans.map(\.operationName) == ["BEGIN", "SELECT one", "COMMIT", "TRANSACTION"])
            try #require(spans.count == 4)
            let transaction = spans[3]
            #expect(transaction.kind == .internal)
            #expect(transaction.attributes.get("db.namespace") == .string(self.database))
            #expect(transaction.attributes.get("network.peer.address") == nil)
            #expect(transaction.attributes.get("db.query.text") == nil)
            #expect(spans[0].parentSpanID == transaction.spanID)
            #expect(spans[2].parentSpanID == transaction.spanID)
            // `withValue` doesn't return `sending`, so the queries of `process` can't nest under the transaction.
            #expect(spans[1].parentSpanID == nil)
        }
    }

    @Test func attributeNamesAreConfigurable() async throws {
        try await self.withTracedConnection(configure: { $0.attributeNames.databaseSystemName = "db.system" }) { connection, tracer in
            try await connection.query("SELECT 1", logger: .psqlTest) { rows in for try await _ in rows {} }

            let span = try #require(tracer.finishedSpans.first)
            #expect(span.attributes.get("db.system") == .string("postgresql"))
            #expect(span.attributes.get("db.system.name") == nil)
        }
    }

    @Test func clientPassesTheSummaryThrough() async throws {
        let tracer = InMemoryTracer()
        var configuration = PostgresClient.Configuration.makeTestConfiguration()
        configuration.options.tracing = PostgresTracingConfiguration(tracer: tracer)
        let client = PostgresClient(
            configuration: configuration,
            eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
            backgroundLogger: .psqlTest
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query("SELECT 1", summary: "SELECT one") { _ in }
            taskGroup.cancelAll()
        }

        #expect(tracer.finishedSpans.map(\.operationName) == ["SELECT one"])
    }

    private func withTracedConnection(
        configure: (inout PostgresTracingConfiguration) -> Void = { _ in },
        _ body: (PostgresConnection, InMemoryTracer) async throws -> Void
    ) async throws {
        let tracer = InMemoryTracer()
        var options = PostgresConnection.Configuration.Options()
        options.tracing = PostgresTracingConfiguration(tracer: tracer)
        configure(&options.tracing)
        let connection = try await PostgresConnection.test(options: options)
        do {
            try await body(connection, tracer)
        } catch {
            try? await connection.close()
            throw error
        }
        try await connection.close()
    }
}
