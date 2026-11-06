import InMemoryTracing
import NIOCore
import NIOEmbedded
import Testing
import Tracing
@testable import PostgresNIO

@Suite struct PostgresTracingTests {
    @Test func queryWithoutSummaryIsNamedAfterTheDatabase() async throws {
        try await self.withTracedConnection { connection, channel, tracer in
            async let query: Void = connection.query("DELETE FROM users", logger: .psqlTest) { rows in
                for try await _ in rows {}
            }
            try await self.respond(to: channel, commandTag: "DELETE 0")
            try await query

            try #require(tracer.finishedSpans.count == 1)
            let span = tracer.finishedSpans[0]
            #expect(span.operationName == "database")
            #expect(span.kind == .client)
            #expect(span.status == nil)
            #expect(span.attributes == self.connectionAttributes(["db.query.text": "DELETE FROM users"]))
        }
    }

    @Test func summaryNamesTheSpan() async throws {
        try await self.withTracedConnection { connection, channel, tracer in
            async let query: Void = connection.query("DELETE FROM users", logger: .psqlTest, summary: "DELETE users") { _ in }
            try await self.respond(to: channel, commandTag: "DELETE 0")
            try await query

            try #require(tracer.finishedSpans.count == 1)
            let span = tracer.finishedSpans[0]
            #expect(span.operationName == "DELETE users")
            #expect(span.attributes == self.connectionAttributes([
                "db.query.text": "DELETE FROM users",
                "db.query.summary": "DELETE users",
            ]))
        }
    }

    @Test func queryWithMetadataIsTraced() async throws {
        try await self.withTracedConnection { connection, channel, tracer in
            async let metadata = connection.query("DELETE FROM users", logger: .psqlTest) { rows in
                for try await _ in rows {}
            }.metadata
            try await self.respond(to: channel, commandTag: "DELETE 0")
            _ = try await metadata

            try #require(tracer.finishedSpans.count == 1)
            #expect(tracer.finishedSpans[0].attributes == self.connectionAttributes(["db.query.text": "DELETE FROM users"]))
        }
    }

    @Test func returnedRowsAreRecordedWhenTurnedOn() async throws {
        try await self.withTracedConnection(configure: { $0.recordsReturnedRows = true }) { connection, channel, tracer in
            async let query: Void = connection.query("SELECT name FROM users", logger: .psqlTest) { rows in
                for try await _ in rows {}
            }
            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedQueryStart(columns: [.textColumn(named: "name")])
            try await channel.sendUnpreparedQueryEnd(dataRows: [["alice"], ["bob"], ["carol"]], commandTag: "SELECT 3")
            try await query

            try #require(tracer.finishedSpans.count == 1)
            #expect(tracer.finishedSpans[0].attributes == self.connectionAttributes([
                "db.query.text": "SELECT name FROM users",
                "db.response.returned_rows": 3,
            ]))
        }
    }

    @Test func queryTextIsNotRecordedWhenTurnedOff() async throws {
        try await self.withTracedConnection(configure: { $0.recordsQueryText = false }) { connection, channel, tracer in
            async let query: Void = connection.query("DELETE FROM users", logger: .psqlTest) { rows in
                for try await _ in rows {}
            }
            try await self.respond(to: channel, commandTag: "DELETE 0")
            try await query

            try #require(tracer.finishedSpans.count == 1)
            #expect(tracer.finishedSpans[0].attributes == self.connectionAttributes())
        }
    }

    @Test func serverErrorRecordsOnlyItsClassification() async throws {
        try await self.withTracedConnection { connection, channel, tracer in
            async let failure = #expect(throws: PSQLError.self) {
                try await connection.query("SELECT * FROM secret_table", logger: .psqlTest) { rows in
                    for try await _ in rows {}
                }
            }
            _ = try await channel.waitForUnpreparedRequest()
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .sqlState: "42P01", // undefined_table
                .message: #"relation "secret_table" does not exist"#,
            ])))
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            _ = await failure

            try #require(tracer.finishedSpans.count == 1)
            let span = tracer.finishedSpans[0]
            #expect(span.status?.code == .error)
            #expect(span.errors.isEmpty)
            #expect(span.attributes == self.connectionAttributes([
                "db.query.text": "SELECT * FROM secret_table",
                "error.type": "42P01",
                "db.response.status_code": "42P01",
            ]))
        }
    }

    @Test func copyIsNamedAfterItsTable() async throws {
        try await self.withTracedConnection { connection, channel, tracer in
            async let copy: Void = connection.copyFrom(table: "copy_table", logger: .psqlTest) { writer in
                try await writer.write(ByteBuffer(staticString: "1\n"))
            }
            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.copyInResponse(.init(format: .textual, columnFormats: [])))
            _ = try await channel.waitForCopyData()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("COPY 1"))
            try await channel.waitForPostgresFrontendMessage(\.sync)
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
            try await copy

            try #require(tracer.finishedSpans.count == 1)
            let span = tracer.finishedSpans[0]
            #expect(span.operationName == "COPY copy_table")
            #expect(span.attributes == self.connectionAttributes([
                "db.query.text": #"COPY "copy_table" FROM STDIN WITH (FORMAT text)"#,
                "db.operation.name": "COPY",
                "db.collection.name": "copy_table",
            ]))
        }
    }

    @Test func transactionScopesItsOwnStatements() async throws {
        try await self.withTracedConnection { connection, channel, tracer in
            async let transaction: Void = connection.withTransaction(logger: .psqlTest) { connection in
                try await connection.query("DELETE FROM users", logger: .psqlTest, summary: "DELETE users") { _ in }
            }
            try await self.respond(to: channel, commandTag: "BEGIN")
            try await self.respond(to: channel, commandTag: "DELETE 0")
            try await self.respond(to: channel, commandTag: "COMMIT")
            try await transaction

            let spans = tracer.finishedSpans
            try #require(spans.map(\.operationName) == ["BEGIN", "DELETE users", "COMMIT", "TRANSACTION"])
            let transactionSpan = spans[3]
            #expect(transactionSpan.kind == .internal)
            #expect(transactionSpan.status == nil)
            #expect(transactionSpan.attributes == [
                "db.system.name": "postgresql",
                "db.namespace": "database",
                "server.address": "127.0.0.1",
            ])
            #expect(spans[0].parentSpanID == transactionSpan.spanID)
            #expect(spans[1].parentSpanID == transactionSpan.spanID)
            #expect(spans[2].parentSpanID == transactionSpan.spanID)
        }
    }

    @Test func failedTransactionHasErrorStatus() async throws {
        struct ProcessError: Error {}

        try await self.withTracedConnection { connection, channel, tracer in
            async let failure = #expect(throws: PostgresTransactionError.self) {
                try await connection.withTransaction(logger: .psqlTest) { _ in throw ProcessError() }
            }
            try await self.respond(to: channel, commandTag: "BEGIN")
            try await self.respond(to: channel, commandTag: "ROLLBACK")
            _ = await failure

            let spans = tracer.finishedSpans
            try #require(spans.map(\.operationName) == ["BEGIN", "ROLLBACK", "TRANSACTION"])
            #expect(spans[2].status?.code == .error)
            #expect(spans[2].attributes.get("error.type") == .string("_OTHER"))
            #expect(spans[1].parentSpanID == spans[2].spanID)
        }
    }

    @Test func databaseSystemNameIsConfigurable() async throws {
        try await self.withTracedConnection(configure: { $0.databaseSystemName = "cockroachdb" }) { connection, channel, tracer in
            async let query: Void = connection.query("DELETE FROM users", logger: .psqlTest) { rows in
                for try await _ in rows {}
            }
            try await self.respond(to: channel, commandTag: "DELETE 0")
            try await query

            try #require(tracer.finishedSpans.count == 1)
            #expect(tracer.finishedSpans[0].attributes.get("db.system.name") == .string("cockroachdb"))
        }
    }

    @Test func noOpTracerInScopeDisablesTracing() async throws {
        try await self.withTracedConnection { connection, channel, tracer in
            try await withTracer(NoOpTracer()) {
                async let query: Void = connection.query("DELETE FROM users", logger: .psqlTest, summary: "DELETE users") { _ in }
                try await self.respond(to: channel, commandTag: "DELETE 0")
                try await query
            }
            #expect(tracer.finishedSpans.isEmpty)
        }
    }

    @Test(arguments: ["2350", "235050", "23a05", "2350!", ""])
    func malformedSQLStateIsDropped(sqlState: String) {
        let error = PSQLError.server(.init(fields: [.sqlState: sqlState]))
        #expect(error.tracingErrorType == nil)
        #expect(error.tracingStatusCode == nil)
    }

    @Test func cancelledQueryHasNoStatusCode() {
        #expect(PSQLError.queryCancelled.tracingErrorType == "cancelled")
        #expect(PSQLError.queryCancelled.tracingStatusCode == nil)
    }

    @Test func clientErrorsHaveNoCode() {
        #expect(PSQLError.serverClosedConnection(underlying: nil).tracingErrorType == nil)
        #expect(PSQLError.serverClosedConnection(underlying: nil).tracingStatusCode == nil)
    }

    private func connectionAttributes(_ extra: SpanAttributes = [:]) -> SpanAttributes {
        var attributes: SpanAttributes = [
            "db.system.name": "postgresql",
            "db.namespace": "database",
            "server.address": "127.0.0.1",
            "network.peer.address": "127.0.0.1",
            "network.peer.port": 5432,
        ]
        attributes.merge(extra)
        return attributes
    }

    private func respond(to channel: NIOAsyncTestingChannel, commandTag: String) async throws {
        _ = try await channel.waitForUnpreparedRequest()
        try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
        try await channel.writeInbound(PostgresBackendMessage.commandComplete(commandTag))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
    }

    private func withTracedConnection(
        configure: (inout PostgresTracingConfiguration) -> Void = { _ in },
        _ body: (PostgresConnection, NIOAsyncTestingChannel, InMemoryTracer) async throws -> Void
    ) async throws {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = try await NIOAsyncTestingChannel(loop: eventLoop) { channel in
            try channel.pipeline.syncOperations.addHandlers(ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()))
            try channel.pipeline.syncOperations.addHandlers(ReverseMessageToByteHandler(PSQLBackendMessageEncoder()))
        }
        try await channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 5432))

        let tracer = InMemoryTracer()
        var configuration = PostgresConnection.Configuration(
            establishedChannel: channel, username: "username", password: "postgres", database: "database"
        )
        configure(&configuration.options.tracing)

        async let connectionPromise = PostgresConnection.connect(on: eventLoop, configuration: configuration, id: 1, logger: .psqlTest)
        _ = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        let connection = try await connectionPromise

        do {
            try await withTracer(tracer) { try await body(connection, channel, tracer) }
        } catch {
            try? await connection.close()
            throw error
        }
        try await connection.close()
    }
}
