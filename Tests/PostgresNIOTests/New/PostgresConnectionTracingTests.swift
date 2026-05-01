import InMemoryTracing
import Logging
import NIOCore
import NIOEmbedded
import Testing
import Tracing
@testable import PostgresNIO

@Suite struct PostgresConnectionTracingTests {
    let logger = Logger(label: "PostgresConnectionTracingTests")

    @Test func testAsyncQuerySpanEndsAfterSequenceConsumption() async throws {
        let tracer = InMemoryTracer()

        try await self.withTracingConnection(tracer: tracer) { connection, channel in
            let queryTask = Task {
                try await connection.query("SELECT 1;", logger: self.logger)
            }

            let request = try await channel.waitForUnpreparedRequest()
            #expect(request.parse.query == "SELECT 1;")

            try await channel.sendUnpreparedRequestResponseStart(columns: [Self.intColumn(name: "value")])

            let rows: PostgresRowSequence = try await queryTask.value
            #expect(tracer.finishedSpans.isEmpty)

            try await channel.writeInbound(PostgresBackendMessage.dataRow([Int(1)]))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("SELECT 1"))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            // Span is still open: CommandComplete arrived but the sequence hasn't been consumed yet.
            #expect(tracer.finishedSpans.isEmpty)

            // Consume the sequence — span ends synchronously when the iterator is torn down,
            // before control returns to the caller. This follows the OTel semantic that the span
            // covers the full operation "as observed by the caller", including row-fetch time.
            var iterator = rows.decode(Int.self).makeAsyncIterator()
            #expect(try await iterator.next() == 1)
            #expect(try await iterator.next() == nil)

            let span = try #require(tracer.finishedSpans.first)
            #expect(span.operationName == "database")
            #expect(span.kind == .client)
            #expect(span.status == nil)
            #expect(span.attributes.stringValue(for: "db.system.name") == "postgresql")
            #expect(span.attributes.stringValue(for: "db.system") == "postgresql")
            #expect(span.attributes.stringValue(for: "db.query.text") == "SELECT 1;")
        }
    }

    @Test func testAsyncQuerySpanFailsWhenSequenceIsDroppedEarly() async throws {
        let tracer = InMemoryTracer()

        try await self.withTracingConnection(tracer: tracer) { connection, channel in
            let queryTask = Task {
                try await connection.query("SELECT 1;", logger: self.logger)
            }

            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestResponseStart(columns: [Self.intColumn(name: "value")])

            var rows: PostgresRowSequence? = try await queryTask.value
            do {
                let iterator = rows?.makeAsyncIterator()
                withExtendedLifetime(iterator) {
                    rows = nil
                }
            }
            try await Task.sleep(for: .milliseconds(10))

            let span = try #require(tracer.finishedSpans.first)
            #expect(span.operationName == "database")
            #expect(span.status?.code == .error)
            #expect(span.attributes.stringValue(for: "error.type") == "Swift.CancellationError")
            #expect(span.attributes.stringValue(for: "db.query.text") == "SELECT 1;")
        }
    }

    @Test func testAsyncQuerySpanFailsWhenServerSendsErrorDuringIteration() async throws {
        let tracer = InMemoryTracer()

        try await self.withTracingConnection(tracer: tracer) { connection, channel in
            let queryTask = Task {
                let rows = try await connection.query("SELECT 1;", logger: self.logger)
                var iterator = rows.decode(Int.self).makeAsyncIterator()
                _ = try await iterator.next()  // consumes the first row
                _ = try await iterator.next()  // throws when the server error arrives
            }

            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestResponseStart(columns: [Self.intColumn(name: "value")])

            // Deliver one row so the consumer progresses into the async sequence state.
            try await channel.writeInbound(PostgresBackendMessage.dataRow([Int(1)]))
            try await channel.testingEventLoop.executeInContext { channel.read() }

            // Server sends an error instead of the next row or CommandComplete.
            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .message: "disk full",
                .sqlState: "53100",
            ])))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            do {
                try await queryTask.value
                Issue.record("Expected iteration to fail")
            } catch {}

            // The span should end with the server error (SQL state 53100), not CancellationError.
            let span = try #require(tracer.finishedSpans.first)
            #expect(span.operationName == "database")
            #expect(span.status?.code == .error)
            #expect(span.attributes.stringValue(for: "db.response.status_code") == "53100")
            #expect(span.attributes.stringValue(for: "error.type") == "53100")
        }
    }

    @Test func testAsyncQueryRecordsEnrichedErrorOnSpan() async throws {
        let tracer = InMemoryTracer()
        let file = "TracingFile.swift"
        let line = 4242
        let query = PostgresQuery(unsafeSQL: "SELECT broken;", binds: PostgresBindings())

        try await self.withTracingConnection(tracer: tracer) { connection, channel in
            let queryTask = Task {
                try await connection.query(query, logger: self.logger, file: file, line: line)
            }

            let request = try await channel.waitForUnpreparedRequest()
            #expect(request.parse.query == query.sql)

            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .message: "syntax error",
                .sqlState: "42601",
            ])))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            do {
                _ = try await queryTask.value
                Issue.record("Expected query to fail")
            } catch {
                let psqlError = try #require(error as? PSQLError)
                #expect(psqlError.file == file)
                #expect(psqlError.line == line)
                #expect(psqlError.query == query)
            }

            let span = try #require(tracer.finishedSpans.first)
            #expect(span.status?.code == .error)
            #expect(span.errors.count == 1)

            let recordedError = try #require(span.errors.first?.error as? PSQLError)
            #expect(recordedError.file == file)
            #expect(recordedError.line == line)
            #expect(recordedError.query == query)
            #expect(recordedError.serverInfo?[.sqlState] == "42601")
        }
    }

    @Test func testAsyncQueryCanAttachDebugErrorDescriptionToRecordedException() async throws {
        let tracer = InMemoryTracer()
        let file = "TracingFile.swift"
        let line = 4242
        let query = PostgresQuery(unsafeSQL: "SELECT broken;", binds: PostgresBindings())

        var tracing = PostgresTracingConfiguration(
            isEnabled: true,
            queryTextPolicy: .recordAll,
            errorDetailsPolicy: .debugDescription
        )
        tracing.tracer = tracer

        try await self.withTracingConnection(tracing: tracing) { connection, channel in
            let queryTask = Task {
                try await connection.query(query, logger: self.logger, file: file, line: line)
            }

            _ = try await channel.waitForUnpreparedRequest()

            try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                .message: "syntax error",
                .sqlState: "42601",
            ])))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            do {
                _ = try await queryTask.value
                Issue.record("Expected query to fail")
            } catch {}

            let span = try #require(tracer.finishedSpans.first)
            let recordedError = try #require(span.errors.first)
            let exceptionMessage = try #require(recordedError.attributes.stringValue(for: "exception.message"))
            #expect(exceptionMessage.contains("PSQLError(code: server"))
            #expect(exceptionMessage.contains("triggeredFromRequestInFile: TracingFile.swift"))
            #expect(exceptionMessage.contains("line: 4242"))
            #expect(exceptionMessage.contains("query: PostgresQuery(sql: SELECT broken;"))
        }
    }

    @Test func testCallbackQueryRunsWithSpanContext() async throws {
        let tracer = InMemoryTracer()

        try await self.withTracingConnection(tracer: tracer) { connection, channel in
            let queryFuture = connection.query("SELECT 1;", logger: self.logger) { _ in
                let childSpan = tracer.startSpan(
                    "row-handler",
                    context: ServiceContext.current ?? .topLevel,
                    ofKind: .internal
                )
                childSpan.end()
            }

            _ = try await channel.waitForUnpreparedRequest()
            try await channel.sendUnpreparedRequestResponseStart(columns: [Self.intColumn(name: "value")])
            try await channel.writeInbound(PostgresBackendMessage.dataRow([Int(1)]))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("SELECT 1"))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            _ = try await queryFuture.get()

            let querySpan = try #require(tracer.finishedSpans.first(where: {
                $0.attributes.stringValue(for: "db.query.text") == "SELECT 1;"
            }))
            let childSpan = try #require(tracer.finishedSpans.first(where: { $0.operationName == "row-handler" }))

            #expect(childSpan.parentSpanID == querySpan.spanID)
        }
    }

    @Test func testTransactionWrapperSpanParentsDatabaseSpans() async throws {
        let tracer = InMemoryTracer()

        try await self.withTracingConnection(tracer: tracer) { connection, channel in
            let transactionTask = Task {
                try await connection.withTransaction(logger: self.logger) { transaction in
                    let rows = try await transaction.query("SELECT 1;", logger: self.logger)
                    var iterator = rows.decode(Int.self).makeAsyncIterator()
                    _ = try await iterator.next()
                    _ = try await iterator.next()
                }
            }

            let begin = try await channel.waitForUnpreparedRequest()
            #expect(begin.parse.query == "BEGIN;")
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("BEGIN"))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            let select = try await channel.waitForUnpreparedRequest()
            #expect(select.parse.query == "SELECT 1;")
            try await channel.sendUnpreparedRequestResponseStart(columns: [Self.intColumn(name: "value")])
            try await channel.writeInbound(PostgresBackendMessage.dataRow([Int(1)]))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("SELECT 1"))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            let commit = try await channel.waitForUnpreparedRequest()
            #expect(commit.parse.query == "COMMIT;")
            try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
            try await channel.writeInbound(PostgresBackendMessage.commandComplete("COMMIT"))
            try await channel.testingEventLoop.executeInContext { channel.read() }
            try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

            try await transactionTask.value

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
    }

    private func withTracingConnection(
        tracer: InMemoryTracer? = nil,
        tracing: PostgresTracingConfiguration? = nil,
        _ body: (PostgresConnection, NIOAsyncTestingChannel) async throws -> Void
    ) async throws {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = try await NIOAsyncTestingChannel(loop: eventLoop) { channel in
            try channel.pipeline.syncOperations.addHandlers(ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()))
            try channel.pipeline.syncOperations.addHandlers(ReverseMessageToByteHandler(PSQLBackendMessageEncoder()))
        }
        try await channel.connect(to: .makeAddressResolvingHost("localhost", port: 5432))

        var configuration = PostgresConnection.Configuration(
            establishedChannel: channel,
            username: "username",
            password: "postgres",
            database: "database"
        )
        configuration.options.tracing = tracing ?? .init(isEnabled: true, queryTextPolicy: .recordAll)
        if let tracer {
            configuration.options.tracing.tracer = tracer
        }

        async let connectionTask = PostgresConnection.connect(
            on: eventLoop,
            configuration: configuration,
            id: 1,
            logger: self.logger
        )

        let startup = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        #expect(startup == .startup(.versionThree(parameters: .init(
            user: "username",
            database: "database",
            options: [],
            replication: .false
        ))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionTask
        do {
            try await body(connection, channel)
        } catch {
            try await connection.close()
            throw error
        }

        try await connection.close()
    }

    private static func intColumn(name: String) -> RowDescription.Column {
        .init(
            name: name,
            tableOID: 0,
            columnAttributeNumber: 0,
            dataType: .int8,
            dataTypeSize: 8,
            dataTypeModifier: 0,
            format: .binary
        )
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

private extension NIOAsyncTestingChannel {
    func sendUnpreparedRequestResponseStart(columns: [RowDescription.Column]) async throws {
        try await self.writeInbound(PostgresBackendMessage.parseComplete)
        try await self.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
        try await self.writeInbound(PostgresBackendMessage.rowDescription(.init(columns: columns)))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.bindComplete)
        try await self.testingEventLoop.executeInContext { self.read() }
    }
}
