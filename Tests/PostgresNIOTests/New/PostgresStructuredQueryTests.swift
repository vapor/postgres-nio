import NIOCore
import NIOEmbedded
import NIOConcurrencyHelpers
import Testing
import Logging
@testable import PostgresNIO

@Suite struct PostgresStructuredQueryTests {

    let logger = Logger(label: "PostgresStructuredQueryTests")

    @Test func bodyReceivesAllRowsAndReturnsResult() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    let names = try await connection.query("SELECT name FROM users", logger: .psqlTest) { rows in
                        var names = [String]()
                        for try await row in rows {
                            names.append(try row.decode(String.self, context: .default))
                        }
                        return names
                    }
                    #expect(names == ["alice", "bob", "carol"])
                }

                let request = try await channel.waitForUnpreparedRequest()
                #expect(request.parse.query == "SELECT name FROM users")

                try await channel.sendUnpreparedQueryStart(columns: [.textColumn(named: "name")])
                try await channel.sendUnpreparedQueryEnd(dataRows: [["alice"], ["bob"], ["carol"]], commandTag: "SELECT 3")

                try await taskGroup.waitForAll()
            }
        }
    }

    @Test func bodyReturnsVoid() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    try await connection.query("SELECT 1", logger: .psqlTest) { rows in
                        var count = 0
                        for try await _ in rows { count += 1 }
                        #expect(count == 1)
                    }
                }

                _ = try await channel.waitForUnpreparedRequest()
                try await channel.sendUnpreparedQueryStart(columns: [.int8Column()])
                try await channel.sendUnpreparedQueryEnd(dataRows: [[Int64(1)]], commandTag: "SELECT 1")

                try await taskGroup.waitForAll()
            }
        }
    }

    @Test func queryWithoutRows() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    let count = try await connection.query("DELETE FROM users", logger: .psqlTest) { rows in
                        var count = 0
                        for try await _ in rows { count += 1 }
                        return count
                    }
                    #expect(count == 0)
                }

                let request = try await channel.waitForUnpreparedRequest()
                #expect(request.parse.query == "DELETE FROM users")

                try await channel.sendUnpreparedRequestWithNoParametersBindResponse()
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("DELETE 0"))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
                try await channel.testingEventLoop.executeInContext { channel.read() }

                try await taskGroup.waitForAll()
            }
        }
    }

    @Test func sequenceEscapingTheScopeCannotBeConsumed() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                let scopeExited = Signal()

                taskGroup.addTask {
                    let escaped = try await connection.query("SELECT name FROM users", logger: .psqlTest) { (rows: PostgresRowSequence) in
                        rows
                    }
                    scopeExited.signal()

                    var rowsSeen = 0
                    await #expect(throws: PSQLError.rowSequenceUsedOutsideScope) {
                        for try await _ in escaped { rowsSeen += 1 }
                    }
                    #expect(rowsSeen == 0)
                }

                _ = try await channel.waitForUnpreparedRequest()
                try await channel.sendUnpreparedQueryStart(columns: [.textColumn(named: "name")])
                // This waits for the scope signal, so after this we're out of scope and can return remaining rows.
                await scopeExited.wait()
                try await channel.sendUnpreparedQueryEnd(dataRows: [["alice"], ["bob"]], commandTag: "SELECT 2")

                try await taskGroup.waitForAll()
            }

            try await self.runSimpleSelect(on: connection, channel: channel)
        }
    }

    @Test func sequenceEscapingTheScopeAfterPartialConsumptionCannotBeConsumedFurther() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                let scopeExited = Signal()

                taskGroup.addTask {
                    let (first, escapedIterator) = try await connection.query("SELECT name FROM users", logger: .psqlTest) { rows in
                        var iterator = rows.makeAsyncIterator()
                        let first = try await iterator.next()
                        return (try first?.decode(String.self, context: .default), iterator)
                    }
                    scopeExited.signal()
                    #expect(first == "alice")

                    var iterator = escapedIterator
                    await #expect(throws: PSQLError.rowSequenceUsedOutsideScope) {
                        _ = try await iterator.next()
                    }
                }

                _ = try await channel.waitForUnpreparedRequest()
                try await channel.sendUnpreparedQueryStart(columns: [.textColumn(named: "name")])
                try await channel.writeInbound(PostgresBackendMessage.dataRow(["alice"]))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                // This waits for the scope signal, so after this we're out of scope and can return remaining rows.
                await scopeExited.wait()
                try await channel.sendUnpreparedQueryEnd(dataRows: [["bob"], ["carol"]], commandTag: "SELECT 3")

                try await taskGroup.waitForAll()
            }

            try await self.runSimpleSelect(on: connection, channel: channel)
        }
    }

    @Test func sequenceEscapingTheScopeYieldsAlreadyBufferedRowsBeforeFailing() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                let scopeExited = Signal()

                taskGroup.addTask {
                    let (first, escapedIterator) = try await connection.query("SELECT name FROM users", logger: .psqlTest) { rows in
                        var iterator = rows.makeAsyncIterator()
                        let first = try await iterator.next()
                        return (try first?.decode(String.self, context: .default), iterator)
                    }
                    scopeExited.signal()
                    #expect(first == "alice")

                    // `NIOThrowingAsyncSequenceProducer.Source.finish(_:)` delivers already buffered elements
                    // before surfacing the failure, so rows that arrived inside the scope are still yielded.
                    var iterator = escapedIterator
                    var remaining = [String]()
                    await #expect(throws: PSQLError.rowSequenceUsedOutsideScope) {
                        while let row = try await iterator.next() {
                            remaining.append(try row.decode(String.self, context: .default))
                        }
                    }
                    #expect(remaining == ["bob", "carol"])
                }

                _ = try await channel.waitForUnpreparedRequest()
                try await channel.sendUnpreparedQueryStart(columns: [.textColumn(named: "name")])
                // Deliver all rows in one batch while the body is running, so `bob` and `carol` sit in the
                // sequence's buffer when the scope exits.
                try await channel.testingEventLoop.executeInContext {
                    for name in ["alice", "bob", "carol"] {
                        channel.pipeline.fireChannelRead(PostgresBackendMessage.dataRow([name]))
                    }
                    channel.pipeline.fireChannelReadComplete()
                }
                try await channel.testingEventLoop.executeInContext { channel.read() }
                await scopeExited.wait()
                try await channel.writeInbound(PostgresBackendMessage.commandComplete("SELECT 3"))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
                try await channel.testingEventLoop.executeInContext { channel.read() }

                try await taskGroup.waitForAll()
            }

            try await self.runSimpleSelect(on: connection, channel: channel)
        }
    }

    @Test func bodyThrowsPropagatesErrorAndConnectionStaysUsable() async throws {
        struct MyError: Error, Equatable {}

        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    await #expect(throws: MyError.self) {
                        try await connection.query("SELECT name FROM users", logger: .psqlTest) { rows in
                            for try await _ in rows {
                                throw MyError()
                            }
                        }
                    }
                }

                _ = try await channel.waitForUnpreparedRequest()
                try await channel.sendUnpreparedQueryStart(columns: [.textColumn(named: "name")])
                try await channel.sendUnpreparedQueryEnd(dataRows: [["alice"], ["bob"]], commandTag: "SELECT 2")

                try await taskGroup.waitForAll()
            }

            try await self.runSimpleSelect(on: connection, channel: channel)
        }
    }

    @Test func serverErrorWhileStreamingIsThrownInsideBody() async throws {
        try await self.withAsyncTestingChannel { connection, channel in
            try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                taskGroup.addTask {
                    do {
                        try await connection.query("SELECT name FROM users", logger: .psqlTest) { rows in
                            var seen = [String]()
                            do {
                                for try await row in rows {
                                    seen.append(try row.decode(String.self, context: .default))
                                }
                                Issue.record("Expected iteration to throw")
                            } catch let error as PSQLError {
                                #expect(error.code == .server)
                                #expect(error.serverInfo?[.sqlState] == "57014")
                            }
                            #expect(seen == ["alice"])
                        }
                    }
                }

                _ = try await channel.waitForUnpreparedRequest()
                try await channel.sendUnpreparedQueryStart(columns: [.textColumn(named: "name")])
                try await channel.writeInbound(PostgresBackendMessage.dataRow(["alice"]))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.error(.init(fields: [
                    .sqlState: "57014" // query_canceled
                ])))
                try await channel.testingEventLoop.executeInContext { channel.read() }
                try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
                try await channel.testingEventLoop.executeInContext { channel.read() }

                try await taskGroup.waitForAll()
            }

            try await self.runSimpleSelect(on: connection, channel: channel)
        }
    }

    // MARK: - Helpers

    /// Runs a `SELECT 1` through the structured API and asserts it round-trips. Used to prove that
    /// the connection is still healthy after a previous query was invalidated, cancelled or failed.
    private func runSimpleSelect(on connection: PostgresConnection, channel: NIOAsyncTestingChannel) async throws {
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                let value = try await connection.query("SELECT 1", logger: .psqlTest) { rows in
                    var value: Int64?
                    for try await row in rows {
                        value = try row.decode(Int64.self, context: .default)
                    }
                    return value
                }
                #expect(value == 1)
            }

            let request = try await channel.waitForUnpreparedRequest()
            #expect(request.parse.query == "SELECT 1")
            try await channel.sendUnpreparedQueryStart(columns: [.int8Column()])
            try await channel.sendUnpreparedQueryEnd(dataRows: [[Int64(1)]], commandTag: "SELECT 1")

            try await taskGroup.waitForAll()
        }
    }

    private func withAsyncTestingChannel(_ body: (PostgresConnection, NIOAsyncTestingChannel) async throws -> ()) async throws {
        let eventLoop = NIOAsyncTestingEventLoop()
        let channel = try await NIOAsyncTestingChannel(loop: eventLoop) { channel in
            try channel.pipeline.syncOperations.addHandlers(ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()))
            try channel.pipeline.syncOperations.addHandlers(ReverseMessageToByteHandler(PSQLBackendMessageEncoder()))
        }
        try await channel.connect(to: .makeAddressResolvingHost("localhost", port: 5432))

        let configuration = PostgresConnection.Configuration(
            establishedChannel: channel,
            username: "username",
            password: "postgres",
            database: "database"
        )

        let logger = self.logger
        async let connectionPromise = PostgresConnection.connect(on: eventLoop, configuration: configuration, id: 1, logger: logger)
        let message = try await channel.waitForOutboundWrite(as: PostgresFrontendMessage.self)
        #expect(message == .startup(.versionThree(parameters: .init(user: "username", database: "database", options: [], replication: .false))))
        try await channel.writeInbound(PostgresBackendMessage.authentication(.ok))
        try await channel.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try await channel.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        let connection = try await connectionPromise

        do {
            try await body(connection, channel)
        } catch {
            try? await connection.close()
            throw error
        }

        try await connection.close()
    }
}

private final class Signal: Sendable {
    private let stream: AsyncStream<Void>
    private let continuation: AsyncStream<Void>.Continuation

    init() {
        (self.stream, self.continuation) = AsyncStream.makeStream(of: Void.self)
    }

    func signal() {
        self.continuation.yield()
        self.continuation.finish()
    }

    func wait() async {
        for await _ in self.stream { return }
    }
}

extension RowDescription.Column {
    fileprivate static func textColumn(named name: String) -> RowDescription.Column {
        .init(
            name: name,
            tableOID: 0,
            columnAttributeNumber: 0,
            dataType: .text,
            dataTypeSize: -1,
            dataTypeModifier: -1,
            format: .binary
        )
    }

    fileprivate static func int8Column(named name: String = "") -> RowDescription.Column {
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

extension NIOAsyncTestingChannel {
    /// Sends everything up to and including `BindComplete` for an unnamed query that returns rows,
    /// which is the point where the `PSQLRowStream` is handed to the caller and the body starts executing.
    fileprivate func sendUnpreparedQueryStart(columns: [RowDescription.Column]) async throws {
        try await self.writeInbound(PostgresBackendMessage.parseComplete)
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.parameterDescription(.init(dataTypes: [])))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.rowDescription(.init(columns: columns)))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.bindComplete)
        try await self.testingEventLoop.executeInContext { self.read() }
    }

    /// Sends the given rows followed by `CommandComplete` and `ReadyForQuery`.
    fileprivate func sendUnpreparedQueryEnd(dataRows: [DataRow], commandTag: String) async throws {
        try await self.testingEventLoop.executeInContext {
            for dataRow in dataRows {
                self.pipeline.fireChannelRead(PostgresBackendMessage.dataRow(dataRow))
            }
            self.pipeline.fireChannelReadComplete()
        }
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.commandComplete(commandTag))
        try await self.testingEventLoop.executeInContext { self.read() }
        try await self.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
        try await self.testingEventLoop.executeInContext { self.read() }
    }
}
