import Atomics
import Logging
import NIOCore
import NIOEmbedded
import NIOPosix
import Testing

@testable import PostgresNIO

@Suite
struct PSQLRowStreamTests {
    let logger = Logger(label: "PSQLRowStreamTests")
    let eventLoop = EmbeddedEventLoop()

    @Test func emptyStream() throws {
        let stream = PSQLRowStream(
            source: .noRows(.success(.tag("INSERT 0 1"))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        #expect(try stream.all().wait() == [])
        #expect(stream.commandTag == "INSERT 0 1")
    }

    @Test func failedStream() {
        let stream = PSQLRowStream(
            source: .noRows(.failure(PSQLError.serverClosedConnection(underlying: nil))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        #expect(throws: PSQLError.serverClosedConnection(underlying: nil)) {
            try stream.all().wait()
        }
    }

    @Test func getArrayAfterStreamHasFinished() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        #expect(dataSource.hitDemand == 0)
        #expect(dataSource.hitCancel == 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        #expect(dataSource.hitDemand == 0, "Before we have a consumer demand is not signaled")
        stream.receive(completion: .success("SELECT 2"))

        // attach consumer
        let future = stream.all()
        #expect(dataSource.hitDemand == 0)  // TODO: Is this right?

        let rows = try future.wait()
        #expect(rows.count == 2)
    }

    @Test func getArrayBeforeStreamHasFinished() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        #expect(dataSource.hitDemand == 0)
        #expect(dataSource.hitCancel == 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        #expect(dataSource.hitDemand == 0, "Before we have a consumer demand is not signaled")

        // attach consumer
        let future = stream.all()
        #expect(dataSource.hitDemand == 1)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        #expect(dataSource.hitDemand == 2)

        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        #expect(dataSource.hitDemand == 3)

        stream.receive(completion: .success("SELECT 2"))

        let rows = try future.wait()
        #expect(rows.count == 6)
    }

    @Test func onRowAfterStreamHasFinished() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        #expect(dataSource.hitDemand == 0)
        #expect(dataSource.hitCancel == 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        stream.receive(completion: .success("SELECT 2"))

        #expect(dataSource.hitDemand == 0)

        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            let value = try row.decode(String.self, context: .default)
            #expect(value == "\(expected)")
        }
        #expect(counter.load(ordering: .relaxed) == 2)
        #expect(dataSource.hitDemand == 0)

        try future.wait()
        #expect(stream.commandTag == "SELECT 2")
    }

    @Test func onRowThrowsErrorOnInitialBatch() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        #expect(dataSource.hitDemand == 0)
        #expect(dataSource.hitCancel == 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")],
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")],
        ])

        stream.receive(completion: .success("SELECT 2"))

        #expect(dataSource.hitDemand == 0)

        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            #expect(try row.decode(String.self, context: .default) == "\(expected)")
            if expected == 1 {
                throw OnRowError(row: expected)
            }
        }
        #expect(counter.load(ordering: .relaxed) == 2)  // one more than where we excited, because we already incremented
        #expect(dataSource.hitDemand == 0)

        #expect(throws: OnRowError(row: 1)) {
            try future.wait()
        }
    }

    @Test func onRowBeforeStreamHasFinished() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        #expect(dataSource.hitDemand == 0)
        #expect(dataSource.hitCancel == 0)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])

        #expect(dataSource.hitDemand == 0, "Before we have a consumer demand is not signaled")

        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            let value = try row.decode(String.self, context: .default)
            #expect(value == "\(expected)")
        }
        #expect(counter.load(ordering: .relaxed) == 2)
        #expect(dataSource.hitDemand == 1)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        #expect(counter.load(ordering: .relaxed) == 4)
        #expect(dataSource.hitDemand == 2)

        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        #expect(counter.load(ordering: .relaxed) == 6)
        #expect(dataSource.hitDemand == 3)

        stream.receive(completion: .success("SELECT 6"))

        try future.wait()
        #expect(stream.commandTag == "SELECT 6")
    }

    @Test func emptyResponseHasEmptyCommandTag() throws {
        let stream = PSQLRowStream(
            source: .noRows(.success(.emptyResponse)),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        #expect(try stream.all().wait() == [])
        #expect(stream.commandTag == "")
    }

    @Test func onRowOnFailedStream() throws {
        let stream = PSQLRowStream(
            source: .noRows(.failure(PSQLError.serverClosedConnection(underlying: nil))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        let counter = ManagedAtomic(0)
        let future = stream.onRow { _ in
            counter.wrappingIncrement(ordering: .relaxed)
        }

        #expect(counter.load(ordering: .relaxed) == 0, "Expected the closure to never be called")
        #expect(throws: PSQLError.serverClosedConnection(underlying: nil)) {
            try future.wait()
        }
    }

    @Test func onRowThrowsErrorOnLaterBatch() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        // attach consumer, while the stream is still streaming
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            #expect(try row.decode(String.self, context: .default) == "\(expected)")
            if expected == 2 {
                throw OnRowError(row: expected)
            }
        }
        #expect(dataSource.hitDemand == 1)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        #expect(counter.load(ordering: .relaxed) == 2)
        #expect(dataSource.hitDemand == 2)
        #expect(dataSource.hitCancel == 0)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        #expect(counter.load(ordering: .relaxed) == 3, "Expected the stream to stop forwarding rows after the throw")
        #expect(dataSource.hitDemand == 2, "Expected no further demand after the throw")
        #expect(dataSource.hitCancel == 1, "Expected the data source to be cancelled")

        #expect(throws: OnRowError(row: 2)) {
            try future.wait()
        }

        // everything the connection sends after the failure must be ignored
        stream.receive([[ByteBuffer(string: "4")]])
        stream.receive(completion: .success("SELECT 4"))
        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))
        #expect(counter.load(ordering: .relaxed) == 3)
        #expect(dataSource.hitCancel == 1)
    }

    @Test func onRowClosureThrowsAfterTheStreamWasCompletedReentrantly() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        // attach consumer, while the stream is still streaming
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            // reentrantly complete the stream, before throwing. The promise is succeeded at this
            // point, which means the throw afterwards must be swallowed.
            stream.receive(completion: .success("SELECT 1"))
            throw OnRowError(row: expected)
        }

        stream.receive([[ByteBuffer(string: "0")]])
        #expect(counter.load(ordering: .relaxed) == 1)
        #expect(dataSource.hitCancel == 0, "Expected the data source to not be cancelled, since it is done already")

        try future.wait()
        #expect(stream.commandTag == "SELECT 1")
    }

    @Test func onRowFailsIfStreamFails() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        let counter = ManagedAtomic(0)
        let future = stream.onRow { _ in
            counter.wrappingIncrement(ordering: .relaxed)
        }
        #expect(dataSource.hitDemand == 1)

        stream.receive([[ByteBuffer(string: "0")]])
        #expect(counter.load(ordering: .relaxed) == 1)

        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))

        #expect(throws: PSQLError.serverClosedConnection(underlying: nil)) {
            try future.wait()
        }
    }

    @Test func getArrayFailsIfStreamFails() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        let future = stream.all()
        #expect(dataSource.hitDemand == 1)

        stream.receive([[ByteBuffer(string: "0")]])
        #expect(dataSource.hitDemand == 2)

        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))

        #expect(throws: PSQLError.serverClosedConnection(underlying: nil)) {
            try future.wait()
        }
    }

    @Test func completionsAfterTheStreamHasFinishedAreIgnored() throws {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        let future = stream.all()
        stream.receive([[ByteBuffer(string: "0")]])
        stream.receive(completion: .success("SELECT 1"))

        #expect(try future.wait().count == 1)
        #expect(stream.commandTag == "SELECT 1")

        // further completions must not change the outcome
        stream.receive(completion: .success("SELECT 2"))
        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))
        #expect(stream.commandTag == "SELECT 1")
    }

    @Test func consumersCanBeAttachedFromOffTheEventLoop() async throws {
        let eventLoop = NIOSingletons.posixEventLoopGroup.next()
        let logger = self.logger

        let allStream = try await eventLoop.submit {
            PSQLRowStream(
                source: .noRows(.success(.tag("INSERT 0 1"))),
                eventLoop: eventLoop,
                logger: logger
            )
        }.get()

        #expect(eventLoop.inEventLoop == false)
        let rows = try await allStream.all().get()
        #expect(rows == [])

        let onRowStream = try await eventLoop.submit {
            PSQLRowStream(
                source: .noRows(.success(.tag("INSERT 0 1"))),
                eventLoop: eventLoop,
                logger: logger
            )
        }.get()

        let counter = ManagedAtomic(0)
        try await onRowStream.onRow { _ in
            counter.wrappingIncrement(ordering: .relaxed)
        }.get()
        #expect(counter.load(ordering: .relaxed) == 0)
    }

    func makeColumnDescription(name: String, dataType: PostgresDataType, format: PostgresFormat) -> RowDescription.Column {
        RowDescription.Column(
            name: "test",
            tableOID: 123,
            columnAttributeNumber: 1,
            dataType: .text,
            dataTypeSize: -1,
            dataTypeModifier: 0,
            format: .binary
        )
    }
}

private struct OnRowError: Error, Equatable {
    var row: Int
}

class CountingDataSource: PSQLRowsDataSource {

    var hitDemand: Int = 0
    var hitCancel: Int = 0

    init() {}

    func cancel(for stream: PSQLRowStream) {
        self.hitCancel += 1
    }

    func request(for stream: PSQLRowStream) {
        self.hitDemand += 1
    }
}
