import Atomics
import NIOCore
import Logging
import XCTest
@testable import PostgresNIO
import NIOCore
import NIOEmbedded
import NIOPosix

final class PSQLRowStreamTests: XCTestCase {
    let logger = Logger(label: "PSQLRowStreamTests")
    let eventLoop = EmbeddedEventLoop()

    func testEmptyStream() {
        let stream = PSQLRowStream(
            source: .noRows(.success(.tag("INSERT 0 1"))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        
        XCTAssertEqual(try stream.all().wait(), [])
        XCTAssertEqual(stream.commandTag, "INSERT 0 1")
    }
    
    func testFailedStream() {
        let stream = PSQLRowStream(
            source: .noRows(.failure(PSQLError.serverClosedConnection(underlying: nil))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        
        XCTAssertThrowsError(try stream.all().wait()) {
            XCTAssertEqual($0 as? PSQLError, .serverClosedConnection(underlying: nil))
        }
    }
    
    func testGetArrayAfterStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")
        stream.receive(completion: .success("SELECT 2"))
        
        // attach consumer
        let future = stream.all()
        XCTAssertEqual(dataSource.hitDemand, 0) // TODO: Is this right?
        
        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try future.wait())
        XCTAssertEqual(rows?.count, 2)
    }

    func testGetArrayBeforeStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")
        
        // attach consumer
        let future = stream.all()
        XCTAssertEqual(dataSource.hitDemand, 1)
        
        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 2)
        
        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(dataSource.hitDemand, 3)
        
        stream.receive(completion: .success("SELECT 2"))
        
        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try future.wait())
        XCTAssertEqual(rows?.count, 6)
    }
    
    func testOnRowAfterStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        stream.receive(completion: .success("SELECT 2"))
        
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            XCTAssertEqual(try row.decode(String.self, context: .default), "\(expected)")
        }
        XCTAssertEqual(counter.load(ordering: .relaxed), 2)
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        XCTAssertNoThrow(try future.wait())
        XCTAssertEqual(stream.commandTag, "SELECT 2")
    }

    func testOnRowThrowsErrorOnInitialBatch() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")],
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")],
        ])
        
        stream.receive(completion: .success("SELECT 2"))
        
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            XCTAssertEqual(try row.decode(String.self, context: .default), "\(expected)")
            if expected == 1 {
                throw OnRowError(row: expected)
            }
        }
        XCTAssertEqual(counter.load(ordering: .relaxed), 2) // one more than where we excited, because we already incremented
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        XCTAssertThrowsError(try future.wait()) {
            XCTAssertEqual($0 as? OnRowError, OnRowError(row: 1))
        }
    }

    func testOnRowBeforeStreamHasFinished() {
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            source: .stream(
                [self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)],
                dataSource
            ),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")
        
        // attach consumer
        let counter = ManagedAtomic(0)
        let future = stream.onRow { row in
            let expected = counter.loadThenWrappingIncrement(ordering: .relaxed)
            XCTAssertEqual(try row.decode(String.self, context: .default), "\(expected)")
        }
        XCTAssertEqual(counter.load(ordering: .relaxed), 2)
        XCTAssertEqual(dataSource.hitDemand, 1)
        
        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(counter.load(ordering: .relaxed), 4)
        XCTAssertEqual(dataSource.hitDemand, 2)
        
        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(counter.load(ordering: .relaxed), 6)
        XCTAssertEqual(dataSource.hitDemand, 3)
        
        stream.receive(completion: .success("SELECT 6"))
        
        XCTAssertNoThrow(try future.wait())
        XCTAssertEqual(stream.commandTag, "SELECT 6")
    }

    func testEmptyResponseHasEmptyCommandTag() {
        let stream = PSQLRowStream(
            source: .noRows(.success(.emptyResponse)),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        XCTAssertEqual(try stream.all().wait(), [])
        XCTAssertEqual(stream.commandTag, "")
    }

    func testOnRowOnFailedStream() {
        let stream = PSQLRowStream(
            source: .noRows(.failure(PSQLError.serverClosedConnection(underlying: nil))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )

        let counter = ManagedAtomic(0)
        let future = stream.onRow { _ in
            counter.wrappingIncrement(ordering: .relaxed)
        }

        XCTAssertEqual(counter.load(ordering: .relaxed), 0, "Expected the closure to never be called")
        XCTAssertThrowsError(try future.wait()) {
            XCTAssertEqual($0 as? PSQLError, .serverClosedConnection(underlying: nil))
        }
    }

    func testOnRowThrowsErrorOnLaterBatch() {
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
            XCTAssertEqual(try row.decode(String.self, context: .default), "\(expected)")
            if expected == 2 {
                throw OnRowError(row: expected)
            }
        }
        XCTAssertEqual(dataSource.hitDemand, 1)

        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        XCTAssertEqual(counter.load(ordering: .relaxed), 2)
        XCTAssertEqual(dataSource.hitDemand, 2)
        XCTAssertEqual(dataSource.hitCancel, 0)

        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(counter.load(ordering: .relaxed), 3, "Expected the stream to stop forwarding rows after the throw")
        XCTAssertEqual(dataSource.hitDemand, 2, "Expected no further demand after the throw")
        XCTAssertEqual(dataSource.hitCancel, 1, "Expected the data source to be cancelled")

        XCTAssertThrowsError(try future.wait()) {
            XCTAssertEqual($0 as? OnRowError, OnRowError(row: 2))
        }

        // everything the connection sends after the failure must be ignored
        stream.receive([[ByteBuffer(string: "4")]])
        stream.receive(completion: .success("SELECT 4"))
        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))
        XCTAssertEqual(counter.load(ordering: .relaxed), 3)
        XCTAssertEqual(dataSource.hitCancel, 1)
    }

    func testOnRowClosureThrowsAfterTheStreamWasCompletedReentrantly() {
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
        XCTAssertEqual(counter.load(ordering: .relaxed), 1)
        XCTAssertEqual(dataSource.hitCancel, 0, "Expected the data source to not be cancelled, since it is done already")

        XCTAssertNoThrow(try future.wait())
        XCTAssertEqual(stream.commandTag, "SELECT 1")
    }

    func testOnRowFailsIfStreamFails() {
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
        XCTAssertEqual(dataSource.hitDemand, 1)

        stream.receive([[ByteBuffer(string: "0")]])
        XCTAssertEqual(counter.load(ordering: .relaxed), 1)

        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))

        XCTAssertThrowsError(try future.wait()) {
            XCTAssertEqual($0 as? PSQLError, .serverClosedConnection(underlying: nil))
        }
    }

    func testGetArrayFailsIfStreamFails() {
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
        XCTAssertEqual(dataSource.hitDemand, 1)

        stream.receive([[ByteBuffer(string: "0")]])
        XCTAssertEqual(dataSource.hitDemand, 2)

        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))

        XCTAssertThrowsError(try future.wait()) {
            XCTAssertEqual($0 as? PSQLError, .serverClosedConnection(underlying: nil))
        }
    }

    func testCompletionsAfterTheStreamHasFinishedAreIgnored() {
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

        XCTAssertEqual(try future.wait().count, 1)
        XCTAssertEqual(stream.commandTag, "SELECT 1")

        // further completions must not change the outcome
        stream.receive(completion: .success("SELECT 2"))
        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))
        XCTAssertEqual(stream.commandTag, "SELECT 1")
    }

    func testConsumersCanBeAttachedFromOffTheEventLoop() async throws {
        let eventLoop = NIOSingletons.posixEventLoopGroup.next()
        let logger = self.logger

        let allStream = try await eventLoop.submit {
            PSQLRowStream(
                source: .noRows(.success(.tag("INSERT 0 1"))),
                eventLoop: eventLoop,
                logger: logger
            )
        }.get()

        XCTAssertFalse(eventLoop.inEventLoop)
        let rows = try await allStream.all().get()
        XCTAssertEqual(rows, [])

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
        XCTAssertEqual(counter.load(ordering: .relaxed), 0)
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
