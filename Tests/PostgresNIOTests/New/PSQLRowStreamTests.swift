import Atomics
import NIOCore
import Logging
import XCTest
@testable import PostgresNIO
import NIOCore
import NIOEmbedded

final class PSQLRowStreamTests: XCTestCase {
    let logger = Logger(label: "PSQLRowStreamTests")
    let eventLoop = EmbeddedEventLoop()

    func testEmptyStream() {
        let stream = PSQLRowStream(
            source: .noRows(.success("INSERT 0 1")),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        
        XCTAssertEqual(try stream.all().wait(), [])
        XCTAssertEqual(stream.commandTag, "INSERT 0 1")
    }
    
    func testFailedStream() {
        let stream = PSQLRowStream(
            source: .noRows(.failure(PostgresError.serverClosedConnection(underlying: nil))),
            eventLoop: self.eventLoop,
            logger: self.logger
        )
        
        XCTAssertThrowsError(try stream.all().wait()) {
            XCTAssertEqual($0 as? PostgresError, .serverClosedConnection(underlying: nil))
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
