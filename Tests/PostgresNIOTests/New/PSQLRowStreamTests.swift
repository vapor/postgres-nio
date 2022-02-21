import NIOCore
import Logging
import XCTest
@testable import PostgresNIO

class PSQLRowStreamTests: XCTestCase {
    func testEmptyStream() {
        let logger = Logger(label: "test")
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        
        let queryContext = ExtendedQueryContext(
            query: "INSERT INTO foo bar;", logger: logger, promise: promise
        )
        
        let stream = PSQLRowStream(
            rowDescription: [],
            queryContext: queryContext,
            eventLoop: eventLoop,
            rowSource: .noRows(.success("INSERT 0 1"))
        )
        promise.succeed(stream)
        
        XCTAssertEqual(try stream.all().wait(), [])
        XCTAssertEqual(stream.commandTag, "INSERT 0 1")
    }
    
    func testFailedStream() {
        let logger = Logger(label: "test")
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM test;", logger: logger, promise: promise
        )
        
        let stream = PSQLRowStream(
            rowDescription: [],
            queryContext: queryContext,
            eventLoop: eventLoop,
            rowSource: .noRows(.failure(PSQLError.connectionClosed))
        )
        promise.succeed(stream)
        
        XCTAssertThrowsError(try stream.all().wait()) {
            XCTAssertEqual($0 as? PSQLError, .connectionClosed)
        }
    }
    
    func testGetArrayAfterStreamHasFinished() {
        let logger = Logger(label: "test")
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM test;", logger: logger, promise: promise
        )
        
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)
            ],
            queryContext: queryContext,
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)
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
        
        var rows: [PSQLRow]?
        XCTAssertNoThrow(rows = try future.wait())
        XCTAssertEqual(rows?.count, 2)
    }

    func testGetArrayBeforeStreamHasFinished() {
        let logger = Logger(label: "test")
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM test;", logger: logger, promise: promise)
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)
            ],
            queryContext: queryContext,
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)
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
        
        var rows: [PSQLRow]?
        XCTAssertNoThrow(rows = try future.wait())
        XCTAssertEqual(rows?.count, 6)
    }
    
    func testOnRowAfterStreamHasFinished() {
        let logger = Logger(label: "test")
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM test;", logger: logger, promise: promise
        )
        
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)
            ],
            queryContext: queryContext,
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        stream.receive(completion: .success("SELECT 2"))
        
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        // attach consumer
        var counter = 0
        let future = stream.onRow { row in
            XCTAssertEqual(try row.decode(column: 0, as: String.self), "\(counter)")
            counter += 1
        }
        XCTAssertEqual(counter, 2)
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        XCTAssertNoThrow(try future.wait())
        XCTAssertEqual(stream.commandTag, "SELECT 2")
    }

    func testOnRowThrowsErrorOnInitialBatch() {
        let logger = Logger(label: "test")
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM test;", logger: logger, promise: promise
        )
        
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)
            ],
            queryContext: queryContext,
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        stream.receive(completion: .success("SELECT 2"))
        
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        // attach consumer
        var counter = 0
        let future = stream.onRow { row in
            XCTAssertEqual(try row.decode(column: 0, as: String.self), "\(counter)")
            if counter == 1 {
                throw OnRowError(row: counter)
            }
            counter += 1
        }
        XCTAssertEqual(counter, 1)
        XCTAssertEqual(dataSource.hitDemand, 0)
        
        XCTAssertThrowsError(try future.wait()) {
            XCTAssertEqual($0 as? OnRowError, OnRowError(row: 1))
        }
    }

    
    func testOnRowBeforeStreamHasFinished() {
        let logger = Logger(label: "test")
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM test;", logger: logger, promise: promise
        )
        
        let dataSource = CountingDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                self.makeColumnDescription(name: "foo", dataType: .text, format: .binary)
            ],
            queryContext: queryContext,
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)
        XCTAssertEqual(dataSource.hitDemand, 0)
        XCTAssertEqual(dataSource.hitCancel, 0)
        
        stream.receive([
            [ByteBuffer(string: "0")],
            [ByteBuffer(string: "1")]
        ])
        
        XCTAssertEqual(dataSource.hitDemand, 0, "Before we have a consumer demand is not signaled")
        
        // attach consumer
        var counter = 0
        let future = stream.onRow { row in
            XCTAssertEqual(try row.decode(column: 0, as: String.self), "\(counter)")
            counter += 1
        }
        XCTAssertEqual(counter, 2)
        XCTAssertEqual(dataSource.hitDemand, 1)
        
        stream.receive([
            [ByteBuffer(string: "2")],
            [ByteBuffer(string: "3")]
        ])
        XCTAssertEqual(counter, 4)
        XCTAssertEqual(dataSource.hitDemand, 2)
        
        stream.receive([
            [ByteBuffer(string: "4")],
            [ByteBuffer(string: "5")]
        ])
        XCTAssertEqual(counter, 6)
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
