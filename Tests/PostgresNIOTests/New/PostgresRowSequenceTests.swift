import Atomics
import NIOEmbedded
import Dispatch
import XCTest
@testable import PostgresNIO
import NIOCore
import Logging

final class PostgresRowSequenceTests: XCTestCase {

    func testBackpressureWorks() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let rowSequence = stream.asyncSequence()
        XCTAssertEqual(dataSource.requestCount, 0)
        let dataRow: DataRow = [ByteBuffer(integer: Int64(1))]
        stream.receive([dataRow])

        var iterator = rowSequence.makeAsyncIterator()
        let row = try await iterator.next()
        XCTAssertEqual(dataSource.requestCount, 1)
        XCTAssertEqual(row?.data, dataRow)

        stream.receive(completion: .success("SELECT 1"))
        let empty = try await iterator.next()
        XCTAssertNil(empty)
    }

    func testCancellationWorksWhileIterating() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let rowSequence = stream.asyncSequence()
        XCTAssertEqual(dataSource.requestCount, 0)
        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)

        var counter = 0
        for try await row in rowSequence {
            XCTAssertEqual(try row.decode(Int.self, context: .default), counter)
            counter += 1

            if counter == 64 {
                break
            }
        }

        XCTAssertEqual(dataSource.cancelCount, 1)
    }

    func testCancellationWorksBeforeIterating() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let rowSequence = stream.asyncSequence()
        XCTAssertEqual(dataSource.requestCount, 0)
        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)

        var iterator: PostgresRowSequence.AsyncIterator? = rowSequence.makeAsyncIterator()
        iterator = nil

        XCTAssertEqual(dataSource.cancelCount, 1)
        XCTAssertNil(iterator, "Surpress warning")
    }

    func testDroppingTheSequenceCancelsTheSource() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        var rowSequence: PostgresRowSequence? = stream.asyncSequence()
        rowSequence = nil

        XCTAssertEqual(dataSource.cancelCount, 1)
        XCTAssertNil(rowSequence, "Surpress warning")
    }

    func testStreamBasedOnCompletedQuery() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let rowSequence = stream.asyncSequence()
        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)
        stream.receive(completion: .success("SELECT 128"))

        var counter = 0
        for try await row in rowSequence {
            XCTAssertEqual(try row.decode(Int.self, context: .default), counter)
            counter += 1
        }

        XCTAssertEqual(dataSource.cancelCount, 0)
    }

    func testStreamIfInitializedWithAllData() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)
        stream.receive(completion: .success("SELECT 128"))

        let rowSequence = stream.asyncSequence()

        var counter = 0
        for try await row in rowSequence {
            XCTAssertEqual(try row.decode(Int.self, context: .default), counter)
            counter += 1
        }

        XCTAssertEqual(dataSource.cancelCount, 0)
    }

    func testStreamIfInitializedWithError() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        stream.receive(completion: .failure(PSQLError.connectionClosed))

        let rowSequence = stream.asyncSequence()

        do {
            var counter = 0
            for try await _ in rowSequence {
                counter += 1
            }
            XCTFail("Expected that an error was thrown before.")
        } catch {
            XCTAssertEqual(error as? PSQLError, .connectionClosed)
        }
    }

    func testSucceedingRowContinuationsWorks() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(1)) {
            let dataRows: [DataRow] = (0..<1).map { [ByteBuffer(integer: Int64($0))] }
            stream.receive(dataRows)
        }

        let row1 = try await rowIterator.next()
        XCTAssertEqual(try row1?.decode(Int.self, context: .default), 0)

        DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(1)) {
            stream.receive(completion: .success("SELECT 1"))
        }

        let row2 = try await rowIterator.next()
        XCTAssertNil(row2)
    }

    func testFailingRowContinuationsWorks() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(1)) {
            let dataRows: [DataRow] = (0..<1).map { [ByteBuffer(integer: Int64($0))] }
            stream.receive(dataRows)
        }

        let row1 = try await rowIterator.next()
        XCTAssertEqual(try row1?.decode(Int.self, context: .default), 0)

        DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(1)) {
            stream.receive(completion: .failure(PSQLError.connectionClosed))
        }

        do {
            _ = try await rowIterator.next()
            XCTFail("Expected that an error was thrown before.")
        } catch {
            XCTAssertEqual(error as? PSQLError, .connectionClosed)
        }
    }

    func testAdaptiveRowBufferShrinksAndGrows() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let initialDataRows: [DataRow] = (0..<AdaptiveRowBuffer.defaultBufferTarget + 1).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(initialDataRows)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        XCTAssertEqual(dataSource.requestCount, 0)
        _ = try await rowIterator.next() // new buffer size will be target -> don't ask for more
        XCTAssertEqual(dataSource.requestCount, 0)
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        XCTAssertEqual(dataSource.requestCount, 1)

        // if the buffer gets new rows so that it has equal or more than target (the target size
        // should be halved), however shrinking is only allowed AFTER the first extra rows were
        // received.
        let addDataRows1: [DataRow] = [[ByteBuffer(integer: Int64(0))]]
        stream.receive(addDataRows1)
        XCTAssertEqual(dataSource.requestCount, 1)
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        XCTAssertEqual(dataSource.requestCount, 2)

        // if the buffer gets new rows so that it has equal or more than target (the target size
        // should be halved)
        let addDataRows2: [DataRow] = [[ByteBuffer(integer: Int64(0))], [ByteBuffer(integer: Int64(0))]]
        stream.receive(addDataRows2) // this should to target being halved.
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        for _ in 0..<(AdaptiveRowBuffer.defaultBufferTarget / 2) {
            _ = try await rowIterator.next() // Remove all rows until we are back at target
            XCTAssertEqual(dataSource.requestCount, 2)
        }

        // if we remove another row we should trigger getting new rows.
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        XCTAssertEqual(dataSource.requestCount, 3)

        // remove all remaining rows... this will trigger a target size double
        for _ in 0..<(AdaptiveRowBuffer.defaultBufferTarget/2 - 1) {
            _ = try await rowIterator.next() // Remove all rows until we are back at target
            XCTAssertEqual(dataSource.requestCount, 3)
        }

        let fillBufferDataRows: [DataRow] = (0..<AdaptiveRowBuffer.defaultBufferTarget + 1).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(fillBufferDataRows)

        XCTAssertEqual(dataSource.requestCount, 3)
        _ = try await rowIterator.next() // new buffer size will be target -> don't ask for more
        XCTAssertEqual(dataSource.requestCount, 3)
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        XCTAssertEqual(dataSource.requestCount, 4)
    }

    func testAdaptiveRowShrinksToMin() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        var currentTarget = AdaptiveRowBuffer.defaultBufferTarget

        let initialDataRows: [DataRow] = (0..<currentTarget).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(initialDataRows)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        // shrinking the buffer is only allowed after the first extra rows were received
        XCTAssertEqual(dataSource.requestCount, 0)
        _ = try await rowIterator.next()
        XCTAssertEqual(dataSource.requestCount, 1)

        stream.receive([[ByteBuffer(integer: Int64(1))]])

        var expectedRequestCount = 1

        while currentTarget > AdaptiveRowBuffer.defaultBufferMinimum {
            // the buffer is filled up to currentTarget at that point, if we remove one row and add
            // one row it should shrink
            XCTAssertEqual(dataSource.requestCount, expectedRequestCount)
            _ = try await rowIterator.next()
            expectedRequestCount += 1
            XCTAssertEqual(dataSource.requestCount, expectedRequestCount)

            stream.receive([[ByteBuffer(integer: Int64(1))], [ByteBuffer(integer: Int64(1))]])
            let newTarget = currentTarget / 2
            let toDrop = currentTarget + 1 - newTarget

            // consume all messages that are to much.
            for _ in 0..<toDrop {
                _ = try await rowIterator.next()
                XCTAssertEqual(dataSource.requestCount, expectedRequestCount)
            }

            currentTarget = newTarget
        }

        XCTAssertEqual(currentTarget, AdaptiveRowBuffer.defaultBufferMinimum)
    }

    func testStreamBufferAcceptsNewRowsEventhoughItDidntAskForIt() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", logger: logger, promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let messagePerChunk = AdaptiveRowBuffer.defaultBufferTarget * 4
        let initialDataRows: [DataRow] = (0..<messagePerChunk).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(initialDataRows)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        XCTAssertEqual(dataSource.requestCount, 0)
        _ = try await rowIterator.next()
        XCTAssertEqual(dataSource.requestCount, 0)

        let finalDataRows: [DataRow] = (0..<messagePerChunk).map { [ByteBuffer(integer: Int64(messagePerChunk + $0))] }
        stream.receive(finalDataRows)
        stream.receive(completion: .success("SELECT \(2 * messagePerChunk)"))

        var counter = 1
        for _ in 0..<(2 * messagePerChunk - 1) {
            let row = try await rowIterator.next()
            XCTAssertEqual(try row?.decode(Int.self, context: .default), counter)
            counter += 1
        }

        let emptyRow = try await rowIterator.next()
        XCTAssertNil(emptyRow)
    }

}

final class MockRowDataSource: PSQLRowsDataSource {
    var requestCount: Int {
        self._requestCount.load(ordering: .relaxed)
    }

    var cancelCount: Int {
        self._cancelCount.load(ordering: .relaxed)
    }

    private let _requestCount = ManagedAtomic(0)
    private let _cancelCount = ManagedAtomic(0)

    func request(for stream: PSQLRowStream) {
        self._requestCount.wrappingIncrement(ordering: .relaxed)
    }

    func cancel(for stream: PSQLRowStream) {
        self._cancelCount.wrappingIncrement(ordering: .relaxed)
    }
}
