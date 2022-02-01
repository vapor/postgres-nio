import NIOEmbedded
import NIOConcurrencyHelpers
import Dispatch
import XCTest
@testable import PostgresNIO

#if swift(>=5.5.2)
final class PSQLRowSequenceTests: XCTestCase {

    func testBackpressureWorks() async throws {
        let eventLoop = EmbeddedEventLoop()
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        let logger = Logger(label: "test")
        let dataSource = MockRowDataSource()
        let stream = PSQLRowStream(
            rowDescription: [
                .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
            ],
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
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
            XCTAssertEqual(try row.decode(column: 0, as: Int.self), counter)
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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        let rowSequence = stream.asyncSequence()
        XCTAssertEqual(dataSource.requestCount, 0)
        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)

        var iterator: PSQLRowSequence.Iterator? = rowSequence.makeAsyncIterator()
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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
            eventLoop: eventLoop,
            rowSource: .stream(dataSource)
        )
        promise.succeed(stream)

        var rowSequence: PSQLRowSequence? = stream.asyncSequence()
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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
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
            XCTAssertEqual(try row.decode(column: 0, as: Int.self), counter)
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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
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
            XCTAssertEqual(try row.decode(column: 0, as: Int.self), counter)
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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
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
        XCTAssertEqual(try row1?.decode(column: 0, as: Int.self), 0)

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
            queryContext: .init(query: "SELECT * FROM foo", bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: promise),
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
        XCTAssertEqual(try row1?.decode(column: 0, as: Int.self), 0)

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
}

final class MockRowDataSource: PSQLRowsDataSource {
    var requestCount: Int {
        self._requestCount.load()
    }

    var cancelCount: Int {
        self._cancelCount.load()
    }

    private let _requestCount = NIOAtomic.makeAtomic(value: 0)
    private let _cancelCount = NIOAtomic.makeAtomic(value: 0)

    func request(for stream: PSQLRowStream) {
        self._requestCount.add(1)
    }

    func cancel(for stream: PSQLRowStream) {
        self._cancelCount.add(1)
    }
}
#endif
