import Atomics
import NIOEmbedded
import NIOPosix
import Testing
@testable import PostgresNIO
import NIOCore
import Logging

@Suite struct PostgresRowSequenceTests {
    let logger = Logger(label: "PSQLRowStreamTests")

    @Test func testBackpressureWorks() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ], 
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let rowSequence = stream.asyncSequence()
        #expect(dataSource.requestCount == 0)
        let dataRow: DataRow = [ByteBuffer(integer: Int64(1))]
        stream.receive([dataRow])

        var iterator = rowSequence.makeAsyncIterator()
        let row = try await iterator.next()
        #expect(dataSource.requestCount == 1)
        #expect(row?.data == dataRow)

        stream.receive(completion: .success("SELECT 1"))
        let empty = try await iterator.next()
        #expect(empty == nil)
    }


    @Test func testCancellationWorksWhileIterating() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let rowSequence = stream.asyncSequence()
        #expect(dataSource.requestCount == 0)
        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)

        var counter = 0
        for try await row in rowSequence {
            #expect(try row.decode(Int.self) == counter)
            counter += 1

            if counter == 64 {
                break
            }
        }

        #expect(dataSource.cancelCount == 1)
    }

    @Test func testCancellationWorksBeforeIterating() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let rowSequence = stream.asyncSequence()
        #expect(dataSource.requestCount == 0)
        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)

        var iterator: PostgresRowSequence.AsyncIterator? = rowSequence.makeAsyncIterator()
        iterator = nil

        #expect(dataSource.cancelCount == 1)
        #expect(iterator == nil, "Surpress warning")
    }

    @Test func testDroppingTheSequenceCancelsTheSource() throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        var rowSequence: PostgresRowSequence? = stream.asyncSequence()
        rowSequence = nil

        #expect(dataSource.cancelCount == 1)
        #expect(rowSequence == nil, "Surpress warning")
    }

    @Test func testStreamBasedOnCompletedQuery() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let rowSequence = stream.asyncSequence()
        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)
        stream.receive(completion: .success("SELECT 128"))

        var counter = 0
        for try await row in rowSequence {
            #expect(try row.decode(Int.self) == counter)
            counter += 1
        }

        #expect(dataSource.cancelCount == 0)
    }

    @Test func testStreamIfInitializedWithAllData() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let dataRows: [DataRow] = (0..<128).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(dataRows)
        stream.receive(completion: .success("SELECT 128"))

        let rowSequence = stream.asyncSequence()

        var counter = 0
        for try await row in rowSequence {
            #expect(try row.decode(Int.self) == counter)
            counter += 1
        }

        #expect(dataSource.cancelCount == 0)
    }

    @Test func testStreamIfInitializedWithError() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))

        let rowSequence = stream.asyncSequence()

        do {
            var counter = 0
            for try await _ in rowSequence {
                counter += 1
            }
            Issue.record("Expected that an error was thrown before.")
        } catch {
            #expect(error as? PSQLError == .serverClosedConnection(underlying: nil))
        }
    }

    @Test func testSucceedingRowContinuationsWorks() async throws {
        let dataSource = MockRowDataSource()
        let eventLoop = NIOSingletons.posixEventLoopGroup.next()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: eventLoop,
            logger: self.logger
        )

        let rowSequence = try await eventLoop.submit { stream.asyncSequence() }.get()
        var rowIterator = rowSequence.makeAsyncIterator()

        eventLoop.scheduleTask(in: .seconds(1)) {
            let dataRows: [DataRow] = (0..<1).map { [ByteBuffer(integer: Int64($0))] }
            stream.receive(dataRows)
        }

        let row1 = try await rowIterator.next()
        #expect(try row1?.decode(Int.self) == 0)

        eventLoop.scheduleTask(in: .seconds(1)) {
            stream.receive(completion: .success("SELECT 1"))
        }

        let row2 = try await rowIterator.next()
        #expect(row2 == nil)
    }

    @Test func testFailingRowContinuationsWorks() async throws {
        let dataSource = MockRowDataSource()
        let eventLoop = NIOSingletons.posixEventLoopGroup.next()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: eventLoop,
            logger: self.logger
        )

        let rowSequence = try await eventLoop.submit { stream.asyncSequence() }.get()
        var rowIterator = rowSequence.makeAsyncIterator()

        eventLoop.scheduleTask(in: .seconds(1)) {
            let dataRows: [DataRow] = (0..<1).map { [ByteBuffer(integer: Int64($0))] }
            stream.receive(dataRows)
        }

        let row1 = try await rowIterator.next()
        #expect(try row1?.decode(Int.self) == 0)

        eventLoop.scheduleTask(in: .seconds(1)) {
            stream.receive(completion: .failure(PSQLError.serverClosedConnection(underlying: nil)))
        }

        do {
            _ = try await rowIterator.next()
            Issue.record("Expected that an error was thrown before.")
        } catch {
            #expect(error as? PSQLError == .serverClosedConnection(underlying: nil))
        }
    }

    @Test func testAdaptiveRowBufferShrinksAndGrows() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let initialDataRows: [DataRow] = (0..<AdaptiveRowBuffer.defaultBufferTarget + 1).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(initialDataRows)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        #expect(dataSource.requestCount == 0)
        _ = try await rowIterator.next() // new buffer size will be target -> don't ask for more
        #expect(dataSource.requestCount == 0)
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        #expect(dataSource.requestCount == 1)

        // if the buffer gets new rows so that it has equal or more than target (the target size
        // should be halved), however shrinking is only allowed AFTER the first extra rows were
        // received.
        let addDataRows1: [DataRow] = [[ByteBuffer(integer: Int64(0))]]
        stream.receive(addDataRows1)
        #expect(dataSource.requestCount == 1)
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        #expect(dataSource.requestCount == 2)

        // if the buffer gets new rows so that it has equal or more than target (the target size
        // should be halved)
        let addDataRows2: [DataRow] = [[ByteBuffer(integer: Int64(0))], [ByteBuffer(integer: Int64(0))]]
        stream.receive(addDataRows2) // this should to target being halved.
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        for _ in 0..<(AdaptiveRowBuffer.defaultBufferTarget / 2) {
            _ = try await rowIterator.next() // Remove all rows until we are back at target
            #expect(dataSource.requestCount == 2)
        }

        // if we remove another row we should trigger getting new rows.
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        #expect(dataSource.requestCount == 3)

        // remove all remaining rows... this will trigger a target size double
        for _ in 0..<(AdaptiveRowBuffer.defaultBufferTarget/2 - 1) {
            _ = try await rowIterator.next() // Remove all rows until we are back at target
            #expect(dataSource.requestCount == 3)
        }

        let fillBufferDataRows: [DataRow] = (0..<AdaptiveRowBuffer.defaultBufferTarget + 1).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(fillBufferDataRows)

        #expect(dataSource.requestCount == 3)
        _ = try await rowIterator.next() // new buffer size will be target -> don't ask for more
        #expect(dataSource.requestCount == 3)
        _ = try await rowIterator.next() // new buffer will be (target - 1) -> ask for more
        #expect(dataSource.requestCount == 4)
    }

    @Test func testAdaptiveRowShrinksToMin() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        var currentTarget = AdaptiveRowBuffer.defaultBufferTarget

        let initialDataRows: [DataRow] = (0..<currentTarget).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(initialDataRows)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        // shrinking the buffer is only allowed after the first extra rows were received
        #expect(dataSource.requestCount == 0)
        _ = try await rowIterator.next()
        #expect(dataSource.requestCount == 1)

        stream.receive([[ByteBuffer(integer: Int64(1))]])

        var expectedRequestCount = 1

        while currentTarget > AdaptiveRowBuffer.defaultBufferMinimum {
            // the buffer is filled up to currentTarget at that point, if we remove one row and add
            // one row it should shrink
            #expect(dataSource.requestCount == expectedRequestCount)
            _ = try await rowIterator.next()
            expectedRequestCount += 1
            #expect(dataSource.requestCount == expectedRequestCount)

            stream.receive([[ByteBuffer(integer: Int64(1))], [ByteBuffer(integer: Int64(1))]])
            let newTarget = currentTarget / 2
            let toDrop = currentTarget + 1 - newTarget

            // consume all messages that are to much.
            for _ in 0..<toDrop {
                _ = try await rowIterator.next()
                #expect(dataSource.requestCount == expectedRequestCount)
            }

            currentTarget = newTarget
        }

        #expect(currentTarget == AdaptiveRowBuffer.defaultBufferMinimum)
    }

    @Test func testStreamBufferAcceptsNewRowsEventhoughItDidntAskForIt() async throws {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()
        let stream = PSQLRowStream(
            source: .stream(
                [
                    .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
                ],
                dataSource
            ),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let messagePerChunk = AdaptiveRowBuffer.defaultBufferTarget * 4
        let initialDataRows: [DataRow] = (0..<messagePerChunk).map { [ByteBuffer(integer: Int64($0))] }
        stream.receive(initialDataRows)

        let rowSequence = stream.asyncSequence()
        var rowIterator = rowSequence.makeAsyncIterator()

        #expect(dataSource.requestCount == 0)
        _ = try await rowIterator.next()
        #expect(dataSource.requestCount == 0)

        let finalDataRows: [DataRow] = (0..<messagePerChunk).map { [ByteBuffer(integer: Int64(messagePerChunk + $0))] }
        stream.receive(finalDataRows)
        stream.receive(completion: .success("SELECT \(2 * messagePerChunk)"))

        var counter = 1
        for _ in 0..<(2 * messagePerChunk - 1) {
            let row = try await rowIterator.next()
            #expect(try row?.decode(Int.self) == counter)
            counter += 1
        }

        let emptyRow = try await rowIterator.next()
        #expect(emptyRow == nil)
    }

    @Test func testGettingColumnsReturnsCorrectColumnInformation() {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()

        let sourceColumns = [
            RowDescription.Column(
                name: "id",
                tableOID: 12345,
                columnAttributeNumber: 1,
                dataType: .int8,
                dataTypeSize: 8,
                dataTypeModifier: -1,
                format: .binary
            ),
            RowDescription.Column(
                name: "name",
                tableOID: 12345,
                columnAttributeNumber: 2,
                dataType: .text,
                dataTypeSize: -1,
                dataTypeModifier: -1,
                format: .text
            )
        ]

        let expectedColumns = PostgresColumns(underlying: sourceColumns)

        let stream = PSQLRowStream(
            source: .stream(sourceColumns, dataSource),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let rowSequence = stream.asyncSequence()
        let actualColumns = rowSequence.columns

        #expect(actualColumns == expectedColumns)
    }

    @Test func testGettingColumnsWithEmptyColumns() {
        let dataSource = MockRowDataSource()
        let embeddedEventLoop = EmbeddedEventLoop()

        let stream = PSQLRowStream(
            source: .stream([], dataSource),
            eventLoop: embeddedEventLoop,
            logger: self.logger
        )

        let rowSequence = stream.asyncSequence()
        let columns = rowSequence.columns

        #expect(columns.isEmpty)
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
