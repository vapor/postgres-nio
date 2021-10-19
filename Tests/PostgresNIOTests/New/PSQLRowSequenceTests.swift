import NIOCore
import NIOConcurrencyHelpers
import XCTest
import Logging
@testable import PostgresNIO

#if swift(>=5.5) && canImport(_Concurrency)
@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
class PSQLRowSequenceTests: XCTestCase {
    func testSimpleSelect() { XCTAsyncTest {
        let embedded = EmbeddedEventLoop()
        let rowDescription: [RowDescription.Column] = [
            .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
        ]
        let logger = Logger(label: "test")
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM foo",
            bind: [],
            logger: logger,
            jsonDecoder: JSONDecoder(),
            promise: embedded.makePromise(of: PSQLRowStream.self)
        )
        let dataSource = CountDataSource()
        let stream = PSQLRowStream(
            rowDescription: rowDescription,
            queryContext: queryContext,
            eventLoop: embedded,
            rowSource: .stream(dataSource)
        )
        queryContext.promise.succeed(stream)
        
        let row1: DataRow = [ByteBuffer(integer: 0)]
        stream.receive([row1])
        stream.receive(completion: .success("SELECT 1"))
        let sequence = stream.asyncSequence()
        
        for try await row in sequence {
            print("\(row)")
        }
    } }
    
    func testBackpressure() { XCTAsyncTest {
        let embedded = EmbeddedEventLoop()
        let rowDescription: [RowDescription.Column] = [
            .init(name: "test", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: 0, format: .binary)
        ]
        let logger = Logger(label: "test")
        let queryContext = ExtendedQueryContext(
            query: "SELECT * FROM foo",
            bind: [],
            logger: logger,
            jsonDecoder: JSONDecoder(),
            promise: embedded.makePromise(of: PSQLRowStream.self)
        )
        let dataSource = BlockingDataSource()
        let stream = PSQLRowStream(
            rowDescription: rowDescription,
            queryContext: queryContext,
            eventLoop: embedded,
            rowSource: .stream(dataSource)
        )
        queryContext.promise.succeed(stream)
        
        @Sendable func workaround() {
            // for the first rows the consumer doesn't signal demand
            let row1Data: DataRow = [Int(0)]
            stream.receive([row1Data])
            
            for i in 1..<1000 {
                XCTAssertNoThrow(try dataSource.waitForDemand(deadline: .now() + .seconds(10)))
                
                let rowData: DataRow = [Int(i)]
                stream.receive([rowData])
            }
            
            // After 1000 rows, send end!
            XCTAssertNoThrow(try dataSource.waitForDemand(deadline: .now() + .seconds(10)))
            stream.receive(completion: .success("SELECT 1"))
        }
        
        DispatchQueue(label: "source").async { workaround() }
        
        var consumed = 0
        for try await int in stream.asyncSequence().decode(Int.self) {
            XCTAssertEqual(int, consumed)
            consumed += 1
            XCTAssertEqual(dataSource.demandCounter, consumed)
        }
    } }
}

final class CountDataSource: PSQLRowsDataSource {
    
    var hitRequestCounter: Int {
        self._hitRequestCounter.load()
    }
    
    var hitCancelCounter: Int {
        self._hitCancelCounter.load()
    }
    
    private let _hitRequestCounter = NIOAtomic<Int>.makeAtomic(value: 0)
    private let _hitCancelCounter = NIOAtomic<Int>.makeAtomic(value: 0)
    
    init() {}
    
    func request(for stream: PSQLRowStream) {
        self._hitRequestCounter.add(1)
    }
    
    func cancel(for stream: PSQLRowStream) {
        self._hitCancelCounter.add(1)
    }
}

final class BlockingDataSource: PSQLRowsDataSource {
    
    struct TimeoutError: Error {}

    private let demandLock = ConditionLock(value: false)
    private var _demandCounter = 0
    
    var demandCounter: Int {
        self.demandLock.lock()
        defer { self.demandLock.unlock() }
        return self._demandCounter
    }
    
    init() {}
    
    func request(for stream: PSQLRowStream) {
        self.demandLock.lock()
        self._demandCounter += 1
        self.demandLock.unlock(withValue: true)
    }
    
    func waitForDemand(deadline: NIODeadline) throws {
        let secondsUntilDeath = deadline - NIODeadline.now()
        
        guard self.demandLock.lock(whenValue: true, timeoutSeconds: .init(secondsUntilDeath.nanoseconds / 1_000_000_000)) else {
            throw TimeoutError()
        }
        self.demandLock.unlock(withValue: false)
    }
    
    func cancel(for stream: PSQLRowStream) {
        preconditionFailure()
    }
}
#endif

#if swift(>=5.5) && canImport(_Concurrency)
// NOTE: workaround until we have async test support on linux
//         https://github.com/apple/swift-corelibs-xctest/pull/326
extension XCTestCase {
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func XCTAsyncTest(
        expectationDescription: String = "Async operation",
        timeout: TimeInterval = 3,
        file: StaticString = #file,
        line: Int = #line,
        operation: @escaping () async throws -> Void
    ) {
        let expectation = self.expectation(description: expectationDescription)
        Task {
            do { try await operation() }
            catch {
                XCTFail("Error thrown while executing async function @ \(file):\(line): \(error)")
                Thread.callStackSymbols.forEach { print($0) }
            }
            expectation.fulfill()
        }
        self.wait(for: [expectation], timeout: timeout)
    }
}
#endif

