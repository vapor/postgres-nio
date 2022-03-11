import Logging
import XCTest
import PostgresNIO
#if canImport(Network)
import NIOTransportServices
#endif

#if swift(>=5.5.2)
final class AsyncPostgresConnectionTests: XCTestCase {

    func test1kRoundTrips() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection in
            for _ in 0..<1_000 {
                let rows = try await connection.query("SELECT version()", logger: .psqlTest)
                var iterator = rows.makeAsyncIterator()
                let firstRow = try await iterator.next()
                XCTAssertEqual(try firstRow?.decode(String.self, context: .default).contains("PostgreSQL"), true)
                let done = try await iterator.next()
                XCTAssertNil(done)
            }
        }
    }

    func testSelect10kRows() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection in
            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 1
            for try await element in rows.decode(Int.self, context: .default) {
                XCTAssertEqual(element, counter)
                counter += 1
            }

            XCTAssertEqual(counter, end + 1)
        }
    }

    #if canImport(Network)
    func testSelect10kRowsNetworkFramework() async throws {
        let eventLoopGroup = NIOTSEventLoopGroup()
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection in
            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 1
            for try await element in rows.decode(Int.self, context: .default) {
                XCTAssertEqual(element, counter)
                counter += 1
            }

            XCTAssertEqual(counter, end + 1)
        }
    }
    #endif
}

extension XCTestCase {

    func withTestConnection<Result>(
        on eventLoop: EventLoop,
        file: StaticString = #file,
        line: UInt = #line,
        _ closure: (PostgresConnection) async throws -> Result
    ) async throws -> Result  {
        let connection = try await PostgresConnection.test(on: eventLoop).get()

        do {
            let result = try await closure(connection)
            try await connection.close()
            return result
        } catch {
            XCTFail("Unexpected error: \(error)", file: file, line: line)
            try await connection.close()
            throw error
        }
    }
}
#endif
