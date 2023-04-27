import Logging
import XCTest
import PostgresNIO
#if canImport(Network)
import NIOTransportServices
#endif
import NIOPosix
import NIOCore

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
            var counter = 0
            for try await element in rows.decode(Int.self, context: .default) {
                XCTAssertEqual(element, counter + 1)
                counter += 1
            }

            XCTAssertEqual(counter, end)
        }
    }

    func testSelectTimeoutWhileLongRunningQuery() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000000

        try await withTestConnection(on: eventLoop) { connection -> () in
            try await connection.query("SET statement_timeout=1000;", logger: .psqlTest)

            let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
            var counter = 0
            do {
                for try await element in rows.decode(Int.self, context: .default) {
                    XCTAssertEqual(element, counter + 1)
                    counter += 1
                }
                XCTFail("Expected to get cancelled while reading the query")
            } catch {
                guard let error = error as? PSQLError else { return XCTFail("Unexpected error type") }

                print(error)

                XCTAssertEqual(error.code, .server)
                XCTAssertEqual(error.serverInfo?[.severity], "ERROR")
            }

            XCTAssertFalse(connection.isClosed, "Connection should survive!")

            for num in 0..<10 {
                for try await decoded in try await connection.query("SELECT \(num);", logger: .psqlTest).decode(Int.self) {
                    XCTAssertEqual(decoded, num)
                }
            }
        }
    }

    func testConnectionSurvives1kQueriesWithATypo() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection -> () in
            for _ in 0..<1000 {
                do {
                    try await connection.query("SELECT generte_series(\(start), \(end));", logger: .psqlTest)
                    XCTFail("Expected to throw from the request")
                } catch {
                    guard let error = error as? PSQLError else { return XCTFail("Unexpected error type: \(error)") }

                    XCTAssertEqual(error.code, .server)
                    XCTAssertEqual(error.serverInfo?[.severity], "ERROR")
                }
            }

            // the connection survived all of this, we can still run normal queries:

            for num in 0..<10 {
                for try await decoded in try await connection.query("SELECT \(num);", logger: .psqlTest).decode(Int.self) {
                    XCTAssertEqual(decoded, num)
                }
            }
        }
    }

    func testSelect10times10kRows() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let start = 1
        let end = 10000

        try await withTestConnection(on: eventLoop) { connection in
            await withThrowingTaskGroup(of: Void.self) { taskGroup in
                for _ in 0..<10 {
                    taskGroup.addTask {
                        try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
                    }
                }
            }
        }
    }

    func testBindMaximumParameters() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection in
            // Max binds limit is UInt16.max which is 65535 which is 3 * 5 * 17 * 257
            // Max columns limit is 1664, so we will only make 5 * 257 columns which is less
            // Then we will insert 3 * 17 rows
            // In the insertion, there will be a total of 3 * 17 * 5 * 257 == UInt16.max bindings
            // If the test is successful, it means Postgres supports UInt16.max bindings
            let columnsCount = 5 * 257
            let rowsCount = 3 * 17

            let createQuery = PostgresQuery(
                unsafeSQL: """
                CREATE TABLE table1 (
                \((0..<columnsCount).map({ #""int\#($0)" int NOT NULL"# }).joined(separator: ", "))
                );
                """
            )
            try await connection.query(createQuery, logger: .psqlTest)

            var binds = PostgresBindings(capacity: Int(UInt16.max))
            for _ in (0..<rowsCount) {
                for num in (0..<columnsCount) {
                    binds.append(num, context: .default)
                }
            }
            XCTAssertEqual(binds.count, Int(UInt16.max))

            let insertionValues = (0..<rowsCount).map { rowIndex in
                let indices = (0..<columnsCount).map { columnIndex -> String in
                    "$\(rowIndex * columnsCount + columnIndex + 1)"
                }
                return "(\(indices.joined(separator: ", ")))"
            }.joined(separator: ", ")
            let insertionQuery = PostgresQuery(
                unsafeSQL: "INSERT INTO table1 VALUES \(insertionValues)",
                binds: binds
            )
            try await connection.query(insertionQuery, logger: .psqlTest)

            let countQuery = PostgresQuery(unsafeSQL: "SELECT COUNT(*) FROM table1")
            let countRows = try await connection.query(countQuery, logger: .psqlTest)
            var countIterator = countRows.makeAsyncIterator()
            let insertedRowsCount = try await countIterator.next()?.decode(Int.self, context: .default)
            XCTAssertEqual(rowsCount, insertedRowsCount)

            let dropQuery = PostgresQuery(unsafeSQL: "DROP TABLE table1")
            try await connection.query(dropQuery, logger: .psqlTest)
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

    func testCancelTaskThatIsVeryLongRunningWhichAlsoFailsWhileInStreamingMode() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection -> () in
            try await connection.query("SET statement_timeout=1000;", logger: .psqlTest)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    let start = 1
                    let end = 10000000000

                    let rows = try await connection.query("SELECT generate_series(\(start), \(end));", logger: .psqlTest)
                    var counter = 0
                    do {
                        for try await element in rows.decode(Int.self, context: .default) {
                            XCTAssertEqual(element, counter + 1)
                            counter += 1
                        }
                        XCTFail("Expected to get cancelled while reading the query")
                    } catch is CancellationError {
                        // Expected
                    } catch {
                        XCTFail("Unexpected error: \(error)")
                    }

                    XCTAssertTrue(Task.isCancelled)
                    XCTAssertFalse(connection.isClosed, "Connection should survive!")
                }

                let delay = UInt64(0.2 * 1_000_000_000)
                try await Task.sleep(nanoseconds: delay)

                group.cancelAll()
            }

            try await connection.query("SELECT 1;", logger: .psqlTest)
        }
    }
}

extension XCTestCase {

    func withTestConnection<Result>(
        on eventLoop: EventLoop,
        file: StaticString = #filePath,
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
