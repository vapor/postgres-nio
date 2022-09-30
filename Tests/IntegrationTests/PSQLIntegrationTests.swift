import XCTest
import Logging
@testable import PostgresNIO
import NIOCore
import NIOPosix
import NIOTestUtils

final class IntegrationTests: XCTestCase {

    func testConnectAndClose() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        XCTAssertNoThrow(try conn?.close().wait())
    }

    func testAuthenticationFailure() throws {
        // If the postgres server trusts every connection, it is really hard to create an
        // authentication failure.
        try XCTSkipIf(env("POSTGRES_HOST_AUTH_METHOD") == "trust")

        let config = PostgresConnection.Configuration(
            connection: .init(
                host: env("POSTGRES_HOSTNAME") ?? "localhost",
                port: 5432
            ),
            authentication: .init(
                username: env("POSTGRES_USER") ?? "test_username",
                database: env("POSTGRES_DB") ?? "test_database",
                password: "wrong_password"
            ),
            tls: .disable
        )

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        var logger = Logger.psqlTest
        logger.logLevel = .info

        var connection: PostgresConnection?
        XCTAssertThrowsError(connection = try PostgresConnection.connect(on: eventLoopGroup.next(), configuration: config, id: 1, logger: logger).wait()) {
            XCTAssertTrue($0 is PSQLError)
        }

        // In case of a test failure the created connection must be closed.
        XCTAssertNoThrow(try connection?.close().wait())
    }

    func testQueryVersion() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("SELECT version()", logger: .psqlTest).wait())
        let rows = result?.rows
        var version: String?
        XCTAssertNoThrow(version = try rows?.first?.decode(String.self, context: .default))
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }

    func testQuery10kItems() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var metadata: PostgresQueryMetadata?
        var received: Int64 = 0
        XCTAssertNoThrow(metadata = try conn?.query("SELECT generate_series(1, 10000);", logger: .psqlTest) { row in
            func workaround() {
                var number: Int64?
                XCTAssertNoThrow(number = try row.decode(Int64.self, context: .default))
                received += 1
                XCTAssertEqual(number, received)
            }

            workaround()
        }.wait())

        XCTAssertEqual(received, 10000)
        XCTAssertEqual(metadata?.command, "SELECT")
        XCTAssertEqual(metadata?.rows, 10000)
    }

    func test1kRoundTrips() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        for _ in 0..<1_000 {
            var result: PostgresQueryResult?
            XCTAssertNoThrow(result = try conn?.query("SELECT version()", logger: .psqlTest).wait())
            var version: String?
            XCTAssertNoThrow(version = try result?.rows.first?.decode(String.self, context: .default))
            XCTAssertEqual(version?.contains("PostgreSQL"), true)
        }
    }

    func testQuerySelectParameter() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("SELECT \("hello")::TEXT as foo", logger: .psqlTest).wait())
        var foo: String?
        XCTAssertNoThrow(foo = try result?.rows.first?.decode(String.self, context: .default))
        XCTAssertEqual(foo, "hello")
    }

    func testDecodeIntegers() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("""
        SELECT
            1::SMALLINT                   as smallint,
            -32767::SMALLINT              as smallint_min,
            32767::SMALLINT               as smallint_max,
            1::INT                        as int,
            -2147483647::INT              as int_min,
            2147483647::INT               as int_max,
            1::BIGINT                     as bigint,
            -9223372036854775807::BIGINT  as bigint_min,
            9223372036854775807::BIGINT   as bigint_max
        """, logger: .psqlTest).wait())

        XCTAssertEqual(result?.rows.count, 1)
        let row = result?.rows.first

        var cells: (Int16, Int16, Int16, Int32, Int32, Int32, Int64, Int64, Int64)?
        XCTAssertNoThrow(cells = try row?.decode((Int16, Int16, Int16, Int32, Int32, Int32, Int64, Int64, Int64).self, context: .default))

        XCTAssertEqual(cells?.0, 1)
        XCTAssertEqual(cells?.1, -32_767)
        XCTAssertEqual(cells?.2, 32_767)
        XCTAssertEqual(cells?.3, 1)
        XCTAssertEqual(cells?.4, -2_147_483_647)
        XCTAssertEqual(cells?.5, 2_147_483_647)
        XCTAssertEqual(cells?.6, 1)
        XCTAssertEqual(cells?.7, -9_223_372_036_854_775_807)
        XCTAssertEqual(cells?.8, 9_223_372_036_854_775_807)
    }

    func testEncodeAndDecodeIntArray() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        let array: [Int64] = [1, 2, 3]
        XCTAssertNoThrow(result = try conn?.query("SELECT \(array)::int8[] as array", logger: .psqlTest).wait())
        XCTAssertEqual(result?.rows.count, 1)
        XCTAssertEqual(try result?.rows.first?.decode([Int64].self, context: .default), array)
    }

    func testDecodeEmptyIntegerArray() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("SELECT '{}'::int[] as array", logger: .psqlTest).wait())

        XCTAssertEqual(result?.rows.count, 1)
        XCTAssertEqual(try result?.rows.first?.decode([Int64].self, context: .default), [])
    }

    func testDoubleArraySerialization() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        let doubles: [Double] = [3.14, 42]
        XCTAssertNoThrow(result = try conn?.query("SELECT \(doubles)::double precision[] as doubles", logger: .psqlTest).wait())
        XCTAssertEqual(result?.rows.count, 1)
        XCTAssertEqual(try result?.rows.first?.decode([Double].self, context: .default), doubles)
    }

    func testDecodeDates() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("""
            SELECT
                '2016-01-18 01:02:03 +0042'::DATE         as date,
                '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
                '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
            """, logger: .psqlTest).wait())

        XCTAssertEqual(result?.rows.count, 1)

        var cells: (Date, Date, Date)?
        XCTAssertNoThrow(cells = try result?.rows.first?.decode((Date, Date, Date).self, context: .default))

        XCTAssertEqual(cells?.0.description, "2016-01-18 00:00:00 +0000")
        XCTAssertEqual(cells?.1.description, "2016-01-18 01:02:03 +0000")
        XCTAssertEqual(cells?.2.description, "2016-01-18 00:20:03 +0000")
    }

    func testDecodeDecimals() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("""
            SELECT
                \(Decimal(string: "123456.789123")!)::numeric     as numeric,
                \(Decimal(string: "-123456.789123")!)::numeric     as numeric_negative
            """, logger: .psqlTest).wait())
        XCTAssertEqual(result?.rows.count, 1)

        var cells: (Decimal, Decimal)?
        XCTAssertNoThrow(cells = try result?.rows.first?.decode((Decimal, Decimal).self, context: .default))

        XCTAssertEqual(cells?.0, Decimal(string: "123456.789123"))
        XCTAssertEqual(cells?.1, Decimal(string: "-123456.789123"))
    }

    func testRoundTripUUID() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        let uuidString = "2c68f645-9ca6-468b-b193-ee97f241c2f8"

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("""
            SELECT \(uuidString)::UUID as uuid
            """,
            logger: .psqlTest
        ).wait())

        XCTAssertEqual(result?.rows.count, 1)
        XCTAssertEqual(try result?.rows.first?.decode(UUID.self, context: .default), UUID(uuidString: uuidString))
    }

    func testRoundTripJSONB() {
        struct Object: Codable, PostgresCodable {
            let foo: Int
            let bar: Int
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        do {
            var result: PostgresQueryResult?
            XCTAssertNoThrow(result = try conn?.query("""
                select \(Object(foo: 1, bar: 2))::jsonb as jsonb
                """, logger: .psqlTest).wait())

            XCTAssertEqual(result?.rows.count, 1)
            var obj: Object?
            XCTAssertNoThrow(obj = try result?.rows.first?.decode(Object.self, context: .default))
            XCTAssertEqual(obj?.foo, 1)
            XCTAssertEqual(obj?.bar, 2)
        }

        do {
            var result: PostgresQueryResult?
            XCTAssertNoThrow(result = try conn?.query("""
                select \(Object(foo: 1, bar: 2))::json as json
                """, logger: .psqlTest).wait())

            XCTAssertEqual(result?.rows.count, 1)
            var obj: Object?
            XCTAssertNoThrow(obj = try result?.rows.first?.decode(Object.self, context: .default))
            XCTAssertEqual(obj?.foo, 1)
            XCTAssertEqual(obj?.bar, 2)
        }
    }
    
#if swift(>=5.5.2)
    func testBindMaximumParameters() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        try await withTestConnection(on: eventLoop) { connection in
            // Mac binds limit is UInt16.max which is 65535 which is 3 * 5 * 17 * 257
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
            
            var binds = PostgresBindings()
            for _ in (0..<rowsCount) {
                for num in (0..<columnsCount) {
                    try binds.append(num, context: .default)
                }
            }
            XCTAssertEqual(binds.count, Int(UInt16.max))
            
            let insertionValues = (0..<rowsCount).map { rowIndex in
                let indices = (0..<columnsCount).map { columnIndex -> String in
                    "$\(rowIndex * 1_000 + columnIndex + 1)"
                }
                return "(\(indices.joined(separator: ", ")))"
            }.joined(separator: ", ")
            let insertionQuery = PostgresQuery(
                unsafeSQL: "INSERT INTO table1 VALUES \(insertionValues)",
                binds: binds
            )
            try await connection.query(insertionQuery, logger: .psqlTest)
            
            let dropQuery = PostgresQuery(unsafeSQL: "DROP TABLE table1")
            try await connection.query(dropQuery, logger: .psqlTest)
        }
    }
#endif
}
