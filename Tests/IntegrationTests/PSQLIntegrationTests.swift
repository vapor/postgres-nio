import Atomics
import XCTest
import Logging
import PostgresNIO
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
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: env("POSTGRES_PORT").flatMap(Int.init(_:)) ?? 5432,
            username: env("POSTGRES_USER") ?? "test_username",
            password: "wrong_password",
            database: env("POSTGRES_DB") ?? "test_database",
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
        let received = ManagedAtomic<Int64>(0)
        XCTAssertNoThrow(metadata = try conn?.query("SELECT generate_series(1, 10000);", logger: .psqlTest) { row in
            func workaround() {
                let expected = received.wrappingIncrementThenLoad(ordering: .relaxed)
                XCTAssertEqual(expected, try row.decode(Int64.self, context: .default))
            }

            workaround()
        }.wait())

        XCTAssertEqual(received.load(ordering: .relaxed), 10000)
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

    func testQueryNothing() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        var _result: PostgresQueryResult?
        XCTAssertNoThrow(_result = try conn?.query("""
            -- Some comments
            """, logger: .psqlTest).wait())

        let result = try XCTUnwrap(_result)
        XCTAssertEqual(result.rows, [])
        XCTAssertEqual(result.metadata.command, "")
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
                \(Decimal(string: "-123456.789123")!)::numeric    as numeric_negative
            """, logger: .psqlTest).wait())
        XCTAssertEqual(result?.rows.count, 1)

        var cells: (Decimal, Decimal)?
        XCTAssertNoThrow(cells = try result?.rows.first?.decode((Decimal, Decimal).self, context: .default))

        XCTAssertEqual(cells?.0, Decimal(string: "123456.789123"))
        XCTAssertEqual(cells?.1, Decimal(string: "-123456.789123"))
    }

    func testDecodeRawRepresentables() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }

        enum StringRR: String, PostgresDecodable {
            case a
        }

        enum IntRR: Int, PostgresDecodable {
            case b
        }

        let stringValue = StringRR.a
        let intValue = IntRR.b

        var result: PostgresQueryResult?
        XCTAssertNoThrow(result = try conn?.query("""
            SELECT
                \(stringValue.rawValue)::varchar     as string,
                \(intValue.rawValue)::int8           as int
            """, logger: .psqlTest).wait())
        XCTAssertEqual(result?.rows.count, 1)

        var cells: (StringRR, IntRR)?
        XCTAssertNoThrow(cells = try result?.rows.first?.decode((StringRR, IntRR).self, context: .default))

        XCTAssertEqual(cells?.0, stringValue)
        XCTAssertEqual(cells?.1, intValue)
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
    
    func testCopyIntoFrom() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let conn = try await PostgresConnection.test(on: eventLoop).get()
        defer { XCTAssertNoThrow(try conn.close().wait()) }

        _ = try? await conn.query("DROP TABLE copy_table", logger: .psqlTest).get()
        _ = try await conn.query("CREATE TABLE copy_table (id INT, name VARCHAR(100))", logger: .psqlTest).get()

        var options = PostgresCopyFromFormat.TextOptions()
        options.delimiter = ","
        try await conn.copyFrom(table: "copy_table", columns: ["id", "name"], format: .text(options), logger: .psqlTest) { writer in
            let records: [(id: Int, name: String)] = [
                (1, "Alice"),
                (42, "Bob")
            ]
            for record in records {
                var buffer = ByteBuffer()
                buffer.writeString("\(record.id),\(record.name)\n")
                try await writer.write(buffer)
            }
        }
        let rows = try await conn.query("SELECT id, name FROM copy_table").get().rows.map { try $0.decode((Int, String).self) }
        guard rows.count == 2 else {
            XCTFail("Expected 2 columns, received \(rows.count)")
            return
        }
        XCTAssertEqual(rows[0].0, 1)
        XCTAssertEqual(rows[0].1, "Alice")
        XCTAssertEqual(rows[1].0, 42)
        XCTAssertEqual(rows[1].1, "Bob")
    }

    func testCopyIntoFromIsTerminatedByThrowingErrorFromClosure() async throws {
        struct MyError: Error, CustomStringConvertible {
            var description: String { "My error" }
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let conn = try await PostgresConnection.test(on: eventLoop).get()
        defer { XCTAssertNoThrow(try conn.close().wait()) }

        _ = try? await conn.query("DROP TABLE copy_table", logger: .psqlTest).get()
        _ = try await conn.query("CREATE TABLE copy_table (id INT, name VARCHAR(100))", logger: .psqlTest).get()

        do {
            try await conn.copyFrom(table: "copy_table", columns: ["id", "name"], logger: .psqlTest) { writer in
                throw MyError()
            }
            XCTFail("Expected error to be thrown")
        } catch {
            XCTAssert(error is MyError, "Expected error of type MyError, got \(String(reflecting: error))")
        }
    }


    func testCopyIntoFromHasBadFormat() async throws {
        struct MyError: Error, CustomStringConvertible {
            var description: String { "My error" }
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let conn = try await PostgresConnection.test(on: eventLoop).get()
        defer { XCTAssertNoThrow(try conn.close().wait()) }

        _ = try? await conn.query("DROP TABLE copy_table", logger: .psqlTest).get()
        _ = try await conn.query("CREATE TABLE copy_table (id INT, name VARCHAR(100))", logger: .psqlTest).get()

        do {
            try await conn.copyFrom(table: "copy_table", columns: ["id", "name"], logger: .psqlTest) { writer in
                try await writer.write(ByteBuffer(staticString: "1Alice\n"))
            }
            XCTFail("Expected error to be thrown")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.serverInfo?[.sqlState], "22P02") // invalid_text_representation
        }
    }

    func testSyntaxErrorInGeneratedQuery() async throws {
        struct MyError: Error, CustomStringConvertible {
            var description: String { "My error" }
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let conn = try await PostgresConnection.test(on: eventLoop).get()
        defer { XCTAssertNoThrow(try conn.close().wait()) }

        do {
            // Use some form of input that generates an invalid query, the exact manner of its invalidness doesn't matter
            try await conn.copyFrom(table: "", logger: .psqlTest) { writer in
                XCTFail("Did not expect to call writeData")
            }
            XCTFail("Expected error to be thrown")
        } catch {
            XCTAssertEqual((error as? PSQLError)?.serverInfo?[.sqlState], "42601") // scanner_yyerror
        }
    }

    func testCopyFromBinary() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        let conn = try await PostgresConnection.test(on: eventLoop).get()
        defer { XCTAssertNoThrow(try conn.close().wait()) }

        _ = try? await conn.query("DROP TABLE copy_table", logger: .psqlTest).get()
        _ = try await conn.query("CREATE TABLE copy_table (id INT, name VARCHAR(100))", logger: .psqlTest).get()

        try await conn.copyFromBinary(table: "copy_table", columns: ["id", "name"], logger: .psqlTest) { writer in
            let records: [(id: Int, name: String)] = [
                (1, "Alice"),
                (42, "Bob")
            ]
            for record in records {
                try await writer.writeRow { columnWriter in
                    try columnWriter.writeColumn(Int32(record.id))
                    try columnWriter.writeColumn(record.name)
                }
            }
        }
        let rows = try await conn.query("SELECT id, name FROM copy_table").get().rows.map { try $0.decode((Int, String).self) }
        guard rows.count == 2 else {
            XCTFail("Expected 2 columns, received \(rows.count)")
            return
        }
        XCTAssertEqual(rows[0].0, 1)
        XCTAssertEqual(rows[0].1, "Alice")
        XCTAssertEqual(rows[1].0, 42)
        XCTAssertEqual(rows[1].1, "Bob")
    }
}
