import Logging
@testable import PostgresNIO
import XCTest
import NIOCore
import NIOPosix
import NIOTestUtils

final class PostgresNIOTests: XCTestCase {
    
    private var group: EventLoopGroup!

    private var eventLoop: EventLoop { self.group.next() }
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }
    
    override func tearDownWithError() throws {
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }
    
    // MARK: Tests

    func testConnectAndClose() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        XCTAssertNoThrow(try conn?.close().wait())
    }

    func testSimpleQueryVersion() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try conn?.simpleQuery("SELECT version()").wait())
        XCTAssertEqual(rows?.count, 1)
        XCTAssertEqual(try rows?.first?.decode(String.self, context: .default).contains("PostgreSQL"), true)
    }

    func testQueryVersion() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("SELECT version()", .init()).wait())
        XCTAssertEqual(rows?.count, 1)
        XCTAssertEqual(try rows?.first?.decode(String.self, context: .default).contains("PostgreSQL"), true)
    }

    func testQuerySelectParameter() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("SELECT $1::TEXT as foo", ["hello"]).wait())
        XCTAssertEqual(rows?.count, 1)
        XCTAssertEqual(try rows?.first?.decode(String.self, context: .default), "hello")
    }

    func testSQLError() throws {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        XCTAssertThrowsError(_ = try conn?.simpleQuery("SELECT &").wait()) { error in
            XCTAssertEqual((error as? PostgresError)?.code, .syntaxError)
        }
    }

    func testNotificationsEmptyPayload() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        var receivedNotifications: [PostgresMessage.NotificationResponse] = []
        conn?.addListener(channel: "example") { context, notification in
            receivedNotifications.append(notification)
        }
        XCTAssertNoThrow(_ = try conn?.simpleQuery("LISTEN example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        // Notifications are asynchronous, so we should run at least one more query to make sure we'll have received the notification response by then
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
        XCTAssertEqual(receivedNotifications.count, 1)
        XCTAssertEqual(receivedNotifications.first?.channel, "example")
        XCTAssertEqual(receivedNotifications.first?.payload, "")
    }

    func testNotificationsNonEmptyPayload() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var receivedNotifications: [PostgresMessage.NotificationResponse] = []
        conn?.addListener(channel: "example") { context, notification in
            receivedNotifications.append(notification)
        }
        XCTAssertNoThrow(_ = try conn?.simpleQuery("LISTEN example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example, 'Notification payload example'").wait())
        // Notifications are asynchronous, so we should run at least one more query to make sure we'll have received the notification response by then
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
        XCTAssertEqual(receivedNotifications.count, 1)
        XCTAssertEqual(receivedNotifications.first?.channel, "example")
        XCTAssertEqual(receivedNotifications.first?.payload, "Notification payload example")
    }

    func testNotificationsRemoveHandlerWithinHandler() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var receivedNotifications = 0
        conn?.addListener(channel: "example") { context, notification in
            receivedNotifications += 1
            context.stop()
        }
        XCTAssertNoThrow(_ = try conn?.simpleQuery("LISTEN example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
        XCTAssertEqual(receivedNotifications, 1)
    }

    func testNotificationsRemoveHandlerOutsideHandler() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var receivedNotifications = 0
        let context = conn?.addListener(channel: "example") { context, notification in
            receivedNotifications += 1
        }
        XCTAssertNotNil(context)
        XCTAssertNoThrow(_ = try conn?.simpleQuery("LISTEN example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
        context?.stop()
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
        XCTAssertEqual(receivedNotifications, 1)
    }

    func testNotificationsMultipleRegisteredHandlers() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var receivedNotifications1 = 0
        conn?.addListener(channel: "example") { context, notification in
            receivedNotifications1 += 1
        }
        var receivedNotifications2 = 0
        conn?.addListener(channel: "example") { context, notification in
            receivedNotifications2 += 1
        }
        XCTAssertNoThrow(_ = try conn?.simpleQuery("LISTEN example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
        XCTAssertEqual(receivedNotifications1, 1)
        XCTAssertEqual(receivedNotifications2, 1)
    }

    func testNotificationsMultipleRegisteredHandlersRemoval() throws {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var receivedNotifications1 = 0
        XCTAssertNotNil(conn?.addListener(channel: "example") { context, notification in
            receivedNotifications1 += 1
            context.stop()
        })
        var receivedNotifications2 = 0
        XCTAssertNotNil(conn?.addListener(channel: "example") { context, notification in
            receivedNotifications2 += 1
        })
        XCTAssertNoThrow(_ = try conn?.simpleQuery("LISTEN example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY example").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
        XCTAssertEqual(receivedNotifications1, 1)
        XCTAssertEqual(receivedNotifications2, 2)
    }

    func testNotificationHandlerFiltersOnChannel() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        XCTAssertNotNil(conn?.addListener(channel: "desired") { context, notification in
            XCTFail("Received notification on channel that handler was not registered for")
        })
        XCTAssertNoThrow(_ = try conn?.simpleQuery("LISTEN undesired").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("NOTIFY undesired").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("SELECT 1").wait())
    }

    func testSelectTypes() throws {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var results: [PostgresRow]?
        XCTAssertNoThrow(results = try conn?.simpleQuery("SELECT * FROM pg_type").wait())
        XCTAssert((results?.count ?? 0) > 350, "Results count not large enough")
    }

    func testSelectType() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var results: [PostgresRow]?
        XCTAssertNoThrow(results = try conn?.simpleQuery("SELECT * FROM pg_type WHERE typname = 'float8'").wait())
        // [
        //     "typreceive": "float8recv",
        //     "typelem": "0",
        //     "typarray": "1022",
        //     "typalign": "d",
        //     "typanalyze": "-",
        //     "typtypmod": "-1",
        //     "typname": "float8",
        //     "typnamespace": "11",
        //     "typdefault": "<null>",
        //     "typdefaultbin": "<null>",
        //     "typcollation": "0",
        //     "typispreferred": "t",
        //     "typrelid": "0",
        //     "typbyval": "t",
        //     "typnotnull": "f",
        //     "typinput": "float8in",
        //     "typlen": "8",
        //     "typcategory": "N",
        //     "typowner": "10",
        //     "typtype": "b",
        //     "typdelim": ",",
        //     "typndims": "0",
        //     "typbasetype": "0",
        //     "typacl": "<null>",
        //     "typisdefined": "t",
        //     "typmodout": "-",
        //     "typmodin": "-",
        //     "typsend": "float8send",
        //     "typstorage": "p",
        //     "typoutput": "float8out"
        // ]
        XCTAssertEqual(results?.count, 1)
        let row = results?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "typname"].string, "float8")
        XCTAssertEqual(row?[data: "typnamespace"].int, 11)
        XCTAssertEqual(row?[data: "typowner"].int, 10)
        XCTAssertEqual(row?[data: "typlen"].int, 8)
    }

    func testIntegers() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        struct Integers: Decodable {
            let smallint: Int16
            let smallint_min: Int16
            let smallint_max: Int16
            let int: Int32
            let int_min: Int32
            let int_max: Int32
            let bigint: Int64
            let bigint_min: Int64
            let bigint_max: Int64
        }
        var results: PostgresQueryResult?
        XCTAssertNoThrow(results = try conn?.query("""
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
        """).wait())
        XCTAssertEqual(results?.count, 1)

        let row = results?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "smallint"].int16, 1)
        XCTAssertEqual(row?[data: "smallint_min"].int16, -32_767)
        XCTAssertEqual(row?[data: "smallint_max"].int16, 32_767)
        XCTAssertEqual(row?[data: "int"].int32, 1)
        XCTAssertEqual(row?[data: "int_min"].int32, -2_147_483_647)
        XCTAssertEqual(row?[data: "int_max"].int32, 2_147_483_647)
        XCTAssertEqual(row?[data: "bigint"].int64, 1)
        XCTAssertEqual(row?[data: "bigint_min"].int64, -9_223_372_036_854_775_807)
        XCTAssertEqual(row?[data: "bigint_max"].int64, 9_223_372_036_854_775_807)
    }

    func testPi() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        struct Pi: Decodable {
            let text: String
            let numeric_string: String
            let numeric_decimal: Decimal
            let double: Double
            let float: Float
        }
        var results: PostgresQueryResult?
        XCTAssertNoThrow(results = try conn?.query("""
        SELECT
            pi()::TEXT     as text,
            pi()::NUMERIC  as numeric_string,
            pi()::NUMERIC  as numeric_decimal,
            pi()::FLOAT8   as double,
            pi()::FLOAT4   as float
        """).wait())
        XCTAssertEqual(results?.count, 1)
        let row = results?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "text"].string?.hasPrefix("3.14159265"), true)
        XCTAssertEqual(row?[data: "numeric_string"].string?.hasPrefix("3.14159265"), true)
        XCTAssertTrue(row?[data: "numeric_decimal"].decimal?.isLess(than: 3.14159265358980) ?? false)
        XCTAssertFalse(row?[data: "numeric_decimal"].decimal?.isLess(than: 3.14159265358978) ?? true)
        XCTAssertTrue(row?[data: "double"].double?.description.hasPrefix("3.141592") ?? false)
        XCTAssertTrue(row?[data: "float"].float?.description.hasPrefix("3.141592") ?? false)
    }

    func testUUID() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        struct Model: Decodable {
            let id: UUID
            let string: String
        }
        var results: PostgresQueryResult?
        XCTAssertNoThrow(results = try conn?.query("""
        SELECT
            '123e4567-e89b-12d3-a456-426655440000'::UUID as id,
            '123e4567-e89b-12d3-a456-426655440000'::UUID as string
        """).wait())
        XCTAssertEqual(results?.count, 1)
        let row = results?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "id"].uuid, UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
        XCTAssertEqual(UUID(uuidString: row?[data: "id"].string ?? ""), UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
    }

    func testDates() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        struct Dates: Decodable {
            var date: Date
            var timestamp: Date
            var timestamptz: Date
        }
        var results: PostgresQueryResult?
        XCTAssertNoThrow(results = try conn?.query("""
        SELECT
            '2016-01-18 01:02:03 +0042'::DATE         as date,
            '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
            '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
        """).wait())
        XCTAssertEqual(results?.count, 1)
        let row = results?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "date"].date?.description, "2016-01-18 00:00:00 +0000")
        XCTAssertEqual(row?[data: "timestamp"].date?.description, "2016-01-18 01:02:03 +0000")
        XCTAssertEqual(row?[data: "timestamptz"].date?.description, "2016-01-18 00:20:03 +0000")
    }

    /// https://github.com/vapor/nio-postgres/issues/20
    func testBindInteger() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        XCTAssertNoThrow(_ = try conn?.simpleQuery("drop table if exists person;").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("create table person(id serial primary key, first_name text, last_name text);").wait())
        defer { XCTAssertNoThrow(_ = try conn?.simpleQuery("drop table person;").wait()) }
        let id = PostgresData(int32: 5)
        XCTAssertNoThrow(_ = try conn?.query("SELECT id, first_name, last_name FROM person WHERE id = $1", [id]).wait())
    }

    // https://github.com/vapor/nio-postgres/issues/21
    func testAverageLengthNumeric() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var results: PostgresQueryResult?
        XCTAssertNoThrow(results = try conn?.query("select avg(length('foo')) as average_length").wait())
        let row = results?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: 0].double, 3.0)
    }

    func testNumericParsing() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            '1234.5678'::numeric as a,
            '-123.456'::numeric as b,
            '123456.789123'::numeric as c,
            '3.14159265358979'::numeric as d,
            '10000'::numeric as e,
            '0.00001'::numeric as f,
            '100000000'::numeric as g,
            '0.000000001'::numeric as h,
            '100000000000'::numeric as i,
            '0.000000000001'::numeric as j,
            '123000000000'::numeric as k,
            '0.000000000123'::numeric as l,
            '0.5'::numeric as m
        """).wait())
        XCTAssertEqual(rows?.count, 1)
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "a"].string, "1234.5678")
        XCTAssertEqual(row?[data: "b"].string, "-123.456")
        XCTAssertEqual(row?[data: "c"].string, "123456.789123")
        XCTAssertEqual(row?[data: "d"].string, "3.14159265358979")
        XCTAssertEqual(row?[data: "e"].string, "10000")
        XCTAssertEqual(row?[data: "f"].string, "0.00001")
        XCTAssertEqual(row?[data: "g"].string, "100000000")
        XCTAssertEqual(row?[data: "h"].string, "0.000000001")
        XCTAssertEqual(row?[data: "k"].string, "123000000000")
        XCTAssertEqual(row?[data: "l"].string, "0.000000000123")
        XCTAssertEqual(row?[data: "m"].string, "0.5")
    }

    func testSingleNumericParsing() {
        // this seemingly duped test is useful for debugging numeric parsing
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        let numeric = "790226039477542363.6032384900176272473"
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            '\(numeric)'::numeric as n
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "n"].string, numeric)
    }

    func testRandomlyGeneratedNumericParsing() throws {
        // this test takes a long time to run
        try XCTSkipUnless(Self.shouldRunLongRunningTests)

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        for _ in 0..<1_000_000 {
            let integer = UInt.random(in: UInt.min..<UInt.max)
            let fraction = UInt.random(in: UInt.min..<UInt.max)
            let number = "\(integer).\(fraction)"
                .trimmingCharacters(in: CharacterSet(["0"]))
            var rows: PostgresQueryResult?
            XCTAssertNoThrow(rows = try conn?.query("""
            select
                '\(number)'::numeric as n
            """).wait())
            let row = rows?.first?.makeRandomAccess()
            XCTAssertEqual(row?[data: "n"].string, number)
        }
    }

    func testNumericSerialization() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        let a = PostgresNumeric(string: "123456.789123")!
        let b = PostgresNumeric(string: "-123456.789123")!
        let c = PostgresNumeric(string: "3.14159265358979")!
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            $1::numeric as a,
            $2::numeric as b,
            $3::numeric as c
        """, [
            .init(numeric: a),
            .init(numeric: b),
            .init(numeric: c)
        ]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "a"].decimal, Decimal(string: "123456.789123")!)
        XCTAssertEqual(row?[data: "b"].decimal, Decimal(string: "-123456.789123")!)
        XCTAssertEqual(row?[data: "c"].decimal, Decimal(string: "3.14159265358979")!)
    }
    
    func testDecimalStringSerialization() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        
        XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE IF EXISTS \"table1\"").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("""
        CREATE TABLE table1 (
            "balance" text NOT NULL
        );
        """).wait())
        defer { XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE \"table1\"").wait()) }
        
        XCTAssertNoThrow(_ = try conn?.query("INSERT INTO table1 VALUES ($1)", [.init(decimal: Decimal(string: "123456.789123")!)]).wait())
        
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        SELECT
            "balance"
        FROM table1
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "balance"].decimal, Decimal(string: "123456.789123")!)
    }

    func testMoney() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            '0'::money as a,
            '0.05'::money as b,
            '0.23'::money as c,
            '3.14'::money as d,
            '12345678.90'::money as e
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "a"].string, "0.00")
        XCTAssertEqual(row?[data: "b"].string, "0.05")
        XCTAssertEqual(row?[data: "c"].string, "0.23")
        XCTAssertEqual(row?[data: "d"].string, "3.14")
        XCTAssertEqual(row?[data: "e"].string, "12345678.90")
    }

    func testIntegerArrayParse() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            '{1,2,3}'::int[] as array
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "array"].array(of: Int.self), [1, 2, 3])
    }

    func testEmptyIntegerArrayParse() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            '{}'::int[] as array
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "array"].array(of: Int.self), [])
    }

    func testNullIntegerArrayParse() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            null::int[] as array
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "array"].array(of: Int.self), nil)
    }

    func testIntegerArraySerialize() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            $1::int8[] as array
        """, [
            PostgresData(array: [1, 2, 3])
        ]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "array"].array(of: Int.self), [1, 2, 3])
    }

    func testEmptyIntegerArraySerialize() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            $1::int8[] as array
        """, [
            PostgresData(array: [] as [Int])
        ]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "array"].array(of: Int.self), [])
    }
    
    // https://github.com/vapor/postgres-nio/issues/143
    func testEmptyStringFromNonNullColumn() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        
        XCTAssertNoThrow(_ = try conn?.simpleQuery(#"DROP TABLE IF EXISTS "non_null_empty_strings""#).wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("""
        CREATE TABLE non_null_empty_strings (
            "id" SERIAL,
            "nonNullString" text NOT NULL,
            PRIMARY KEY ("id")
        );
        """).wait())
        defer { XCTAssertNoThrow(_ = try conn?.simpleQuery(#"DROP TABLE "non_null_empty_strings""#).wait()) }
        
        XCTAssertNoThrow(_ = try conn?.simpleQuery("""
        INSERT INTO non_null_empty_strings ("nonNullString") VALUES ('')
        """).wait())
        
        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try conn?.simpleQuery(#"SELECT * FROM "non_null_empty_strings""#).wait())
        XCTAssertEqual(rows?.count, 1)
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "nonNullString"].string, "") // <--- this fails
    }


    func testBoolSerialize() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        do {
            var rows: PostgresQueryResult?
            XCTAssertNoThrow(rows = try conn?.query("select $1::bool as bool", [true]).wait())
            let row = rows?.first?.makeRandomAccess()
            XCTAssertEqual(row?[data: "bool"].bool, true)
        }
        do {
            var rows: PostgresQueryResult?
            XCTAssertNoThrow(rows = try conn?.query("select $1::bool as bool", [false]).wait())
            let row = rows?.first?.makeRandomAccess()
            XCTAssertEqual(row?[data: "bool"].bool, false)
        }
        do {
            var rows: [PostgresRow]?
            XCTAssertNoThrow(rows = try conn?.simpleQuery("select true::bool as bool").wait())
            let row = rows?.first?.makeRandomAccess()
            XCTAssertEqual(row?[data: "bool"].bool, true)
        }
        do {
            var rows: [PostgresRow]?
            XCTAssertNoThrow(rows = try conn?.simpleQuery("select false::bool as bool").wait())
            let row = rows?.first?.makeRandomAccess()
            XCTAssertEqual(row?[data: "bool"].bool, false)
        }
    }

    func testBytesSerialize() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("select $1::bytea as bytes", [
            PostgresData(bytes: [1, 2, 3])
        ]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "bytes"].bytes, [1, 2, 3])
    }

    func testJSONBSerialize() {
        struct Object: Codable, PostgresCodable {
            let foo: Int
            let bar: Int
        }

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        do {
            var postgresData: PostgresData?
            XCTAssertNoThrow(postgresData = try PostgresData(jsonb: Object(foo: 1, bar: 2)))
            var rows: PostgresQueryResult?
            XCTAssertNoThrow(rows = try conn?.query("select $1::jsonb as jsonb", [XCTUnwrap(postgresData)]).wait())

            var object: Object?
            XCTAssertNoThrow(object = try rows?.first?.decode(Object.self, context: .default))
            XCTAssertEqual(object?.foo, 1)
            XCTAssertEqual(object?.bar, 2)
        }

        do {
            var rows: PostgresQueryResult?
            XCTAssertNoThrow(rows = try conn?.query("select jsonb_build_object('foo',1,'bar',2) as jsonb").wait())

            var object: Object?
            XCTAssertNoThrow(object = try rows?.first?.decode(Object.self, context: .default))
            XCTAssertEqual(object?.foo, 1)
            XCTAssertEqual(object?.bar, 2)
        }
    }

    func testRemoteTLSServer() {
        // postgres://uymgphwj:7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA@elmer.db.elephantsql.com:5432/uymgphwj
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.connect(
            to: SocketAddress.makeAddressResolvingHost("elmer.db.elephantsql.com", port: 5432),
            tlsConfiguration: .makeClientConfiguration(),
            serverHostname: "elmer.db.elephantsql.com",
            on: eventLoop
        ).wait())
        XCTAssertNoThrow(try conn?.authenticate(
            username: "uymgphwj",
            database: "uymgphwj",
            password: "7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA"
        ).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try conn?.simpleQuery("SELECT version()").wait())
        XCTAssertEqual(rows?.count, 1)
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "version"].string?.contains("PostgreSQL"), true)
    }

    func testFailingTLSConnectionClosesConnection() {
        // There was a bug (https://github.com/vapor/postgres-nio/issues/133) where we would hit
        // an assert because we didn't close the connection. This test should succeed without hitting
        // the assert

        // postgres://uymgphwj:7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA@elmer.db.elephantsql.com:5432/uymgphwj

        // We should get an error because you can't use an IP address for SNI, but we shouldn't bomb out by
        // hitting the assert
        XCTAssertThrowsError(
            try PostgresConnection.connect(
                to: SocketAddress.makeAddressResolvingHost("elmer.db.elephantsql.com", port: 5432),
                tlsConfiguration: .makeClientConfiguration(),
                serverHostname: "34.228.73.168",
                on: eventLoop
            ).wait()
        )
        // If we hit this, we're all good
        XCTAssertTrue(true)
    }

    func testInvalidPassword() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.testUnauthenticated(on: eventLoop).wait())
        let authFuture = conn?.authenticate(username: "invalid", database: "invalid", password: "bad")
        XCTAssertThrowsError(_ = try authFuture?.wait()) { error in
            XCTAssert((error as? PostgresError)?.code == .invalidPassword || (error as? PostgresError)?.code == .invalidAuthorizationSpecification)
        }
        
        // in this case the connection will be closed by the remote
        XCTAssertNoThrow(try conn?.closeFuture.wait())
    }

    func testColumnsInJoin() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        let dateInTable1 = Date(timeIntervalSince1970: 1234)
        let dateInTable2 = Date(timeIntervalSince1970: 5678)
        XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE IF EXISTS \"table1\"").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("""
        CREATE TABLE table1 (
            "id" int8 NOT NULL,
            "table2_id" int8,
            "intValue" int8,
            "stringValue" text,
            "dateValue" timestamptz,
            PRIMARY KEY ("id")
        );
        """).wait())
        defer { XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE \"table1\"").wait()) }

        XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE IF EXISTS \"table2\"").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("""
        CREATE TABLE table2 (
            "id" int8 NOT NULL,
            "intValue" int8,
            "stringValue" text,
            "dateValue" timestamptz,
            PRIMARY KEY ("id")
        );
        """).wait())
        defer { XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE \"table2\"").wait()) }

        XCTAssertNoThrow(_ = try conn?.simpleQuery("INSERT INTO table1 VALUES (12, 34, 56, 'stringInTable1', to_timestamp(1234))").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("INSERT INTO table2 VALUES (34, 78, 'stringInTable2', to_timestamp(5678))").wait())

        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        SELECT
            "table1"."id" as "t1_id",
            "table1"."intValue" as "t1_intValue",
            "table1"."dateValue" as "t1_dateValue",
            "table1"."stringValue" as "t1_stringValue",
            "table2"."id" as "t2_id",
            "table2"."intValue" as "t2_intValue",
            "table2"."dateValue" as "t2_dateValue",
            "table2"."stringValue" as "t2_stringValue",
            *
        FROM table1 INNER JOIN table2 ON table1.table2_id = table2.id
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "t1_id"].int, 12)
        XCTAssertEqual(row?[data: "table2_id"].int, 34)
        XCTAssertEqual(row?[data: "t1_intValue"].int, 56)
        XCTAssertEqual(row?[data: "t1_stringValue"].string, "stringInTable1")
        XCTAssertEqual(row?[data: "t1_dateValue"].date, dateInTable1)
        XCTAssertEqual(row?[data: "t2_id"].int, 34)
        XCTAssertEqual(row?[data: "t2_intValue"].int, 78)
        XCTAssertEqual(row?[data: "t2_stringValue"].string, "stringInTable2")
        XCTAssertEqual(row?[data: "t2_dateValue"].date, dateInTable2)
    }

    func testStringArrays() {
        let query = """
        SELECT
            $1::uuid as "id",
            $2::bigint as "revision",
            $3::timestamp as "updated_at",
            $4::timestamp as "created_at",
            $5::text as "name",
            $6::text[] as "countries",
            $7::text[] as "languages",
            $8::text[] as "currencies"
        """

        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query(query, [
            PostgresData(uuid: UUID(uuidString: "D2710E16-EB07-4FD6-A87E-B1BE41C9BD3D")!),
            PostgresData(int: Int(0)),
            PostgresData(date: Date(timeIntervalSince1970: 0)),
            PostgresData(date: Date(timeIntervalSince1970: 0)),
            PostgresData(string: "Foo"),
            PostgresData(array: ["US"]),
            PostgresData(array: ["en"]),
            PostgresData(array: ["USD", "DKK"]),
        ]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "countries"].array(of: String.self), ["US"])
        XCTAssertEqual(row?[data: "languages"].array(of: String.self), ["en"])
        XCTAssertEqual(row?[data: "currencies"].array(of: String.self), ["USD", "DKK"])
    }

    func testBindDate() {
        // https://github.com/vapor/postgres-nio/issues/53
        let date =  Date(timeIntervalSince1970: 1571425782)
        let query = """
        SELECT $1::json as "date"
        """
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        XCTAssertThrowsError(_ = try conn?.query(query, [.init(date: date)]).wait()) { error in
            guard let postgresError = try? XCTUnwrap(error as? PostgresError) else { return }
            guard case let .server(serverError) = postgresError else {
                XCTFail("Expected a .serverError but got \(postgresError)")
                return
            }
            XCTAssertEqual(serverError.fields[.routine], "transformTypeCast")
        }

    }

    func testBindCharString() {
        // https://github.com/vapor/postgres-nio/issues/53
        let query = """
        SELECT $1::char as "char"
        """
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query(query, [.init(string: "f")]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "char"].string, "f")
    }

    func testBindCharUInt8() {
        // https://github.com/vapor/postgres-nio/issues/53
        let query = """
        SELECT $1::char as "char"
        """
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query(query, [.init(uint8: 42)]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "char"].string, "*")
    }

    func testDoubleArraySerialization() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        let doubles: [Double] = [3.14, 42]
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            $1::double precision[] as doubles
        """, [
            .init(array: doubles)
        ]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "doubles"].array(of: Double.self), doubles)
    }

    // https://github.com/vapor/postgres-nio/issues/42
    func testUInt8Serialization() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            $1::"char" as int
        """, [
            .init(uint8: 5)
        ]).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "int"].uint8, 5)
    }

    func testPreparedQuery() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var prepared: PreparedQuery?
        XCTAssertNoThrow(prepared = try conn?.prepare(query: "SELECT 1 as one;").wait())
        var rows: [PostgresRow]?
        XCTAssertNoThrow(rows = try prepared?.execute().wait())

        XCTAssertEqual(rows?.count, 1)
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "one"].int, 1)
     }

    func testPrepareQueryClosure() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var queries: [[PostgresRow]]?
        XCTAssertNoThrow(queries = try conn?.prepare(query: "SELECT $1::text as foo;", handler: { query in
            let a = query.execute(["a"])
            let b = query.execute(["b"])
            let c = query.execute(["c"])
            return EventLoopFuture.whenAllSucceed([a, b, c], on: self.eventLoop)
        }).wait())
        XCTAssertEqual(queries?.count, 3)
        var resutIterator = queries?.makeIterator()
        XCTAssertEqual(try resutIterator?.next()?.first?.decode(String.self, context: .default), "a")
        XCTAssertEqual(try resutIterator?.next()?.first?.decode(String.self, context: .default), "b")
        XCTAssertEqual(try resutIterator?.next()?.first?.decode(String.self, context: .default), "c")
    }

    // https://github.com/vapor/postgres-nio/issues/122
    func testPreparedQueryNoResults() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE IF EXISTS \"table_no_results\"").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("""
        CREATE TABLE table_no_results (
            "id" int8 NOT NULL,
            "stringValue" text,
            PRIMARY KEY ("id")
        );
        """).wait())
        defer { XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE \"table_no_results\"").wait()) }

        XCTAssertNoThrow(_ = try conn?.prepare(query: "DELETE FROM \"table_no_results\" WHERE id = $1").wait())
    }


    // https://github.com/vapor/postgres-nio/issues/71
    func testChar1Serialization() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            '5'::char(1) as one,
            '5'::char(2) as two
        """).wait())

        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "one"].uint8, 53)
        XCTAssertEqual(row?[data: "one"].int16, 53)
        XCTAssertEqual(row?[data: "one"].string, "5")
        XCTAssertEqual(row?[data: "two"].uint8, nil)
        XCTAssertEqual(row?[data: "two"].int16, nil)
        XCTAssertEqual(row?[data: "two"].string, "5 ")
    }

    func testUserDefinedType() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        XCTAssertNoThrow(_ = try conn?.query("DROP TYPE IF EXISTS foo").wait())
        XCTAssertNoThrow(_ = try conn?.query("CREATE TYPE foo AS ENUM ('bar', 'qux')").wait())
        defer {
            XCTAssertNoThrow(_ = try conn?.query("DROP TYPE foo").wait())
        }
        var res: PostgresQueryResult?
        XCTAssertNoThrow(res = try conn?.query("SELECT 'qux'::foo as foo").wait())
        let row = res?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "foo"].string, "qux")
    }

    func testNullBind() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        var res: PostgresQueryResult?
        XCTAssertNoThrow(res = try conn?.query("SELECT $1::text as foo", [String?.none.postgresData!]).wait())
        let row = res?.first?.makeRandomAccess()
        XCTAssertNil(row?[data: "foo"].string)
    }

    func testUpdateMetadata() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        XCTAssertNoThrow(_ = try conn?.simpleQuery("DROP TABLE IF EXISTS test_table").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("CREATE TABLE test_table(pk int PRIMARY KEY)").wait())
        XCTAssertNoThrow(_ = try conn?.simpleQuery("INSERT INTO test_table VALUES(1)").wait())
        XCTAssertNoThrow(try conn?.query("DELETE FROM test_table", onMetadata: { metadata in
            XCTAssertEqual(metadata.command, "DELETE")
            XCTAssertEqual(metadata.oid, nil)
            XCTAssertEqual(metadata.rows, 1)
        }, onRow: { _ in }).wait())
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("DELETE FROM test_table").wait())
        XCTAssertEqual(rows?.metadata.command, "DELETE")
        XCTAssertEqual(rows?.metadata.oid, nil)
        XCTAssertEqual(rows?.metadata.rows, 0)
    }

    func testTooManyBinds() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        let binds = [PostgresData].init(repeating: .null, count: Int(Int16.max) + 1)
        XCTAssertThrowsError(try conn?.query("SELECT version()", binds).wait()) { error in
            XCTAssertEqual((error as? PSQLError)?.code, .tooManyParameters)
        }
    }

    func testRemoteClose() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        XCTAssertNoThrow( try conn?.underlying.channel.close().wait() )
    }

    // https://github.com/vapor/postgres-nio/issues/113
    func testVaryingCharArray() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        var res: PostgresQueryResult?
        XCTAssertNoThrow(res = try conn?.query(#"SELECT '{"foo", "bar", "baz"}'::VARCHAR[] as foo"#).wait())
        let row = res?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "foo"].array(of: String.self), ["foo", "bar", "baz"])
    }

    // https://github.com/vapor/postgres-nio/issues/115
    func testSetTimeZone() {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }

        XCTAssertNoThrow(_ = try conn?.simpleQuery("SET TIME ZONE INTERVAL '+5:45' HOUR TO MINUTE").wait())
        XCTAssertNoThrow(_ = try conn?.query("SET TIME ZONE INTERVAL '+5:45' HOUR TO MINUTE").wait())
    }

    func testIntegerConversions() throws {
        var conn: PostgresConnection?
        XCTAssertNoThrow(conn = try PostgresConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow( try conn?.close().wait() ) }
        var rows: PostgresQueryResult?
        XCTAssertNoThrow(rows = try conn?.query("""
        select
            'a'::char as test8,

            '-32768'::smallint as min16,
            '32767'::smallint as max16,

            '-2147483648'::integer as min32,
            '2147483647'::integer as max32,

            '-9223372036854775808'::bigint as min64,
            '9223372036854775807'::bigint as max64
        """).wait())
        let row = rows?.first?.makeRandomAccess()
        XCTAssertEqual(row?[data: "test8"].uint8, 97)
        XCTAssertEqual(row?[data: "test8"].int16, 97)
        XCTAssertEqual(row?[data: "test8"].int32, 97)
        XCTAssertEqual(row?[data: "test8"].int64, 97)

        XCTAssertEqual(row?[data: "min16"].uint8, nil)
        XCTAssertEqual(row?[data: "max16"].uint8, nil)
        XCTAssertEqual(row?[data: "min16"].int16, .min)
        XCTAssertEqual(row?[data: "max16"].int16, .max)
        XCTAssertEqual(row?[data: "min16"].int32, -32768)
        XCTAssertEqual(row?[data: "max16"].int32, 32767)
        XCTAssertEqual(row?[data: "min16"].int64,  -32768)
        XCTAssertEqual(row?[data: "max16"].int64, 32767)

        XCTAssertEqual(row?[data: "min32"].uint8, nil)
        XCTAssertEqual(row?[data: "max32"].uint8, nil)
        XCTAssertEqual(row?[data: "min32"].int16, nil)
        XCTAssertEqual(row?[data: "max32"].int16, nil)
        XCTAssertEqual(row?[data: "min32"].int32, .min)
        XCTAssertEqual(row?[data: "max32"].int32, .max)
        XCTAssertEqual(row?[data: "min32"].int64, -2147483648)
        XCTAssertEqual(row?[data: "max32"].int64, 2147483647)

        XCTAssertEqual(row?[data: "min64"].uint8, nil)
        XCTAssertEqual(row?[data: "max64"].uint8, nil)
        XCTAssertEqual(row?[data: "min64"].int16, nil)
        XCTAssertEqual(row?[data: "max64"].int16, nil)
        XCTAssertEqual(row?[data: "min64"].int32, nil)
        XCTAssertEqual(row?[data: "max64"].int32, nil)
        XCTAssertEqual(row?[data: "min64"].int64, .min)
        XCTAssertEqual(row?[data: "max64"].int64, .max)
    }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()
