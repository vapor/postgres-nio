import Logging
import PostgresNIO
import XCTest
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

    func testConnectAndClose() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        try conn.close().wait()
    }
    
    func testSimpleQueryVersion() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testQueryVersion() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("SELECT version()", .init()).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
        
    }
    
    func testQuerySelectParameter() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("SELECT $1::TEXT as foo", ["hello"]).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("foo")?.string
        XCTAssertEqual(version, "hello")
    }
    
    func testSQLError() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        
        XCTAssertThrowsError(_ = try conn.simpleQuery("SELECT &").wait()) { error in
            guard let postgresError = try? XCTUnwrap(error as? PostgresError) else { return }
            
            XCTAssertEqual(postgresError.code, .syntaxError)
        }
    }
    
    func testNotificationsEmptyPayload() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        var receivedNotifications: [PostgresMessage.NotificationResponse] = []
        conn.addListener(channel: "example") { context, notification in
            receivedNotifications.append(notification)
        }
        _ = try conn.simpleQuery("LISTEN example").wait()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        // Notifications are asynchronous, so we should run at least one more query to make sure we'll have received the notification response by then
        _ = try conn.simpleQuery("SELECT 1").wait()
        XCTAssertEqual(receivedNotifications.count, 1)
        XCTAssertEqual(receivedNotifications[0].channel, "example")
        XCTAssertEqual(receivedNotifications[0].payload, "")
    }

    func testNotificationsNonEmptyPayload() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        var receivedNotifications: [PostgresMessage.NotificationResponse] = []
        conn.addListener(channel: "example") { context, notification in
            receivedNotifications.append(notification)
        }
        _ = try conn.simpleQuery("LISTEN example").wait()
        _ = try conn.simpleQuery("NOTIFY example, 'Notification payload example'").wait()
        // Notifications are asynchronous, so we should run at least one more query to make sure we'll have received the notification response by then
        _ = try conn.simpleQuery("SELECT 1").wait()
        XCTAssertEqual(receivedNotifications.count, 1)
        XCTAssertEqual(receivedNotifications[0].channel, "example")
        XCTAssertEqual(receivedNotifications[0].payload, "Notification payload example")
    }

    func testNotificationsRemoveHandlerWithinHandler() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        var receivedNotifications = 0
        conn.addListener(channel: "example") { context, notification in
            receivedNotifications += 1
            context.stop()
        }
        _ = try conn.simpleQuery("LISTEN example").wait()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        _ = try conn.simpleQuery("SELECT 1").wait()
        XCTAssertEqual(receivedNotifications, 1)
    }

    func testNotificationsRemoveHandlerOutsideHandler() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        var receivedNotifications = 0
        let context = conn.addListener(channel: "example") { context, notification in
            receivedNotifications += 1
        }
        _ = try conn.simpleQuery("LISTEN example").wait()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        _ = try conn.simpleQuery("SELECT 1").wait()
        context.stop()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        _ = try conn.simpleQuery("SELECT 1").wait()
        XCTAssertEqual(receivedNotifications, 1)
    }

    func testNotificationsMultipleRegisteredHandlers() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        var receivedNotifications1 = 0
        conn.addListener(channel: "example") { context, notification in
            receivedNotifications1 += 1
        }
        var receivedNotifications2 = 0
        conn.addListener(channel: "example") { context, notification in
            receivedNotifications2 += 1
        }
        _ = try conn.simpleQuery("LISTEN example").wait()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        _ = try conn.simpleQuery("SELECT 1").wait()
        XCTAssertEqual(receivedNotifications1, 1)
        XCTAssertEqual(receivedNotifications2, 1)
    }

    func testNotificationsMultipleRegisteredHandlersRemoval() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        var receivedNotifications1 = 0
        conn.addListener(channel: "example") { context, notification in
            receivedNotifications1 += 1
            context.stop()
        }
        var receivedNotifications2 = 0
        conn.addListener(channel: "example") { context, notification in
            receivedNotifications2 += 1
        }
        _ = try conn.simpleQuery("LISTEN example").wait()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        _ = try conn.simpleQuery("NOTIFY example").wait()
        _ = try conn.simpleQuery("SELECT 1").wait()
        XCTAssertEqual(receivedNotifications1, 1)
        XCTAssertEqual(receivedNotifications2, 2)
    }

    func testNotificationHandlerFiltersOnChannel() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        conn.addListener(channel: "desired") { context, notification in
            XCTFail("Received notification on channel that handler was not registered for")
        }
        _ = try conn.simpleQuery("LISTEN undesired").wait()
        _ = try conn.simpleQuery("NOTIFY undesired").wait()
        _ = try conn.simpleQuery("SELECT 1").wait()
    }

    func testSelectTypes() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let results = try conn.simpleQuery("SELECT * FROM pg_type").wait()
        XCTAssert(results.count >= 350, "Results count not large enough: \(results.count)")
    }
    
    func testSelectType() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let results = try conn.simpleQuery("SELECT * FROM pg_type WHERE typname = 'float8'").wait()
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
        switch results.count {
        case 1:
            XCTAssertEqual(results[0].column("typname")?.string, "float8")
            XCTAssertEqual(results[0].column("typnamespace")?.int, 11)
            XCTAssertEqual(results[0].column("typowner")?.int, 10)
            XCTAssertEqual(results[0].column("typlen")?.int, 8)
        default: XCTFail("Should be exactly one result, but got \(results.count)")
        }
    }
    
    func testIntegers() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
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
        let results = try conn.query("""
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
        """).wait()
        switch results.count {
        case 1:
            XCTAssertEqual(results[0].column("smallint")?.int16, 1)
            XCTAssertEqual(results[0].column("smallint_min")?.int16, -32_767)
            XCTAssertEqual(results[0].column("smallint_max")?.int16, 32_767)
            XCTAssertEqual(results[0].column("int")?.int32, 1)
            XCTAssertEqual(results[0].column("int_min")?.int32, -2_147_483_647)
            XCTAssertEqual(results[0].column("int_max")?.int32, 2_147_483_647)
            XCTAssertEqual(results[0].column("bigint")?.int64, 1)
            XCTAssertEqual(results[0].column("bigint_min")?.int64, -9_223_372_036_854_775_807)
            XCTAssertEqual(results[0].column("bigint_max")?.int64, 9_223_372_036_854_775_807)
        default: XCTFail("Should be exactly one result, but got \(results.count)")
        }
    }

    func testPi() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        struct Pi: Decodable {
            let text: String
            let numeric_string: String
            let numeric_decimal: Decimal
            let double: Double
            let float: Float
        }
        let results = try conn.query("""
        SELECT
            pi()::TEXT     as text,
            pi()::NUMERIC  as numeric_string,
            pi()::NUMERIC  as numeric_decimal,
            pi()::FLOAT8   as double,
            pi()::FLOAT4   as float
        """).wait()
        switch results.count {
        case 1:
            //print(results[0])
            XCTAssertEqual(results[0].column("text")?.string?.hasPrefix("3.14159265"), true)
            XCTAssertEqual(results[0].column("numeric_string")?.string?.hasPrefix("3.14159265"), true)
            XCTAssertTrue(results[0].column("numeric_decimal")?.decimal?.isLess(than: 3.14159265358980) ?? false)
            XCTAssertFalse(results[0].column("numeric_decimal")?.decimal?.isLess(than: 3.14159265358978) ?? true)
            XCTAssertTrue(results[0].column("double")?.double?.description.hasPrefix("3.141592") ?? false)
            XCTAssertTrue(results[0].column("float")?.float?.description.hasPrefix("3.141592") ?? false)
        default: XCTFail("Should be exactly one result, but got \(results.count)")
        }
    }
    
    func testUUID() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        struct Model: Decodable {
            let id: UUID
            let string: String
        }
        let results = try conn.query("""
        SELECT
            '123e4567-e89b-12d3-a456-426655440000'::UUID as id,
            '123e4567-e89b-12d3-a456-426655440000'::UUID as string
        """).wait()
        switch results.count {
        case 1:
            //print(results[0])
            XCTAssertEqual(results[0].column("id")?.uuid, UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
            XCTAssertEqual(UUID(uuidString: results[0].column("id")?.string ?? ""), UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
        default: XCTFail("Should be exactly one result, but got \(results.count)")
        }
    }
    
    func testDates() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        struct Dates: Decodable {
            var date: Date
            var timestamp: Date
            var timestamptz: Date
        }
        let results = try conn.query("""
        SELECT
            '2016-01-18 01:02:03 +0042'::DATE         as date,
            '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
            '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
        """).wait()
        switch results.count {
        case 1:
            //print(results[0])
            XCTAssertEqual(results[0].column("date")?.date?.description, "2016-01-18 00:00:00 +0000")
            XCTAssertEqual(results[0].column("timestamp")?.date?.description, "2016-01-18 01:02:03 +0000")
            XCTAssertEqual(results[0].column("timestamptz")?.date?.description, "2016-01-18 00:20:03 +0000")
        default: XCTFail("Should be exactly one result, but got \(results.count)")
        }
    }
    
    /// https://github.com/vapor/nio-postgres/issues/20
    func testBindInteger() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        _ = try conn.simpleQuery("drop table if exists person;").wait()
        _ = try conn.simpleQuery("create table person(id serial primary key, first_name text, last_name text);").wait()
        defer { _ = try! conn.simpleQuery("drop table person;").wait() }
        let id = PostgresData(int32: 5)
        _ = try conn.query("SELECT id, first_name, last_name FROM person WHERE id = $1", [id]).wait()
    }

    // https://github.com/vapor/nio-postgres/issues/21
    func testAverageLengthNumeric() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("select avg(length('foo')) as average_length").wait()
        let length = try XCTUnwrap(rows[0].column("average_length")?.double)
        XCTAssertEqual(length, 3.0)
    }
    
    func testNumericParsing() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
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
        """).wait()
        XCTAssertEqual(rows[0].column("a")?.string, "1234.5678")
        XCTAssertEqual(rows[0].column("b")?.string, "-123.456")
        XCTAssertEqual(rows[0].column("c")?.string, "123456.789123")
        XCTAssertEqual(rows[0].column("d")?.string, "3.14159265358979")
        XCTAssertEqual(rows[0].column("e")?.string, "10000")
        XCTAssertEqual(rows[0].column("f")?.string, "0.00001")
        XCTAssertEqual(rows[0].column("g")?.string, "100000000")
        XCTAssertEqual(rows[0].column("h")?.string, "0.000000001")
        XCTAssertEqual(rows[0].column("k")?.string, "123000000000")
        XCTAssertEqual(rows[0].column("l")?.string, "0.000000000123")
        XCTAssertEqual(rows[0].column("m")?.string, "0.5")
    }

    func testSingleNumericParsing() throws {
        // this seemingly duped test is useful for debugging numeric parsing
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let numeric = "790226039477542363.6032384900176272473"
        let rows = try conn.query("""
        select
            '\(numeric)'::numeric as n
        """).wait()
        XCTAssertEqual(rows[0].column("n")?.string, numeric)
    }

    func testRandomlyGeneratedNumericParsing() throws {
        // this test takes a long time to run
        try XCTSkipUnless(Self.shouldRunLongRunningTests)

        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        for _ in 0..<1_000_000 {
            let integer = UInt.random(in: UInt.min..<UInt.max)
            let fraction = UInt.random(in: UInt.min..<UInt.max)
            let number = "\(integer).\(fraction)"
                .trimmingCharacters(in: CharacterSet(["0"]))
            let rows = try conn.query("""
            select
                '\(number)'::numeric as n
            """).wait()
            XCTAssertEqual(rows[0].column("n")?.string, number)
        }
    }
    
    func testNumericSerialization() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let a = PostgresNumeric(string: "123456.789123")!
        let b = PostgresNumeric(string: "-123456.789123")!
        let c = PostgresNumeric(string: "3.14159265358979")!
        let rows = try conn.query("""
        select
            $1::numeric::text as a,
            $2::numeric::text as b,
            $3::numeric::text as c
        """, [
            .init(numeric: a),
            .init(numeric: b),
            .init(numeric: c)
        ]).wait()
        XCTAssertEqual(rows[0].column("a")?.string, "123456.789123")
        XCTAssertEqual(rows[0].column("b")?.string, "-123456.789123")
        XCTAssertEqual(rows[0].column("c")?.string, "3.14159265358979")
    }
    
    func testMoney() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            '0'::money as a,
            '0.05'::money as b,
            '0.23'::money as c,
            '3.14'::money as d,
            '12345678.90'::money as e
        """).wait()
        XCTAssertEqual(rows[0].column("a")?.string, "0.00")
        XCTAssertEqual(rows[0].column("b")?.string, "0.05")
        XCTAssertEqual(rows[0].column("c")?.string, "0.23")
        XCTAssertEqual(rows[0].column("d")?.string, "3.14")
        XCTAssertEqual(rows[0].column("e")?.string, "12345678.90")
    }

    func testIntegerArrayParse() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            '{1,2,3}'::int[] as array
        """).wait()
        XCTAssertEqual(rows[0].column("array")?.array(of: Int.self), [1, 2, 3])
    }

    func testEmptyIntegerArrayParse() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            '{}'::int[] as array
        """).wait()
        XCTAssertEqual(rows[0].column("array")?.array(of: Int.self), [])
    }

    func testNullIntegerArrayParse() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            null::int[] as array
        """).wait()
        XCTAssertEqual(rows[0].column("array")?.array(of: Int.self), nil)
    }

    func testIntegerArraySerialize() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            $1::int8[] as array
        """, [
            PostgresData(array: [1, 2, 3])
        ]).wait()
        XCTAssertEqual(rows[0].column("array")?.array(of: Int.self), [1, 2, 3])
    }

    func testEmptyIntegerArraySerialize() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            $1::int8[] as array
        """, [
            PostgresData(array: [] as [Int])
        ]).wait()
        XCTAssertEqual(rows[0].column("array")?.array(of: Int.self), [])
    }
    
    func testBoolSerialize() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        do {
            let rows = try conn.query("select $1::bool as bool", [true]).wait()
            XCTAssertEqual(rows[0].column("bool")?.bool, true)
        }
        do {
            let rows = try conn.query("select $1::bool as bool", [false]).wait()
            XCTAssertEqual(rows[0].column("bool")?.bool, false)
        }
        do {
            let rows = try conn.simpleQuery("select true::bool as bool").wait()
            XCTAssertEqual(rows[0].column("bool")?.bool, true)
        }
        do {
            let rows = try conn.simpleQuery("select false::bool as bool").wait()
            XCTAssertEqual(rows[0].column("bool")?.bool, false)
        }
    }

    func testBytesSerialize() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("select $1::bytea as bytes", [
            PostgresData(bytes: [1, 2, 3])
        ]).wait()
        XCTAssertEqual(rows[0].column("bytes")?.bytes, [1, 2, 3])
    }
    
    func testJSONBSerialize() throws {
        struct Object: Codable {
            let foo: Int
            let bar: Int
        }
        
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        do {
            let postgresData = try PostgresData(jsonb: Object(foo: 1, bar: 2))
            let rows = try conn.query("select $1::jsonb as jsonb", [postgresData]).wait()
            
            let object = try rows[0].column("jsonb")?.jsonb(as: Object.self)
            XCTAssertEqual(object?.foo, 1)
            XCTAssertEqual(object?.bar, 2)
        }
        
        do {
            let rows = try conn.query("select jsonb_build_object('foo',1,'bar',2) as jsonb").wait()
            
            let object = try rows[0].column("jsonb")?.jsonb(as: Object.self)
            XCTAssertEqual(object?.foo, 1)
            XCTAssertEqual(object?.bar, 2)
        }
    }
    
    func testJSONBConvertible() throws {
        struct Object: PostgresJSONBCodable {
            let foo: Int
            let bar: Int
        }

        XCTAssertEqual(Object.postgresDataType, .jsonb)

        let postgresData = Object(foo: 1, bar: 2).postgresData
        XCTAssertEqual(postgresData?.type, .jsonb)
        
        let object = Object(postgresData: postgresData!)
        XCTAssertEqual(object?.foo, 1)
        XCTAssertEqual(object?.bar, 2)
    }
    
    func testRemoteTLSServer() throws {
        // postgres://uymgphwj:7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA@elmer.db.elephantsql.com:5432/uymgphwj
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! elg.syncShutdownGracefully() }
        
        let conn = try PostgresConnection.connect(
            to: SocketAddress.makeAddressResolvingHost("elmer.db.elephantsql.com", port: 5432),
            tlsConfiguration: .forClient(certificateVerification: .none),
            serverHostname: "elmer.db.elephantsql.com",
            on: elg.next()
        ).wait()
        try! conn.authenticate(
            username: "uymgphwj",
            database: "uymgphwj",
            password: "7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA"
        ).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testInvalidPassword() throws {
        let conn = try PostgresConnection.testUnauthenticated(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let auth = conn.authenticate(username: "invalid", database: "invalid", password: "bad")
        XCTAssertThrowsError(_ = try auth.wait()) { error in
            guard let postgresError = try? XCTUnwrap(error as? PostgresError) else { return }
            
            XCTAssertEqual(postgresError.code, .invalidPassword)
        }
    }

    func testColumnsInJoin() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let dateInTable1 = Date(timeIntervalSince1970: 1234)
        let dateInTable2 = Date(timeIntervalSince1970: 5678)
        _ = try conn.simpleQuery("DROP TABLE IF EXISTS \"table1\"").wait()
        _ = try conn.simpleQuery("""
        CREATE TABLE table1 (
            "id" int8 NOT NULL,
            "table2_id" int8,
            "intValue" int8,
            "stringValue" text,
            "dateValue" timestamptz,
            PRIMARY KEY ("id")
        );
        """).wait()
        defer { _ = try! conn.simpleQuery("DROP TABLE \"table1\"").wait() }

        _ = try conn.simpleQuery("DROP TABLE IF EXISTS \"table2\"").wait()
        _ = try conn.simpleQuery("""
        CREATE TABLE table2 (
            "id" int8 NOT NULL,
            "intValue" int8,
            "stringValue" text,
            "dateValue" timestamptz,
            PRIMARY KEY ("id")
        );
        """).wait()
        defer { _ = try! conn.simpleQuery("DROP TABLE \"table2\"").wait() }

        _ = try conn.simpleQuery("INSERT INTO table1 VALUES (12, 34, 56, 'stringInTable1', to_timestamp(1234))").wait()
        _ = try conn.simpleQuery("INSERT INTO table2 VALUES (34, 78, 'stringInTable2', to_timestamp(5678))").wait()

        let row = try conn.query("""
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
        """).wait().first!
        XCTAssertEqual(12, row.column("t1_id")?.int)
        XCTAssertEqual(34, row.column("table2_id")?.int)
        XCTAssertEqual(56, row.column("t1_intValue")?.int)
        XCTAssertEqual("stringInTable1", row.column("t1_stringValue")?.string)
        XCTAssertEqual(dateInTable1, row.column("t1_dateValue")?.date)
        XCTAssertEqual(34, row.column("t2_id")?.int)
        XCTAssertEqual(78, row.column("t2_intValue")?.int)
        XCTAssertEqual("stringInTable2", row.column("t2_stringValue")?.string, "stringInTable2")
        XCTAssertEqual(dateInTable2, row.column("t2_dateValue")?.date)
    }

    func testStringArrays() throws {
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

        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query(query, [
            PostgresData(uuid: UUID(uuidString: "D2710E16-EB07-4FD6-A87E-B1BE41C9BD3D")!),
            PostgresData(int: Int(0)),
            PostgresData(date: Date(timeIntervalSince1970: 0)),
            PostgresData(date: Date(timeIntervalSince1970: 0)),
            PostgresData(string: "Foo"),
            PostgresData(array: ["US"]),
            PostgresData(array: ["en"]),
            PostgresData(array: ["USD", "DKK"]),
        ]).wait()
        XCTAssertEqual(rows[0].column("countries")?.array(of: String.self), ["US"])
        XCTAssertEqual(rows[0].column("languages")?.array(of: String.self), ["en"])
        XCTAssertEqual(rows[0].column("currencies")?.array(of: String.self), ["USD", "DKK"])
    }

    func testBindDate() throws {
        // https://github.com/vapor/postgres-nio/issues/53
        let date =  Date(timeIntervalSince1970: 1571425782)
        let query = """
        SELECT $1::json as "date"
        """
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        XCTAssertThrowsError(_ = try conn.query(query, [.init(date: date)]).wait()) { error in
            guard let postgresError = try? XCTUnwrap(error as? PostgresError) else { return }
            guard case let .server(serverError) = postgresError else {
                XCTFail("Expected a .serverError but got \(postgresError)")
                return
            }
            XCTAssertEqual(serverError.fields[.routine], "transformTypeCast")
        }

    }

    func testBindCharString() throws {
        // https://github.com/vapor/postgres-nio/issues/53
        let query = """
        SELECT $1::char as "char"
        """
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query(query, [.init(string: "f")]).wait()
        XCTAssertEqual(rows[0].column("char")?.string, "f")
    }

    func testBindCharUInt8() throws {
        // https://github.com/vapor/postgres-nio/issues/53
        let query = """
        SELECT $1::char as "char"
        """
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query(query, [.init(uint8: 42)]).wait()
        XCTAssertEqual(rows[0].column("char")?.string, "*")
    }
    
    func testDoubleArraySerialization() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let doubles: [Double] = [3.14, 42]
        let rows = try conn.query("""
        select
            $1::double precision[] as doubles
        """, [
            .init(array: doubles)
        ]).wait()
        XCTAssertEqual(rows[0].column("doubles")?.array(of: Double.self), doubles)
    }

    // https://github.com/vapor/postgres-nio/issues/42
    func testUInt8Serialization() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            $1::"char" as int
        """, [
            .init(uint8: 5)
        ]).wait()
        XCTAssertEqual(rows[0].column("int")?.uint8, 5)
    }

    func testMessageDecoder() throws {
        let sample: [UInt8] = [
            0x52, // R - authentication
                0x00, 0x00, 0x00, 0x0C, // length = 12
                0x00, 0x00, 0x00, 0x05, // md5
                0x01, 0x02, 0x03, 0x04, // salt
            0x4B, // B - backend key data
                0x00, 0x00, 0x00, 0x0C, // length = 12
                0x05, 0x05, 0x05, 0x05, // process id
                0x01, 0x01, 0x01, 0x01, // secret key
        ]
        var input = ByteBufferAllocator().buffer(capacity: 0)
        input.writeBytes(sample)

        let output: [PostgresMessage] = [
            PostgresMessage(identifier: .authentication, bytes: [
                0x00, 0x00, 0x00, 0x05,
                0x01, 0x02, 0x03, 0x04,
            ]),
            PostgresMessage(identifier: .backendKeyData, bytes: [
                0x05, 0x05, 0x05, 0x05,
                0x01, 0x01, 0x01, 0x01,
            ])
        ]
        try XCTUnwrap(ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(input, output)],
            decoderFactory: {
                PostgresMessageDecoder()
            }
        ))
    }

    func testPreparedQuery() throws {
         let conn = try PostgresConnection.test(on: eventLoop).wait()

         defer { try! conn.close().wait() }
         let prepared = try conn.prepare(query: "SELECT 1 as one;").wait()
         let rows = try prepared.execute().wait()


         XCTAssertEqual(rows.count, 1)
         let value = rows[0].column("one")
         XCTAssertEqual(value?.int, 1)
     }

    func testPrepareQueryClosure() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()

        defer { try! conn.close().wait() }
        let x = conn.prepare(query: "SELECT $1::text as foo;", handler: { query in
            let a = query.execute(["a"])
            let b = query.execute(["b"])
            let c = query.execute(["c"])
            return EventLoopFuture.whenAllSucceed([a, b, c], on: conn.eventLoop)

        })
        let rows = try x.wait()
        XCTAssertEqual(rows.count, 3)
        XCTAssertEqual(rows[0][0].column("foo")?.string, "a")
        XCTAssertEqual(rows[1][0].column("foo")?.string, "b")
        XCTAssertEqual(rows[2][0].column("foo")?.string, "c")
    }


    // https://github.com/vapor/postgres-nio/issues/71
    func testChar1Serialization() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            '5'::char(1) as one,
            '5'::char(2) as two
        """).wait()
        XCTAssertEqual(rows[0].column("one")?.uint8, 53)
        XCTAssertEqual(rows[0].column("one")?.uint16, 53)
        XCTAssertEqual(rows[0].column("one")?.string, "5")
        XCTAssertEqual(rows[0].column("two")?.uint8, nil)
        XCTAssertEqual(rows[0].column("two")?.uint16, nil)
        XCTAssertEqual(rows[0].column("two")?.string, "5 ")
    }

    func testUserDefinedType() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        _ = try conn.query("DROP TYPE IF EXISTS foo").wait()
        _ = try conn.query("CREATE TYPE foo AS ENUM ('bar', 'qux')").wait()
        defer {
            _ = try! conn.query("DROP TYPE foo").wait()
        }
        let res = try conn.query("SELECT 'qux'::foo as foo").wait()
        XCTAssertEqual(res[0].column("foo")?.string, "qux")
    }

    func testNullBind() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let res = try conn.query("SELECT $1::text as foo", [String?.none.postgresData!]).wait()
        XCTAssertEqual(res[0].column("foo")?.string, nil)
    }

    func testUpdateMetadata() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        _ = try conn.simpleQuery("DROP TABLE IF EXISTS test_table").wait()
        _ = try conn.simpleQuery("CREATE TABLE test_table(pk int PRIMARY KEY)").wait()
        _ = try conn.simpleQuery("INSERT INTO test_table VALUES(1)").wait()
        try conn.query("DELETE FROM test_table", onMetadata: { metadata in
            XCTAssertEqual(metadata.command, "DELETE")
            XCTAssertEqual(metadata.oid, nil)
            XCTAssertEqual(metadata.rows, 1)
        }, onRow: { _ in }).wait()
        let rows = try conn.query("DELETE FROM test_table").wait()
        XCTAssertEqual(rows.metadata.command, "DELETE")
        XCTAssertEqual(rows.metadata.oid, nil)
        XCTAssertEqual(rows.metadata.rows, 0)
    }

    func testTooManyBinds() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let binds = [PostgresData].init(repeating: .null, count: Int(Int16.max) + 1)
        do {
            _ = try conn.query("SELECT version()", binds).wait()
            XCTFail("Should have failed")
        } catch PostgresError.connectionClosed { }
    }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = .debug
        return handler
    }
    return true
}()
