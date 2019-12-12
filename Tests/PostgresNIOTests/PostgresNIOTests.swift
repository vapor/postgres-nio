import Logging
import PostgresNIO
import XCTest
import NIOTestUtils
import NIOTransportServices

final class PostgresNIOTests: XCTestCase {
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
        do {
            _ = try conn.simpleQuery("SELECT &").wait()
            XCTFail("An error should have been thrown")
        } catch let error as PostgresError {
            XCTAssertEqual(error.code, .syntaxError)
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
        default: XCTFail("Should be only one result")
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
        default: XCTFail("incorrect result count")
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
            print(results[0])
            XCTAssertEqual(results[0].column("text")?.string?.hasPrefix("3.14159265"), true)
            XCTAssertEqual(results[0].column("numeric_string")?.string?.hasPrefix("3.14159265"), true)
            XCTAssertTrue(results[0].column("numeric_decimal")?.decimal?.isLess(than: 3.14159265358980) ?? false)
            XCTAssertFalse(results[0].column("numeric_decimal")?.decimal?.isLess(than: 3.14159265358978) ?? true)
            XCTAssertTrue(results[0].column("double")?.double?.description.hasPrefix("3.141592") ?? false)
            XCTAssertTrue(results[0].column("float")?.float?.description.hasPrefix("3.141592") ?? false)
        default: XCTFail("incorrect result count")
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
            print(results[0])
            XCTAssertEqual(results[0].column("id")?.uuid, UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
            XCTAssertEqual(UUID(uuidString: results[0].column("id")?.string ?? ""), UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
        default: XCTFail("incorrect result count")
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
            print(results[0])
            XCTAssertEqual(results[0].column("date")?.date?.description, "2016-01-18 00:00:00 +0000")
            XCTAssertEqual(results[0].column("timestamp")?.date?.description, "2016-01-18 01:02:03 +0000")
            XCTAssertEqual(results[0].column("timestamptz")?.date?.description, "2016-01-18 00:20:03 +0000")
        default: XCTFail("incorrect result count")
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
        guard let length = rows[0].column("average_length")?.double else {
            XCTFail("could not decode length")
            return
        }
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
            '3.14159265358979'::numeric as d
        """).wait()
        XCTAssertEqual(rows[0].column("a")?.string, "1234.5678")
        XCTAssertEqual(rows[0].column("b")?.string, "-123.456")
        XCTAssertEqual(rows[0].column("c")?.string, "123456.789123")
        XCTAssertEqual(rows[0].column("d")?.string, "3.14159265358979")
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
        let url = "postgres://uymgphwj:7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA@elmer.db.elephantsql.com:5432/uymgphwj"
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
        do {
            let _ = try auth.wait()
            XCTFail("The authentication should fail")
        } catch let error as PostgresError {
            XCTAssertEqual(error.code, .invalidPassword)
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
        do {
            _ = try conn.query(query, [.init(date: date)]).wait()
            XCTFail("should have failed")
        } catch PostgresError.server(let error) {
            XCTAssertEqual(error.fields[.routine], "transformTypeCast")
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
    
    // MARK: Performance
    
    func testPerformanceRangeSelectDecodePerformance() throws {
        guard performance() else {
            return
        }
        struct Series: Decodable {
            var num: Int
        }
        
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        measure {
            do {
                for _ in 0..<5 {
                    try conn.query("SELECT * FROM generate_series(1, 10000) num") { row in
                        _ = row.column("num")?.int
                    }.wait()
                }
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectTinyModel() throws {
        guard performance() else {
            return
        }
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let now = Date()
        let uuid = UUID()
        try prepareTableToMeasureSelectPerformance(
            rowCount: 300_000, batchSize: 5_000,
            schema:
            """
                "int" int8,
            """,
            fixtureData: [PostgresData(int: 1234)],
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("int")?.int
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectMediumModel() throws {
        guard performance() else {
            return
        }
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let now = Date()
        let uuid = UUID()
        try prepareTableToMeasureSelectPerformance(
            rowCount: 300_000,
            schema:
            // TODO: Also add a `Double` and a `Data` field to this performance test.
            """
                "string" text,
                "int" int8,
                "date" timestamptz,
                "uuid" uuid,
            """,
            fixtureData: [
                PostgresData(string: "foo"),
                PostgresData(int: 0),
                now.postgresData!,
                PostgresData(uuid: uuid)
            ],
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    _ = row.column("string")?.string
                    _ = row.column("int")?.int
                    _ = row.column("date")?.date
                    _ = row.column("uuid")?.uuid
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectLargeModel() throws {
        guard performance() else {
            return
        }
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let now = Date()
        let uuid = UUID()
        try prepareTableToMeasureSelectPerformance(
            rowCount: 100_000,
            schema:
            // TODO: Also add `Double` and `Data` fields to this performance test.
            """
                "string1" text,
                "string2" text,
                "string3" text,
                "string4" text,
                "string5" text,
                "int1" int8,
                "int2" int8,
                "int3" int8,
                "int4" int8,
                "int5" int8,
                "date1" timestamptz,
                "date2" timestamptz,
                "date3" timestamptz,
                "date4" timestamptz,
                "date5" timestamptz,
                "uuid1" uuid,
                "uuid2" uuid,
                "uuid3" uuid,
                "uuid4" uuid,
                "uuid5" uuid,
            """,
            fixtureData: [
                PostgresData(string: "string1"),
                PostgresData(string: "string2"),
                PostgresData(string: "string3"),
                PostgresData(string: "string4"),
                PostgresData(string: "string5"),
                PostgresData(int: 1),
                PostgresData(int: 2),
                PostgresData(int: 3),
                PostgresData(int: 4),
                PostgresData(int: 5),
                now.postgresData!,
                now.postgresData!,
                now.postgresData!,
                now.postgresData!,
                now.postgresData!,
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid)
            ],
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    _ = row.column("string1")?.string
                    _ = row.column("string2")?.string
                    _ = row.column("string3")?.string
                    _ = row.column("string4")?.string
                    _ = row.column("string5")?.string
                    _ = row.column("int1")?.int
                    _ = row.column("int2")?.int
                    _ = row.column("int3")?.int
                    _ = row.column("int4")?.int
                    _ = row.column("int5")?.int
                    _ = row.column("date1")?.date
                    _ = row.column("date2")?.date
                    _ = row.column("date3")?.date
                    _ = row.column("date4")?.date
                    _ = row.column("date5")?.date
                    _ = row.column("uuid1")?.uuid
                    _ = row.column("uuid2")?.uuid
                    _ = row.column("uuid3")?.uuid
                    _ = row.column("uuid4")?.uuid
                    _ = row.column("uuid5")?.uuid
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectLargeModelWithLongFieldNames() throws {
        guard performance() else {
            return
        }
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let fieldIndices = Array(1...20)
        let fieldNames = fieldIndices.map { "veryLongFieldNameVeryLongFieldName\($0)" }
        try prepareTableToMeasureSelectPerformance(
            rowCount: 50_000, batchSize: 200,
            schema: fieldNames.map { "\"\($0)\" int8" }.joined(separator: ", ") + ",",
            fixtureData: fieldIndices.map { PostgresData(int: $0) },
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    for fieldName in fieldNames {
                        _ = row.column(fieldName)?.int
                    }
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectHugeModel() throws {
        guard performance() else {
            return
        }
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let fieldIndices = Array(1...100)
        let fieldNames = fieldIndices.map { "int\($0)" }
        try prepareTableToMeasureSelectPerformance(
            rowCount: 10_000, batchSize: 200,
            schema: fieldNames.map { "\"\($0)\" int8" }.joined(separator: ", ") + ",",
            fixtureData: fieldIndices.map { PostgresData(int: $0) },
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    for fieldName in fieldNames {
                        _ = row.column(fieldName)?.int
                    }
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
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
            $1::char as int
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
        do {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(input, output)],
                decoderFactory: {
                    PostgresMessageDecoder()
                }
            )
        } catch {
            XCTFail("\(error)")
        }
    }

    #if canImport(Network)
    func testNIOTS() throws {
        let elg = NIOTSEventLoopGroup()
        defer { try! elg.syncShutdownGracefully() }
        let conn = try PostgresConnection.test(on: elg.next()).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    #endif

    private var group: EventLoopGroup!
    private var eventLoop: EventLoop {
        return self.group.next()
    }

    override func setUp() {
        testLogLevel = .info
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    override func tearDown() {
        XCTAssertNoThrow(try self.group.syncShutdownGracefully())
        self.group = nil
    }
}

private func performance(function: String = #function) -> Bool {
    if _isDebugAssertConfiguration() {
        print("Debug build, skipping \(function)")
        return false
    } else {
        print("Running performance test \(function)")
        return true
    }
}

private func prepareTableToMeasureSelectPerformance(
    rowCount: Int,
    batchSize: Int = 1_000,
    schema: String,
    fixtureData: [PostgresData],
    on eventLoop: EventLoop,
    file: StaticString = #file,
    line: UInt = #line
) throws {
    XCTAssertEqual(rowCount % batchSize, 0, "`rowCount` must be a multiple of `batchSize`", file: file, line: line)
    let conn = try PostgresConnection.test(on: eventLoop).wait()
    defer { try! conn.close().wait() }
    
    _ = try conn.simpleQuery("DROP TABLE IF EXISTS \"measureSelectPerformance\"").wait()
    _ = try conn.simpleQuery("""
        CREATE TABLE "measureSelectPerformance" (
        "id" int8 NOT NULL,
        \(schema)
        PRIMARY KEY ("id")
        );
        """).wait()
    
    // Batch `batchSize` inserts into one for better insert performance.
    let totalArgumentsPerRow = fixtureData.count + 1
    let insertArgumentsPlaceholder = (0..<batchSize).map { indexInBatch in
        "("
            + (0..<totalArgumentsPerRow).map { argumentIndex in "$\(indexInBatch * totalArgumentsPerRow + argumentIndex + 1)" }
                .joined(separator: ", ")
            + ")"
        }.joined(separator: ", ")
    let insertQuery = "INSERT INTO \"measureSelectPerformance\" VALUES \(insertArgumentsPlaceholder)"
    var batchedFixtureData = Array(repeating: [PostgresData(int: 0)] + fixtureData, count: batchSize).flatMap { $0 }
    for batchIndex in 0..<(rowCount / batchSize) {
        for indexInBatch in 0..<batchSize {
            let rowIndex = batchIndex * batchSize + indexInBatch
            batchedFixtureData[indexInBatch * totalArgumentsPerRow] = PostgresData(int: rowIndex)
        }
        _ = try conn.query(insertQuery, batchedFixtureData).wait()
    }
}
