import Foundation
import Logging
import NIOCore
import NIOPosix
import NIOSSL
import NIOTestUtils
import Synchronization
import Testing
@testable import PostgresNIO

@Suite(.serialized)
struct PostgresNIOTests {
    init() {
        #expect(isLoggingConfigured)
    }

    // MARK: Tests

    @Test func connectAndClose() async throws {
        let conn = try await PostgresConnection.test()
        try await conn.close()
    }

    @Test(.enabled(if: env("POSTGRES_SOCKET") != nil))
    func connectUDSAndClose() async throws {
        let conn = try await PostgresConnection.testUDS()
        try await conn.close()
    }

    @Test func connectEstablishedChannelAndClose() async throws {
        let eventLoop = MultiThreadedEventLoopGroup.singleton.any()
        let channel = try await ClientBootstrap(group: eventLoop).connect(to: PostgresConnection.address()).get()
        let conn = try await PostgresConnection.testChannel(channel, on: eventLoop)
        try await conn.close()
    }

    @Test func simpleQueryVersion() async throws {
        try await withConnection { conn in
            let rows = try await conn.simpleQuery("SELECT version()").get()
            #expect(rows.count == 1)
            #expect(try rows.first?.decode(String.self, context: .default).contains("PostgreSQL") == true)
        }
    }

    @Test(.enabled(if: env("POSTGRES_SOCKET") != nil))
    func simpleQueryVersionUsingUDS() async throws {
        let conn = try await PostgresConnection.testUDS()
        do {
            let rows = try await conn.simpleQuery("SELECT version()").get()
            #expect(rows.count == 1)
            #expect(try rows.first?.decode(String.self, context: .default).contains("PostgreSQL") == true)
        } catch {
            try? await conn.close()
            throw error
        }
        try await conn.close()
    }

    @Test func simpleQueryVersionUsingEstablishedChannel() async throws {
        let eventLoop = MultiThreadedEventLoopGroup.singleton.any()
        let channel = try await ClientBootstrap(group: eventLoop).connect(to: PostgresConnection.address()).get()
        let conn = try await PostgresConnection.testChannel(channel, on: eventLoop)
        do {
            let rows = try await conn.simpleQuery("SELECT version()").get()
            #expect(rows.count == 1)
            #expect(try rows.first?.decode(String.self, context: .default).contains("PostgreSQL") == true)
        } catch {
            try? await conn.close()
            throw error
        }
        try await conn.close()
    }

    @Test func queryVersion() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("SELECT version()", .init()).get()
            #expect(rows.count == 1)
            #expect(try rows.first?.decode(String.self, context: .default).contains("PostgreSQL") == true)
        }
    }

    @Test func querySelectParameter() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("SELECT $1::TEXT as foo", ["hello"]).get()
            #expect(rows.count == 1)
            #expect(try rows.first?.decode(String.self, context: .default) == "hello")
        }
    }

    @Test func sqlError() async throws {
        try await withConnection { conn in
            let error = await #expect(throws: PostgresError.self) {
                _ = try await conn.simpleQuery("SELECT &").get()
            }
            #expect(error?.code == .syntaxError)
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func notificationsEmptyPayload() async throws {
        try await withConnection { conn in
            let received = Mutex<[(channel: String, payload: String)]>([])
            conn.addListener(channel: "example") { context, notification in
                received.withLock { $0.append((notification.channel, notification.payload)) }
            }
            _ = try await conn.simpleQuery("LISTEN example").get()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            // Notifications are asynchronous, so we should run at least one more query to make sure we'll have received the notification response by then
            _ = try await conn.simpleQuery("SELECT 1").get()

            let notifications = received.withLock { $0 }
            #expect(notifications.count == 1)
            #expect(notifications.first?.channel == "example")
            #expect(notifications.first?.payload == "")
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func notificationsNonEmptyPayload() async throws {
        try await withConnection { conn in
            let received = Mutex<[(channel: String, payload: String)]>([])
            conn.addListener(channel: "example") { context, notification in
                received.withLock { $0.append((notification.channel, notification.payload)) }
            }
            _ = try await conn.simpleQuery("LISTEN example").get()
            _ = try await conn.simpleQuery("NOTIFY example, 'Notification payload example'").get()
            // Notifications are asynchronous, so we should run at least one more query to make sure we'll have received the notification response by then
            _ = try await conn.simpleQuery("SELECT 1").get()

            let notifications = received.withLock { $0 }
            #expect(notifications.count == 1)
            #expect(notifications.first?.channel == "example")
            #expect(notifications.first?.payload == "Notification payload example")
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func notificationsRemoveHandlerWithinHandler() async throws {
        try await withConnection { conn in
            let receivedNotifications = Mutex(0)
            conn.addListener(channel: "example") { context, notification in
                receivedNotifications.withLock { $0 += 1 }
                context.stop()
            }
            _ = try await conn.simpleQuery("LISTEN example").get()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            _ = try await conn.simpleQuery("SELECT 1").get()
            #expect(receivedNotifications.withLock { $0 } == 1)
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func notificationsRemoveHandlerOutsideHandler() async throws {
        try await withConnection { conn in
            let receivedNotifications = Mutex(0)
            let context = conn.addListener(channel: "example") { context, notification in
                receivedNotifications.withLock { $0 += 1 }
            }
            _ = try await conn.simpleQuery("LISTEN example").get()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            _ = try await conn.simpleQuery("SELECT 1").get()
            context.stop()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            _ = try await conn.simpleQuery("SELECT 1").get()
            #expect(receivedNotifications.withLock { $0 } == 1)
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func notificationsMultipleRegisteredHandlers() async throws {
        try await withConnection { conn in
            let receivedNotifications1 = Mutex(0)
            conn.addListener(channel: "example") { context, notification in
                receivedNotifications1.withLock { $0 += 1 }
            }
            let receivedNotifications2 = Mutex(0)
            conn.addListener(channel: "example") { context, notification in
                receivedNotifications2.withLock { $0 += 1 }
            }
            _ = try await conn.simpleQuery("LISTEN example").get()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            _ = try await conn.simpleQuery("SELECT 1").get()
            #expect(receivedNotifications1.withLock { $0 } == 1)
            #expect(receivedNotifications2.withLock { $0 } == 1)
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func notificationsMultipleRegisteredHandlersRemoval() async throws {
        try await withConnection { conn in
            let receivedNotifications1 = Mutex(0)
            conn.addListener(channel: "example") { context, notification in
                receivedNotifications1.withLock { $0 += 1 }
                context.stop()
            }
            let receivedNotifications2 = Mutex(0)
            conn.addListener(channel: "example") { context, notification in
                receivedNotifications2.withLock { $0 += 1 }
            }
            _ = try await conn.simpleQuery("LISTEN example").get()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            _ = try await conn.simpleQuery("NOTIFY example").get()
            _ = try await conn.simpleQuery("SELECT 1").get()
            #expect(receivedNotifications1.withLock { $0 } == 1)
            #expect(receivedNotifications2.withLock { $0 } == 2)
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func notificationHandlerFiltersOnChannel() async throws {
        try await withConnection { conn in
            let receivedNotifications = Mutex(0)
            conn.addListener(channel: "desired") { context, notification in
                receivedNotifications.withLock { $0 += 1 }
            }
            _ = try await conn.simpleQuery("LISTEN undesired").get()
            _ = try await conn.simpleQuery("NOTIFY undesired").get()
            _ = try await conn.simpleQuery("SELECT 1").get()
            #expect(receivedNotifications.withLock { $0 } == 0, "Received notification on channel that handler was not registered for")
        }
    }

    @Test func selectTypes() async throws {
        try await withConnection { conn in
            let results = try await conn.simpleQuery("SELECT * FROM pg_type").get()
            #expect(results.count > 350, "Results count not large enough")
        }
    }

    @Test func selectType() async throws {
        try await withConnection { conn in
            let results = try await conn.simpleQuery("SELECT * FROM pg_type WHERE typname = 'float8'").get()
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
            #expect(results.count == 1)
            let row = try #require(results.first).makeRandomAccess()
            #expect(row[data: "typname"].string == "float8")
            #expect(row[data: "typnamespace"].int == 11)
            #expect(row[data: "typowner"].int == 10)
            #expect(row[data: "typlen"].int == 8)
        }
    }

    @Test func integers() async throws {
        try await withConnection { conn in
            let results = try await conn.query("""
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
            """).get()
            #expect(results.count == 1)

            let row = try #require(results.first).makeRandomAccess()
            #expect(row[data: "smallint"].int16 == 1)
            #expect(row[data: "smallint_min"].int16 == -32_767)
            #expect(row[data: "smallint_max"].int16 == 32_767)
            #expect(row[data: "int"].int32 == 1)
            #expect(row[data: "int_min"].int32 == -2_147_483_647)
            #expect(row[data: "int_max"].int32 == 2_147_483_647)
            #expect(row[data: "bigint"].int64 == 1)
            #expect(row[data: "bigint_min"].int64 == -9_223_372_036_854_775_807)
            #expect(row[data: "bigint_max"].int64 == 9_223_372_036_854_775_807)
        }
    }

    @Test func pi() async throws {
        try await withConnection { conn in
            let results = try await conn.query("""
            SELECT
                pi()::TEXT     as text,
                pi()::NUMERIC  as numeric_string,
                pi()::NUMERIC  as numeric_decimal,
                pi()::FLOAT8   as double,
                pi()::FLOAT4   as float
            """).get()
            #expect(results.count == 1)
            let row = try #require(results.first).makeRandomAccess()
            #expect(row[data: "text"].string?.hasPrefix("3.14159265") == true)
            #expect(row[data: "numeric_string"].string?.hasPrefix("3.14159265") == true)
            #expect(row[data: "numeric_decimal"].decimal?.isLess(than: 3.14159265358980) == true)
            #expect(row[data: "numeric_decimal"].decimal?.isLess(than: 3.14159265358978) == false)
            #expect(row[data: "double"].double?.description.hasPrefix("3.141592") == true)
            #expect(row[data: "float"].float?.description.hasPrefix("3.141592") == true)
        }
    }

    @Test func uuid() async throws {
        try await withConnection { conn in
            let results = try await conn.query("""
            SELECT
                '123e4567-e89b-12d3-a456-426655440000'::UUID as id,
                '123e4567-e89b-12d3-a456-426655440000'::UUID as string
            """).get()
            #expect(results.count == 1)
            let row = try #require(results.first).makeRandomAccess()
            #expect(row[data: "id"].uuid == UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
            #expect(UUID(uuidString: row[data: "id"].string ?? "") == UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
        }
    }

    @Test func int4Range() async throws {
        try await withConnection { conn in
            let results1 = try await conn.query("""
            SELECT
                '[\(Int32.min), \(Int32.max))'::int4range AS range
            """).get()
            #expect(results1.count == 1)
            let row1 = try #require(results1.first).makeRandomAccess()
            let expectedRange: Range<Int32> = Int32.min..<Int32.max
            let decodedRange = try row1.decode(column: "range", as: Range<Int32>.self, context: .default)
            #expect(decodedRange == expectedRange)

            let results2 = try await conn.query("""
            SELECT
                ARRAY[
                    '[0, 1)'::int4range,
                    '[10, 11)'::int4range
                ] AS ranges
            """).get()
            #expect(results2.count == 1)
            let row2 = try #require(results2.first).makeRandomAccess()
            let decodedRangeArray = try row2.decode(column: "ranges", as: [Range<Int32>].self, context: .default)
            let decodedClosedRangeArray = try row2.decode(column: "ranges", as: [ClosedRange<Int32>].self, context: .default)
            #expect(decodedRangeArray == [0..<1, 10..<11])
            #expect(decodedClosedRangeArray == [0...0, 10...10])
        }
    }

    @Test func emptyInt4Range() async throws {
        try await withConnection { conn in
            let randomValue = Int32.random(in: Int32.min...Int32.max)
            let results = try await conn.query("""
            SELECT
                '[\(randomValue),\(randomValue))'::int4range AS range
            """).get()
            #expect(results.count == 1)
            let row = try #require(results.first).makeRandomAccess()
            let expectedRange: Range<Int32> = Int32.valueForEmptyRange..<Int32.valueForEmptyRange
            let decodedRange = try row.decode(column: "range", as: Range<Int32>.self, context: .default)
            #expect(decodedRange == expectedRange)

            #expect(throws: (any Error).self) {
                try row.decode(column: "range", as: ClosedRange<Int32>.self, context: .default)
            }
        }
    }

    @Test func int8Range() async throws {
        try await withConnection { conn in
            let results1 = try await conn.query("""
            SELECT
                '[\(Int64.min), \(Int64.max))'::int8range AS range
            """).get()
            #expect(results1.count == 1)
            let row1 = try #require(results1.first).makeRandomAccess()
            let expectedRange: Range<Int64> = Int64.min..<Int64.max
            let decodedRange = try row1.decode(column: "range", as: Range<Int64>.self, context: .default)
            #expect(decodedRange == expectedRange)

            let results2 = try await conn.query("""
            SELECT
                ARRAY[
                    '[0, 1)'::int8range,
                    '[10, 11)'::int8range
                ] AS ranges
            """).get()
            #expect(results2.count == 1)
            let row2 = try #require(results2.first).makeRandomAccess()
            let decodedRangeArray = try row2.decode(column: "ranges", as: [Range<Int64>].self, context: .default)
            let decodedClosedRangeArray = try row2.decode(column: "ranges", as: [ClosedRange<Int64>].self, context: .default)
            #expect(decodedRangeArray == [0..<1, 10..<11])
            #expect(decodedClosedRangeArray == [0...0, 10...10])
        }
    }

    @Test func emptyInt8Range() async throws {
        try await withConnection { conn in
            let randomValue = Int64.random(in: Int64.min...Int64.max)
            let results = try await conn.query("""
            SELECT
                '[\(randomValue),\(randomValue))'::int8range AS range
            """).get()
            #expect(results.count == 1)
            let row = try #require(results.first).makeRandomAccess()
            let expectedRange: Range<Int64> = Int64.valueForEmptyRange..<Int64.valueForEmptyRange
            let decodedRange = try row.decode(column: "range", as: Range<Int64>.self, context: .default)
            #expect(decodedRange == expectedRange)

            #expect(throws: (any Error).self) {
                try row.decode(column: "range", as: ClosedRange<Int64>.self, context: .default)
            }
        }
    }

    @Test func dates() async throws {
        try await withConnection { conn in
            let results = try await conn.query("""
            SELECT
                '2016-01-18 01:02:03 +0042'::DATE         as date,
                '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
                '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
            """).get()
            #expect(results.count == 1)
            let row = try #require(results.first).makeRandomAccess()
            #expect(row[data: "date"].date?.description == "2016-01-18 00:00:00 +0000")
            #expect(row[data: "timestamp"].date?.description == "2016-01-18 01:02:03 +0000")
            #expect(row[data: "timestamptz"].date?.description == "2016-01-18 00:20:03 +0000")
        }
    }

    /// https://github.com/vapor/nio-postgres/issues/20
    @Test func bindInteger() async throws {
        try await withConnection { conn in
            _ = try await conn.simpleQuery("drop table if exists person;").get()
            _ = try await conn.simpleQuery("create table person(id serial primary key, first_name text, last_name text);").get()
            let id = PostgresData(int32: 5)
            _ = try await conn.query("SELECT id, first_name, last_name FROM person WHERE id = $1", [id]).get()
            _ = try await conn.simpleQuery("drop table person;").get()
        }
    }

    // https://github.com/vapor/nio-postgres/issues/21
    @Test func averageLengthNumeric() async throws {
        try await withConnection { conn in
            let results = try await conn.query("select avg(length('foo')) as average_length").get()
            let row = try #require(results.first).makeRandomAccess()
            #expect(row[data: 0].double == 3.0)
        }
    }

    @Test func numericParsing() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
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
            """).get()
            #expect(rows.count == 1)
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "a"].string == "1234.5678")
            #expect(row[data: "b"].string == "-123.456")
            #expect(row[data: "c"].string == "123456.789123")
            #expect(row[data: "d"].string == "3.14159265358979")
            #expect(row[data: "e"].string == "10000")
            #expect(row[data: "f"].string == "0.00001")
            #expect(row[data: "g"].string == "100000000")
            #expect(row[data: "h"].string == "0.000000001")
            #expect(row[data: "k"].string == "123000000000")
            #expect(row[data: "l"].string == "0.000000000123")
            #expect(row[data: "m"].string == "0.5")
        }
    }

    @Test func singleNumericParsing() async throws {
        // this seemingly duped test is useful for debugging numeric parsing
        try await withConnection { conn in
            let numeric = "790226039477542363.6032384900176272473"
            let rows = try await conn.query("""
            select
                '\(numeric)'::numeric as n
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "n"].string == numeric)
        }
    }

    // this test takes a long time to run
    @Test(.enabled(if: shouldRunLongRunningTests))
    func randomlyGeneratedNumericParsing() async throws {
        try await withConnection { conn in
            for _ in 0..<1_000_000 {
                let integer = UInt.random(in: UInt.min..<UInt.max)
                let fraction = UInt.random(in: UInt.min..<UInt.max)
                let number = "\(integer).\(fraction)"
                    .trimmingCharacters(in: CharacterSet(["0"]))
                let rows = try await conn.query("""
                select
                    '\(number)'::numeric as n
                """).get()
                let row = try #require(rows.first).makeRandomAccess()
                #expect(row[data: "n"].string == number)
            }
        }
    }

    @Test func numericSerialization() async throws {
        try await withConnection { conn in
            let a = PostgresNumeric(string: "123456.789123")!
            let b = PostgresNumeric(string: "-123456.789123")!
            let c = PostgresNumeric(string: "3.14159265358979")!
            let d = PostgresNumeric(string: "1234567898765")!
            let e = PostgresNumeric(string: "0.00000080216390553684")!
            let f = PostgresNumeric(string: "0.0000080216390553684")!
            let g = PostgresNumeric(string: "0.000080216390553684")!
            let h = PostgresNumeric(string: "802163905536840000.0")!
            let i = PostgresNumeric(string: "8021639055368400000.0")!
            let j = PostgresNumeric(string: "80216390553684000000.0")!
            let k = PostgresNumeric(string: "802163905536840000.000080216390553684")!
            let rows = try await conn.query("""
            select
                $1::numeric as a,
                $2::numeric as b,
                $3::numeric as c,
                $4::numeric as d,
                $5::numeric as e,
                $6::numeric as f,
                $7::numeric as g,
                $8::numeric as h,
                $9::numeric as i,
                $10::numeric as j,
                $11::numeric as k
            """, [
                .init(numeric: a),
                .init(numeric: b),
                .init(numeric: c),
                .init(numeric: d),
                .init(numeric: e),
                .init(numeric: f),
                .init(numeric: g),
                .init(numeric: h),
                .init(numeric: i),
                .init(numeric: j),
                .init(numeric: k)
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "a"].decimal == Decimal(string: "123456.789123")!)
            #expect(row[data: "b"].decimal == Decimal(string: "-123456.789123")!)
            #expect(row[data: "c"].decimal == Decimal(string: "3.14159265358979")!)
            #expect(row[data: "d"].decimal == Decimal(string: "1234567898765")!)
            #expect(row[data: "e"].decimal == Decimal(string: "0.00000080216390553684")!)
            #expect(row[data: "f"].decimal == Decimal(string: "0.0000080216390553684")!)
            #expect(row[data: "g"].decimal == Decimal(string: "0.000080216390553684")!)
            #expect(row[data: "h"].decimal == Decimal(string: "802163905536840000.0")!)
            #expect(row[data: "i"].decimal == Decimal(string: "8021639055368400000.0")!)
            #expect(row[data: "j"].decimal == Decimal(string: "80216390553684000000.0")!)
            #expect(row[data: "k"].decimal == Decimal(string: "802163905536840000.000080216390553684")!)
        }
    }

    @Test func decimalStringSerialization() async throws {
        try await withConnection { conn in
            _ = try await conn.simpleQuery("DROP TABLE IF EXISTS \"table1\"").get()
            _ = try await conn.simpleQuery("""
            CREATE TABLE table1 (
                "balance" text NOT NULL
            );
            """).get()

            _ = try await conn.query("INSERT INTO table1 VALUES ($1)", [.init(decimal: Decimal(string: "123456.789123")!)]).get()

            let rows = try await conn.query("""
            SELECT
                "balance"
            FROM table1
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "balance"].decimal == Decimal(string: "123456.789123")!)

            _ = try await conn.simpleQuery("DROP TABLE \"table1\"").get()
        }
    }

    @Test func money() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                '0'::money as a,
                '0.05'::money as b,
                '0.23'::money as c,
                '3.14'::money as d,
                '12345678.90'::money as e
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "a"].string == "0.00")
            #expect(row[data: "b"].string == "0.05")
            #expect(row[data: "c"].string == "0.23")
            #expect(row[data: "d"].string == "3.14")
            #expect(row[data: "e"].string == "12345678.90")
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func integerArrayParse() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                '{1,2,3}'::int[] as array
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Int.self) == [1, 2, 3])
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func emptyIntegerArrayParse() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                '{}'::int[] as array
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Int.self) == [])
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func optionalIntegerArrayParse() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                '{1, 2, NULL, 4}'::int8[] as array
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Int?.self) == [1, 2, nil, 4])
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func nullIntegerArrayParse() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                null::int[] as array
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Int.self) == nil)
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func integerArraySerialize() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                $1::int8[] as array
            """, [
                PostgresData(array: [1, 2, 3])
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Int.self) == [1, 2, 3])
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func emptyIntegerArraySerialize() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                $1::int8[] as array
            """, [
                PostgresData(array: [] as [Int])
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Int.self) == [])
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func optionalIntegerArraySerialize() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                $1::int8[] as array
            """, [
                PostgresData(array: [1, nil, 3] as [Int64?])
            ]).get()
            #expect(rows.count == 1)
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Int64?.self) == [1, nil, 3])
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func dateArraySerialize() async throws {
        try await withConnection { conn in
            let date1 = Date(timeIntervalSince1970: 1704088800),
                date2 = Date(timeIntervalSince1970: 1706767200),
                date3 = Date(timeIntervalSince1970: 1709272800)
            let rows = try await conn.query("""
            select
                $1::timestamptz[] as array
            """, [
                PostgresData(array: [date1, date2, date3])
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "array"].array(of: Date.self) == [date1, date2, date3])
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func dateArraySerializeAsPostgresDate() async throws {
        try await withConnection { conn in
            let date1 = Date(timeIntervalSince1970: 1704088800),//8766
                date2 = Date(timeIntervalSince1970: 1706767200),//8797
                date3 = Date(timeIntervalSince1970: 1709272800) //8826
            var data = PostgresData(array: [date1, date2, date3].map { Int32(($0.timeIntervalSince1970 - 946_684_800) / 86_400).postgresData }, elementType: .date)
            data.type = .dateArray // N.B.: `.date` format is an Int32 count of days since psqlStartDate
            let rows = try await conn.query("select $1::date[] as array", [data]).get()
            let row = try #require(rows.first).makeRandomAccess()
            func daysSincePsqlStart(_ date: Date) -> Int32 {
                Int32(((date.timeIntervalSince1970 - 946_684_800) / 86_400).rounded(.toNearestOrAwayFromZero))
            }
            let decoded = row[data: "array"].array(of: Date.self)?.map(daysSincePsqlStart)
            #expect(decoded == [date1, date2, date3].map(daysSincePsqlStart))
        }
    }

    // https://github.com/vapor/postgres-nio/issues/143
    @Test func emptyStringFromNonNullColumn() async throws {
        try await withConnection { conn in
            _ = try await conn.simpleQuery(#"DROP TABLE IF EXISTS "non_null_empty_strings""#).get()
            _ = try await conn.simpleQuery("""
            CREATE TABLE non_null_empty_strings (
                "id" SERIAL,
                "nonNullString" text NOT NULL,
                PRIMARY KEY ("id")
            );
            """).get()

            _ = try await conn.simpleQuery("""
            INSERT INTO non_null_empty_strings ("nonNullString") VALUES ('')
            """).get()

            let rows = try await conn.simpleQuery(#"SELECT * FROM "non_null_empty_strings""#).get()
            #expect(rows.count == 1)
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "nonNullString"].string == "") // <--- this fails

            _ = try await conn.simpleQuery(#"DROP TABLE "non_null_empty_strings""#).get()
        }
    }


    @Test func boolSerialize() async throws {
        try await withConnection { conn in
            do {
                let rows = try await conn.query("select $1::bool as bool", [true]).get()
                let row = try #require(rows.first).makeRandomAccess()
                #expect(row[data: "bool"].bool == true)
            }
            do {
                let rows = try await conn.query("select $1::bool as bool", [false]).get()
                let row = try #require(rows.first).makeRandomAccess()
                #expect(row[data: "bool"].bool == false)
            }
            do {
                let rows = try await conn.simpleQuery("select true::bool as bool").get()
                let row = try #require(rows.first).makeRandomAccess()
                #expect(row[data: "bool"].bool == true)
            }
            do {
                let rows = try await conn.simpleQuery("select false::bool as bool").get()
                let row = try #require(rows.first).makeRandomAccess()
                #expect(row[data: "bool"].bool == false)
            }
        }
    }

    @Test func bytesSerialize() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("select $1::bytea as bytes", [
                PostgresData(bytes: [1, 2, 3])
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "bytes"].bytes == [1, 2, 3])
        }
    }

    @Test func jsonbSerialize() async throws {
        struct Object: Codable, PostgresCodable {
            let foo: Int
            let bar: Int
        }

        try await withConnection { conn in
            do {
                let postgresData = try PostgresData(jsonb: Object(foo: 1, bar: 2))
                let rows = try await conn.query("select $1::jsonb as jsonb", [postgresData]).get()

                let object = try #require(rows.first).decode(Object.self, context: .default)
                #expect(object.foo == 1)
                #expect(object.bar == 2)
            }

            do {
                let rows = try await conn.query("select jsonb_build_object('foo',1,'bar',2) as jsonb").get()

                let object = try #require(rows.first).decode(Object.self, context: .default)
                #expect(object.foo == 1)
                #expect(object.bar == 2)
            }
        }
    }

    @Test func jsonbDecodeString() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("select '{\"hello\": \"world\"}'::jsonb as data").get()

            let resultString = try #require(rows.first).decode(String.self, context: .default)

            #expect(resultString == "{\"hello\": \"world\"}")
        }
    }

    @Test func int4RangeSerialize() async throws {
        try await withConnection { conn in
            do {
                let range: Range<Int32> = Int32.min..<Int32.max
                var binds = PostgresBindings()
                binds.append(range, context: .default)
                let query = PostgresQuery(
                    unsafeSQL: "select $1::int4range as range",
                    binds: binds
                )
                let rowSequence = try await conn.query(query, logger: .psqlTest)
                var rowIterator = rowSequence.makeAsyncIterator()
                let row = try await rowIterator.next()
                let decodedRange = try #require(row).decode(Range<Int32>.self, context: .default)
                #expect(range == decodedRange)
            }
            do {
                let emptyRange: Range<Int32> = Int32.min..<Int32.min
                var binds = PostgresBindings()
                binds.append(emptyRange, context: .default)
                let query = PostgresQuery(
                    unsafeSQL: "select $1::int4range as range",
                    binds: binds
                )
                let rowSequence = try await conn.query(query, logger: .psqlTest)
                var rowIterator = rowSequence.makeAsyncIterator()
                let row = try await rowIterator.next()
                let decodedEmptyRange = try #require(row).decode(Range<Int32>.self, context: .default)
                let expectedRange: Range<Int32> = Int32.valueForEmptyRange..<Int32.valueForEmptyRange
                #expect(emptyRange != expectedRange)
                #expect(expectedRange == decodedEmptyRange)
            }
            do {
                let closedRange: ClosedRange<Int32> = Int32.min...(Int32.max - 1)
                var binds = PostgresBindings()
                binds.append(closedRange, context: .default)
                let query = PostgresQuery(
                    unsafeSQL: "select $1::int4range as range",
                    binds: binds
                )
                let rowSequence = try await conn.query(query, logger: .psqlTest)
                var rowIterator = rowSequence.makeAsyncIterator()
                let row = try await rowIterator.next()
                let decodedClosedRange = try #require(row).decode(ClosedRange<Int32>.self, context: .default)
                #expect(closedRange == decodedClosedRange)
            }
        }
    }

    @Test func int8RangeSerialize() async throws {
        try await withConnection { conn in
            do {
                let range: Range<Int64> = Int64.min..<Int64.max
                var binds = PostgresBindings()
                binds.append(range, context: .default)
                let query = PostgresQuery(
                    unsafeSQL: "select $1::int8range as range",
                    binds: binds
                )
                let rowSequence = try await conn.query(query, logger: .psqlTest)
                var rowIterator = rowSequence.makeAsyncIterator()
                let row = try await rowIterator.next()
                let decodedRange = try #require(row).decode(Range<Int64>.self, context: .default)
                #expect(range == decodedRange)
            }
            do {
                let emptyRange: Range<Int64> = Int64.min..<Int64.min
                var binds = PostgresBindings()
                binds.append(emptyRange, context: .default)
                let query = PostgresQuery(
                    unsafeSQL: "select $1::int8range as range",
                    binds: binds
                )
                let rowSequence = try await conn.query(query, logger: .psqlTest)
                var rowIterator = rowSequence.makeAsyncIterator()
                let row = try await rowIterator.next()
                let decodedEmptyRange = try #require(row).decode(Range<Int64>.self, context: .default)
                let expectedRange: Range<Int64> = Int64.valueForEmptyRange..<Int64.valueForEmptyRange
                #expect(emptyRange != expectedRange)
                #expect(expectedRange == decodedEmptyRange)
            }
            do {
                let closedRange: ClosedRange<Int64> = Int64.min...(Int64.max - 1)
                var binds = PostgresBindings()
                binds.append(closedRange, context: .default)
                let query = PostgresQuery(
                    unsafeSQL: "select $1::int8range as range",
                    binds: binds
                )
                let rowSequence = try await conn.query(query, logger: .psqlTest)
                var rowIterator = rowSequence.makeAsyncIterator()
                let row = try await rowIterator.next()
                let decodedClosedRange = try #require(row).decode(ClosedRange<Int64>.self, context: .default)
                #expect(closedRange == decodedClosedRange)
            }
        }
    }

    @available(*, deprecated, message: "Test deprecated functionality")
    @Test func failingTLSConnectionClosesConnection() async throws {
        // There was a bug (https://github.com/vapor/postgres-nio/issues/133) where we would hit
        // an assert because we didn't close the connection. This test should succeed without hitting
        // the assert

        // postgres://uymgphwj:7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA@elmer.db.elephantsql.com:5432/uymgphwj

        // We should get an error because you can't use an IP address for SNI, but we shouldn't bomb out by
        // hitting the assert
        await #expect(throws: (any Error).self) {
            try await PostgresConnection.connect(
                to: SocketAddress.makeAddressResolvingHost("elmer.db.elephantsql.com", port: 5432),
                tlsConfiguration: .makeClientConfiguration(),
                serverHostname: "34.228.73.168",
                on: MultiThreadedEventLoopGroup.singleton.any()
            ).get()
        }
        // If we hit this, we're all good
    }

    @available(*, deprecated, message: "Test deprecated functionality")
    @Test func invalidPassword() async throws {
        let conn = try await PostgresConnection.testUnauthenticated(on: MultiThreadedEventLoopGroup.singleton.any()).get()
        let error = await #expect(throws: PostgresError.self) {
            try await conn.authenticate(username: "invalid", database: "invalid", password: "bad").get()
        }
        #expect(error?.code == .invalidPassword || error?.code == .invalidAuthorizationSpecification)

        // in this case the connection will be closed by the remote
        try await conn.closeFuture.get()
    }

    @Test func columnsInJoin() async throws {
        try await withConnection { conn in
            let dateInTable1 = Date(timeIntervalSince1970: 1234)
            let dateInTable2 = Date(timeIntervalSince1970: 5678)
            _ = try await conn.simpleQuery("DROP TABLE IF EXISTS \"table1\"").get()
            _ = try await conn.simpleQuery("""
            CREATE TABLE table1 (
                "id" int8 NOT NULL,
                "table2_id" int8,
                "intValue" int8,
                "stringValue" text,
                "dateValue" timestamptz,
                PRIMARY KEY ("id")
            );
            """).get()

            _ = try await conn.simpleQuery("DROP TABLE IF EXISTS \"table2\"").get()
            _ = try await conn.simpleQuery("""
            CREATE TABLE table2 (
                "id" int8 NOT NULL,
                "intValue" int8,
                "stringValue" text,
                "dateValue" timestamptz,
                PRIMARY KEY ("id")
            );
            """).get()

            _ = try await conn.simpleQuery("INSERT INTO table1 VALUES (12, 34, 56, 'stringInTable1', to_timestamp(1234))").get()
            _ = try await conn.simpleQuery("INSERT INTO table2 VALUES (34, 78, 'stringInTable2', to_timestamp(5678))").get()

            let rows = try await conn.query("""
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
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "t1_id"].int == 12)
            #expect(row[data: "table2_id"].int == 34)
            #expect(row[data: "t1_intValue"].int == 56)
            #expect(row[data: "t1_stringValue"].string == "stringInTable1")
            #expect(row[data: "t1_dateValue"].date == dateInTable1)
            #expect(row[data: "t2_id"].int == 34)
            #expect(row[data: "t2_intValue"].int == 78)
            #expect(row[data: "t2_stringValue"].string == "stringInTable2")
            #expect(row[data: "t2_dateValue"].date == dateInTable2)

            _ = try await conn.simpleQuery("DROP TABLE \"table2\"").get()
            _ = try await conn.simpleQuery("DROP TABLE \"table1\"").get()
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func stringArrays() async throws {
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

        try await withConnection { conn in
            let rows = try await conn.query(query, [
                PostgresData(uuid: UUID(uuidString: "D2710E16-EB07-4FD6-A87E-B1BE41C9BD3D")!),
                PostgresData(int: Int(0)),
                PostgresData(date: Date(timeIntervalSince1970: 0)),
                PostgresData(date: Date(timeIntervalSince1970: 0)),
                PostgresData(string: "Foo"),
                PostgresData(array: ["US"]),
                PostgresData(array: ["en"]),
                PostgresData(array: ["USD", "DKK"]),
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "countries"].array(of: String.self) == ["US"])
            #expect(row[data: "languages"].array(of: String.self) == ["en"])
            #expect(row[data: "currencies"].array(of: String.self) == ["USD", "DKK"])
        }
    }

    @Test func bindDate() async throws {
        // https://github.com/vapor/postgres-nio/issues/53
        let date =  Date(timeIntervalSince1970: 1571425782)
        let query = """
        SELECT $1::json as "date"
        """
        try await withConnection { conn -> Void in
            let error = await #expect(throws: PostgresError.self) {
                _ = try await conn.query(query, [.init(date: date)]).get()
            }
            guard case let .server(serverError) = try #require(error) else {
                Issue.record("Expected a .server error but got \(String(describing: error))")
                return
            }
            #expect(serverError.fields[.routine] == "transformTypeCast")
        }
    }

    @Test func bindCharString() async throws {
        // https://github.com/vapor/postgres-nio/issues/53
        let query = """
        SELECT $1::char as "char"
        """
        try await withConnection { conn in
            let rows = try await conn.query(query, [.init(string: "f")]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "char"].string == "f")
        }
    }

    @Test func bindCharUInt8() async throws {
        // https://github.com/vapor/postgres-nio/issues/53
        let query = """
        SELECT $1::char as "char"
        """
        try await withConnection { conn in
            let rows = try await conn.query(query, [.init(uint8: 42)]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "char"].string == "*")
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func doubleArraySerialization() async throws {
        try await withConnection { conn in
            let doubles: [Double] = [3.14, 42]
            let rows = try await conn.query("""
            select
                $1::double precision[] as doubles
            """, [
                .init(array: doubles)
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "doubles"].array(of: Double.self) == doubles)
        }
    }

    // https://github.com/vapor/postgres-nio/issues/42
    @Test func uint8Serialization() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                $1::"char" as int
            """, [
                .init(uint8: 5)
            ]).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "int"].uint8 == 5)
        }
    }

    @Test func preparedQuery() async throws {
        try await withConnection { conn in
            let prepared = try await conn.prepare(query: "SELECT 1 as one;").get()
            let rows = try await prepared.execute().get()

            #expect(rows.count == 1)
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "one"].int == 1)
        }
    }

    @Test func prepareQueryClosure() async throws {
        try await withConnection { conn in
            let eventLoop = conn.eventLoop
            let queries = try await conn.prepare(query: "SELECT $1::text as foo;", handler: { query in
                let a = query.execute(["a"])
                let b = query.execute(["b"])
                let c = query.execute(["c"])
                return EventLoopFuture.whenAllSucceed([a, b, c], on: eventLoop)
            }).get()
            #expect(queries.count == 3)
            var resultIterator = queries.makeIterator()
            #expect(try resultIterator.next()?.first?.decode(String.self, context: .default) == "a")
            #expect(try resultIterator.next()?.first?.decode(String.self, context: .default) == "b")
            #expect(try resultIterator.next()?.first?.decode(String.self, context: .default) == "c")
        }
    }

    // https://github.com/vapor/postgres-nio/issues/122
    @Test func preparedQueryNoResults() async throws {
        try await withConnection { conn in
            _ = try await conn.simpleQuery("DROP TABLE IF EXISTS \"table_no_results\"").get()
            _ = try await conn.simpleQuery("""
            CREATE TABLE table_no_results (
                "id" int8 NOT NULL,
                "stringValue" text,
                PRIMARY KEY ("id")
            );
            """).get()

            _ = try await conn.prepare(query: "DELETE FROM \"table_no_results\" WHERE id = $1").get()

            _ = try await conn.simpleQuery("DROP TABLE \"table_no_results\"").get()
        }
    }


    // https://github.com/vapor/postgres-nio/issues/71
    @Test func char1Serialization() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                '5'::char(1) as one,
                '5'::char(2) as two
            """).get()

            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "one"].uint8 == 53)
            #expect(row[data: "one"].int16 == 53)
            #expect(row[data: "one"].string == "5")
            #expect(row[data: "two"].uint8 == nil)
            #expect(row[data: "two"].int16 == nil)
            #expect(row[data: "two"].string == "5 ")
        }
    }

    @Test func userDefinedType() async throws {
        try await withConnection { conn in
            _ = try await conn.query("DROP TYPE IF EXISTS foo").get()
            _ = try await conn.query("CREATE TYPE foo AS ENUM ('bar', 'qux')").get()

            let res = try await conn.query("SELECT 'qux'::foo as foo").get()
            let row = try #require(res.first).makeRandomAccess()
            #expect(row[data: "foo"].string == "qux")

            _ = try await conn.query("DROP TYPE foo").get()
        }
    }

    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func nullBind() async throws {
        try await withConnection { conn in
            let res = try await conn.query("SELECT $1::text as foo", [String?.none.postgresData!]).get()
            let row = try #require(res.first).makeRandomAccess()
            #expect(row[data: "foo"].string == nil)
        }
    }

    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    @Test func updateMetadata() async throws {
        try await withConnection { conn in
            _ = try await conn.simpleQuery("DROP TABLE IF EXISTS test_table").get()
            _ = try await conn.simpleQuery("CREATE TABLE test_table(pk int PRIMARY KEY)").get()
            _ = try await conn.simpleQuery("INSERT INTO test_table VALUES(1)").get()

            let receivedMetadata = Mutex<PostgresQueryMetadata?>(nil)
            _ = try await conn.query("DELETE FROM test_table", onMetadata: { metadata in
                receivedMetadata.withLock { $0 = metadata }
            }, onRow: { _ in }).get()
            let metadata = try #require(receivedMetadata.withLock { $0 })
            #expect(metadata.command == "DELETE")
            #expect(metadata.oid == nil)
            #expect(metadata.rows == 1)

            let rows = try await conn.query("DELETE FROM test_table").get()
            #expect(rows.metadata.command == "DELETE")
            #expect(rows.metadata.oid == nil)
            #expect(rows.metadata.rows == 0)

            _ = try await conn.simpleQuery("DROP TABLE test_table").get()
        }
    }

    @Test func tooManyBinds() async throws {
        try await withConnection { conn in
            let binds = [PostgresData].init(repeating: .null, count: Int(UInt16.max) + 1)
            let error = await #expect(throws: PSQLError.self) {
                _ = try await conn.query("SELECT version()", binds).get()
            }
            #expect(error?.code == .tooManyParameters)
        }
    }

    @Test func remoteClose() async throws {
        let conn = try await PostgresConnection.test()
        try await conn.channel.close().get()
    }

    // https://github.com/vapor/postgres-nio/issues/113
    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func varyingCharArray() async throws {
        try await withConnection { conn in
            let res = try await conn.query(#"SELECT '{"foo", "bar", "baz"}'::VARCHAR[] as foo"#).get()
            let row = try #require(res.first).makeRandomAccess()
            #expect(row[data: "foo"].array(of: String.self) == ["foo", "bar", "baz"])
        }
    }

    // https://github.com/vapor/postgres-nio/issues/115
    @Test func setTimeZone() async throws {
        try await withConnection { conn in
            _ = try await conn.simpleQuery("SET TIME ZONE INTERVAL '+5:45' HOUR TO MINUTE").get()
            _ = try await conn.query("SET TIME ZONE INTERVAL '+5:45' HOUR TO MINUTE").get()
        }
    }

    @Test func integerConversions() async throws {
        try await withConnection { conn in
            let rows = try await conn.query("""
            select
                'a'::char as test8,

                '-32768'::smallint as min16,
                '32767'::smallint as max16,

                '-2147483648'::integer as min32,
                '2147483647'::integer as max32,

                '-9223372036854775808'::bigint as min64,
                '9223372036854775807'::bigint as max64
            """).get()
            let row = try #require(rows.first).makeRandomAccess()
            #expect(row[data: "test8"].uint8 == 97)
            #expect(row[data: "test8"].int16 == 97)
            #expect(row[data: "test8"].int32 == 97)
            #expect(row[data: "test8"].int64 == 97)

            #expect(row[data: "min16"].uint8 == nil)
            #expect(row[data: "max16"].uint8 == nil)
            #expect(row[data: "min16"].int16 == .min)
            #expect(row[data: "max16"].int16 == .max)
            #expect(row[data: "min16"].int32 == -32768)
            #expect(row[data: "max16"].int32 == 32767)
            #expect(row[data: "min16"].int64 ==  -32768)
            #expect(row[data: "max16"].int64 == 32767)

            #expect(row[data: "min32"].uint8 == nil)
            #expect(row[data: "max32"].uint8 == nil)
            #expect(row[data: "min32"].int16 == nil)
            #expect(row[data: "max32"].int16 == nil)
            #expect(row[data: "min32"].int32 == .min)
            #expect(row[data: "max32"].int32 == .max)
            #expect(row[data: "min32"].int64 == -2147483648)
            #expect(row[data: "max32"].int64 == 2147483647)

            #expect(row[data: "min64"].uint8 == nil)
            #expect(row[data: "max64"].uint8 == nil)
            #expect(row[data: "min64"].int16 == nil)
            #expect(row[data: "max64"].int16 == nil)
            #expect(row[data: "min64"].int32 == nil)
            #expect(row[data: "max64"].int32 == nil)
            #expect(row[data: "min64"].int64 == .min)
            #expect(row[data: "max64"].int64 == .max)
        }
    }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { .init(rawValue: $0) } ?? .info
        return handler
    }
    return true
}()
