import NIOPostgres
import XCTest

final class NIOPostgresTests: XCTestCase {
    private var group: EventLoopGroup!
    private var eventLoop: EventLoop {
        return self.group.next()
    }
    
    override func setUp() {
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }
    
    override func tearDown() {
        XCTAssertNoThrow(try self.group.syncShutdownGracefully())
        self.group = nil
    }
    
    // MARK: Tests
    
    func testConnectAndClose() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        try conn.close().wait()
    }
    
    func testSimpleQueryVersion() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testQueryVersion() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.query("SELECT version()", .init()).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
        
    }
    
    func testQuerySelectParameter() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.query("SELECT $1::TEXT as foo", [.init(string: "hello")]).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("foo")?.string
        XCTAssertEqual(version, "hello")
    }
    
    func testSQLError() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        do {
            _ = try conn.simpleQuery("SELECT &").wait()
            XCTFail("An error should have been thrown")
        } catch let error as PostgresError {
            XCTAssertEqual(error.code, .syntax_error)
        }
    }
    
    func testSelectTypes() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        let results = try conn.simpleQuery("SELECT * FROM pg_type").wait()
        XCTAssert(results.count >= 350, "Results count not large enough: \(results.count)")
    }
    
    func testSelectType() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
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
            #warning("TODO: finish adding columns")
        default: XCTFail("Should be only one result")
        }
    }
    
    func testIntegers() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
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
            XCTAssertEqual(results[0].column("text")?.string, "3.14159265358979")
            XCTAssertEqual(results[0].column("numeric_string")?.string, "3.14159265358979")
            XCTAssertTrue(results[0].column("numeric_decimal")?.as(custom: Decimal.self)?.isLess(than: 3.14159265358980) ?? false)
            XCTAssertFalse(results[0].column("numeric_decimal")?.as(custom: Decimal.self)?.isLess(than: 3.14159265358978) ?? true)
            XCTAssertTrue(results[0].column("double")?.double?.description.hasPrefix("3.141592") ?? false)
            XCTAssertTrue(results[0].column("float")?.float?.description.hasPrefix("3.141592") ?? false)
        default: XCTFail("incorrect result count")
        }
    }
    
    func testUUID() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
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
    
    func testRemoteTLSServer() throws {
        let url = "postgres://uymgphwj:7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA@elmer.db.elephantsql.com:5432/uymgphwj"
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! elg.syncShutdownGracefully() }
        
        let conn = try PostgresConnection.connect(
            to: SocketAddress.makeAddressResolvingHost("elmer.db.elephantsql.com", port: 5432),
            on: elg.next()
        ).wait()
        let upgraded = try conn.requestTLS(using: .forClient(certificateVerification: .none)).wait()
        XCTAssertTrue(upgraded)
        try! conn.authenticate(username: "uymgphwj", database: "uymgphwj", password: "7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA").wait()
        defer { try? conn.close().wait() }
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testSelectPerformance() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        measure {
            do {
                _ = try conn.query("SELECT * FROM pg_type").wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }
    
    func testRangeSelectPerformance() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        measure {
            do {
                _ = try conn.simpleQuery("SELECT * FROM generate_series(1, 10000) num").wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }
    
    func testRangeSelectDecodePerformance() throws {
        struct Series: Decodable {
            var num: Int
        }
        
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        measure {
            do {
                try conn.query("SELECT * FROM generate_series(1, 10000) num") { row in
                    _ = row.column("num")?.int
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testInvalidPassword() throws {
        let auth = PostgresConnection.testUnauthenticated(on: eventLoop).flatMap({ (connection) in
            connection.authenticate(username: "invalid", database: "invalid", password: "bad").map { connection }
        })
        do {
            let _ = try auth.wait()
            XCTFail("The authentication should fail")
        } catch let error as PostgresError {
           XCTAssertEqual(error.code, .invalid_password)
        }
    }
}
