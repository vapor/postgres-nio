import NIO
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
        let version = try rows[0].decode(String.self, at: "version")
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testQueryVersion() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.query("SELECT version()", .init()).wait()
        XCTAssertEqual(rows.count, 1)
        let version = try rows[0].decode(String.self, at: "version")
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
        
    }
    
    func testQuerySelectParameter() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.query("SELECT $1::TEXT as foo", ["hello"]).wait()
        XCTAssertEqual(rows.count, 1)
        let version = try rows[0].decode(String.self, at: "foo")
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
            let pgtype = try results[0].decode(PGType.self, table: "pg_type")
            XCTAssertEqual(pgtype.typname, "float8")
            XCTAssertEqual(pgtype.typnamespace, 11)
            XCTAssertEqual(pgtype.typowner, 10)
            XCTAssertEqual(pgtype.typlen, 8)
            #warning("finish adding columns")
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
            print(results[0])
            let kitchenSink = try results[0].decode(Integers.self)
            XCTAssertEqual(kitchenSink.smallint, 1)
            XCTAssertEqual(kitchenSink.smallint_min, -32_767)
            XCTAssertEqual(kitchenSink.smallint_max, 32_767)
            XCTAssertEqual(kitchenSink.int, 1)
            XCTAssertEqual(kitchenSink.int_min, -2_147_483_647)
            XCTAssertEqual(kitchenSink.int_max, 2_147_483_647)
            XCTAssertEqual(kitchenSink.bigint, 1)
            XCTAssertEqual(kitchenSink.bigint_min, -9_223_372_036_854_775_807)
            XCTAssertEqual(kitchenSink.bigint_max, 9_223_372_036_854_775_807)
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
            let kitchenSink = try results[0].decode(Pi.self)
            XCTAssertEqual(kitchenSink.text, "3.14159265358979")
            XCTAssertEqual(kitchenSink.numeric_string, "3.14159265358979")
            XCTAssertTrue(kitchenSink.numeric_decimal.isLess(than: 3.14159265358980))
            XCTAssertFalse(kitchenSink.numeric_decimal.isLess(than: 3.14159265358978))
            XCTAssertTrue(kitchenSink.double.description.hasPrefix("3.141592"))
            XCTAssertTrue(kitchenSink.float.description.hasPrefix("3.141592"))
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
            let kitchenSink = try results[0].decode(Model.self)
            XCTAssertEqual(kitchenSink.id.uuidString, "123E4567-E89B-12D3-A456-426655440000")
            XCTAssertEqual(kitchenSink.string, "123E4567-E89B-12D3-A456-426655440000")
        default: XCTFail("incorrect result count")
        }
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
                let results = try conn.simpleQuery("SELECT * FROM generate_series(1, 10000) num").wait()
                for result in results {
                    _ = try result.decode(Series.self)
                }
            } catch {
                XCTFail("\(error)")
            }
        }
    }
}
