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
        let rows = try conn.query("SELECT $1 as foo", ["hello"]).wait()
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
    
    func testSelectPerformance() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        measure {
            do {
                _ = try conn.simpleQuery("SELECT * FROM pg_type").wait()
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
}
