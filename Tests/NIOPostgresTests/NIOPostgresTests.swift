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
        let version = rows[0].decode(String.self, at: "version")
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testQueryVersion() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.query("SELECT version()", .init()).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].decode(String.self, at: "version")
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
        
    }
    
    func testQuerySelectParameter() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try? conn.close().wait() }
        let rows = try conn.query("SELECT $1 as foo", ["hello"]).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].decode(String.self, at: "foo")
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
        // 1247.typisdefined: 0x01 (BOOLEAN)
        // 1247.typbasetype: 0x00000000 (OID)
        // 1247.typnotnull: 0x00 (BOOLEAN)
        // 1247.typcategory: 0x42 (CHAR)
        // 1247.typname: 0x626f6f6c (NAME)
        // 1247.typbyval: 0x01 (BOOLEAN)
        // 1247.typrelid: 0x00000000 (OID)
        // 1247.typalign: 0x63 (CHAR)
        // 1247.typndims: 0x00000000 (INTEGER)
        // 1247.typacl: null
        // 1247.typsend: 0x00000985 (REGPROC)
        // 1247.typmodout: 0x00000000 (REGPROC)
        // 1247.typstorage: 0x70 (CHAR)
        // 1247.typispreferred: 0x01 (BOOLEAN)
        // 1247.typinput: 0x000004da (REGPROC)
        // 1247.typoutput: 0x000004db (REGPROC)
        // 1247.typlen: 0x0001 (SMALLINT)
        // 1247.typcollation: 0x00000000 (OID)
        // 1247.typdefaultbin: null
        // 1247.typelem: 0x00000000 (OID)
        // 1247.typnamespace: 0x0000000b (OID)
        // 1247.typtype: 0x62 (CHAR)
        // 1247.typowner: 0x0000000a (OID)
        // 1247.typdefault: null
        // 1247.typtypmod: 0xffffffff (INTEGER)
        // 1247.typarray: 0x000003e8 (OID)
        // 1247.typreceive: 0x00000984 (REGPROC)
        // 1247.typmodin: 0x00000000 (REGPROC)
        // 1247.typanalyze: 0x00000000 (REGPROC)
        // 1247.typdelim: 0x2c (CHAR)
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
            let result = results[0]
            XCTAssertEqual(result.decode(String.self, at: "typname"), "float8")
            XCTAssertEqual(result.decode(UInt32.self, at: "typnamespace"), 11)
            XCTAssertEqual(result.decode(UInt32.self, at: "typowner"), 10)
            XCTAssertEqual(result.decode(Int16.self, at: "typlen"), 8)
            #warning("finish adding columns")
        default: XCTFail("Should be only one result")
        }
    }
}
