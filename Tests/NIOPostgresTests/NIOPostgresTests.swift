import NIO
import NIOPostgres
import XCTest

final class NIOPostgresTests: XCTestCase {
    func testConnectAndClose() throws {
        let conn = try PostgresConnection.test().wait()
        try conn.close().wait()
    }
    
    func testSimpleQueryVersion() throws {
        let conn = try PostgresConnection.test().wait()
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].decode(String.self, at: "version")
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
        try conn.close().wait()
    }
}
