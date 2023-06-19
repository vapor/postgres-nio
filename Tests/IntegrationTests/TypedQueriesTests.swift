import Logging
import XCTest
import PostgresNIO

final class TypedQueriesTests: XCTestCase {
    func testTypedPostgresQuery() async throws {
        struct MyQuery: PostgresTypedQuery {
            struct Row: PostgresTypedRow {
                let id: Int
                let name: String

                init(from row: PostgresRow) throws {
                    (id, name) = try row.decode((Int, String).self, context: .default)
                }
            }

            var sql: PostgresQuery {
                "SELECT id, name FROM users"
            }
        }

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()

        try await withTestConnection(on: eventLoop) { connection in
            let createTableQuery = PostgresQuery(unsafeSQL: """
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name character varying(255) NOT NULL
            );
            """)
            let name = "foobar"

            try await connection.query(createTableQuery, logger: .psqlTest)
            try await connection.query("INSERT INTO users (name) VALUES (\(name));", logger: .psqlTest)

            let rows = try await connection.query(MyQuery(), logger: .psqlTest)
            for try await row in rows {
                XCTAssertEqual(row.name, name)
            }

            let dropQuery = PostgresQuery(unsafeSQL: "DROP TABLE users")
            try await connection.query(dropQuery, logger: .psqlTest)
        }
    }
}
