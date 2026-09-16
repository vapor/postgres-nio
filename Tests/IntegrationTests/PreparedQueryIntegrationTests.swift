import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import Testing

@Suite
struct PreparedQueryIntegrationTests {
    @Test(.bug("https://github.com/vapor/postgres-nio/issues/303"))
    func selectPlainPrepared() async throws {
        try await withTestConnection(on: MultiThreadedEventLoopGroup.singleton.any()) { connection in
            let prepared = try await connection.prepare(query: "SELECT 10, 20").get()
            let rows = try await prepared.execute().get()

            #expect(rows.count == 1)
            let values = try #require(rows.first).decode((Int, Int).self)
            #expect(values == (10, 20))

            try await prepared.deallocate().get()
        }
    }

    @Test(.bug("https://github.com/vapor/postgres-nio/issues/303"))
    func selectBoundPrepared() async throws {
        try await withTestConnection(on: MultiThreadedEventLoopGroup.singleton.any()) { connection in
            let prepared = try await connection.prepare(query: "SELECT $1::int8, $2::int8").get()
            let rows = try await prepared.execute([PostgresData(int: 10), PostgresData(int: 20)]).get()

            #expect(rows.count == 1)
            let values = try #require(rows.first).decode((Int, Int).self)
            #expect(values == (10, 20))

            try await prepared.deallocate().get()
        }
    }
}
