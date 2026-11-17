import NIOCore
import Testing

@testable import PostgresNIO

@Suite
struct PSQLMetadataTests {
    @Test func select() {
        #expect(100 == PostgresQueryMetadata(string: "SELECT 100")?.rows)
        #expect(PostgresQueryMetadata(string: "SELECT") != nil)
        #expect(PostgresQueryMetadata(string: "SELECT")?.rows == nil)
        #expect(PostgresQueryMetadata(string: "SELECT 100 100") == nil)
    }

    @Test func update() {
        #expect(100 == PostgresQueryMetadata(string: "UPDATE 100")?.rows)
        #expect(PostgresQueryMetadata(string: "UPDATE") == nil)
        #expect(PostgresQueryMetadata(string: "UPDATE 100 100") == nil)
    }
}
