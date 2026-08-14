import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import Testing

@Suite
struct PostgresDataTypeIntegrationTests {
    @Test func decodeOIDFromCatalog() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try eventLoopGroup.syncShutdownGracefully() } }

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let rows = try await connection.query(
                "SELECT 'text'::regtype",
                logger: .psqlTest
            )

            var iterator = rows.makeAsyncIterator()
            let firstRow = try await iterator.next()
            #expect(try firstRow?.decode(PostgresDataType.self) == .text)
            let done = try await iterator.next()
            #expect(done == nil)
        }
    }

    func testEncodeOIDAsQueryParameter() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try eventLoopGroup.syncShutdownGracefully() } }

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let rows = try await connection.query(
                "SELECT typname FROM pg_type WHERE oid = \(PostgresDataType.text)",
                logger: .psqlTest
            )

            var iterator = rows.makeAsyncIterator()
            let firstRow = try await iterator.next()
            #expect(try firstRow?.decode(String.self) == "text")
            let done = try await iterator.next()
            #expect(done == nil)
        }
    }

    func testOIDAboveInt32MaxRoundTrips() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try eventLoopGroup.syncShutdownGracefully() } }

        let large = PostgresDataType(3_000_000_000)

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let serverSide = try await connection.query("SELECT 3000000000::oid", logger: .psqlTest)
            var serverSideIterator = serverSide.makeAsyncIterator()
            let serverSideRow = try await serverSideIterator.next()
            #expect(try serverSideRow?.decode(PostgresDataType.self) == large)

            let roundTrip = try await connection.query("SELECT \(large)::oid", logger: .psqlTest)
            var roundTripIterator = roundTrip.makeAsyncIterator()
            let roundTripRow = try await roundTripIterator.next()
            #expect(try roundTripRow?.decode(PostgresDataType.self) == large)
        }
    }

    func testOIDArrayRoundTrips() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try eventLoopGroup.syncShutdownGracefully() } }

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let wanted: [PostgresDataType] = [.bool, .text]
            let rows = try await connection.query(
                "SELECT typname FROM pg_type WHERE oid = ANY(\(wanted)) ORDER BY oid",
                logger: .psqlTest
            )

            var typeNames = [String]()
            for try await row in rows {
                typeNames.append(try row.decode(String.self))
            }
            #expect(typeNames == ["bool", "text"])

            let arrayRows = try await connection.query("SELECT ARRAY[16, 25]::oid[]", logger: .psqlTest)
            var arrayIterator = arrayRows.makeAsyncIterator()
            let arrayRow = try await arrayIterator.next()
            #expect(try arrayRow?.decode([PostgresDataType].self) == [.bool, .text])
        }
    }

    func testDecodeRegprocFromCatalog() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try eventLoopGroup.syncShutdownGracefully() } }

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let rows = try await connection.query(
                "SELECT typinput, typinput::oid FROM pg_type WHERE typname = 'bool'",
                logger: .psqlTest
            )

            var iterator = rows.makeAsyncIterator()
            let firstRow = try await iterator.next()
            let (asRegproc, asOID) = try #require(firstRow).decode((PostgresDataType, PostgresDataType).self)
            #expect(asRegproc == asOID)
            #expect(asRegproc.rawValue == 0)
        }
    }
}
