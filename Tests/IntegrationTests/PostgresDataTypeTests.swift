import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import XCTest

final class PostgresDataTypeIntegrationTests: XCTestCase {
    func testDecodeOIDFromCatalog() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let rows = try await connection.query(
                "SELECT 'text'::regtype::oid",
                logger: .psqlTest
            )

            var iterator = rows.makeAsyncIterator()
            let firstRow = try await iterator.next()
            XCTAssertEqual(try firstRow?.decode(PostgresDataType.self), .text)
            let done = try await iterator.next()
            XCTAssertNil(done)
        }
    }

    func testEncodeOIDAsQueryParameter() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let rows = try await connection.query(
                "SELECT typname FROM pg_type WHERE oid = \(PostgresDataType.text)",
                logger: .psqlTest
            )

            var iterator = rows.makeAsyncIterator()
            let firstRow = try await iterator.next()
            XCTAssertEqual(try firstRow?.decode(String.self), "text")
            let done = try await iterator.next()
            XCTAssertNil(done)
        }
    }

    func testOIDAboveInt32MaxRoundTrips() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        let large = PostgresDataType(3_000_000_000)

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let serverSide = try await connection.query("SELECT 3000000000::oid", logger: .psqlTest)
            var serverSideIterator = serverSide.makeAsyncIterator()
            let serverSideRow = try await serverSideIterator.next()
            XCTAssertEqual(try serverSideRow?.decode(PostgresDataType.self), large)

            let roundTrip = try await connection.query("SELECT \(large)::oid", logger: .psqlTest)
            var roundTripIterator = roundTrip.makeAsyncIterator()
            let roundTripRow = try await roundTripIterator.next()
            XCTAssertEqual(try roundTripRow?.decode(PostgresDataType.self), large)
        }
    }

    func testOIDArrayRoundTrips() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

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
            XCTAssertEqual(typeNames, ["bool", "text"])

            let arrayRows = try await connection.query("SELECT ARRAY[16, 25]::oid[]", logger: .psqlTest)
            var arrayIterator = arrayRows.makeAsyncIterator()
            let arrayRow = try await arrayIterator.next()
            XCTAssertEqual(try arrayRow?.decode([PostgresDataType].self), [.bool, .text])
        }
    }

    func testDecodeRegprocFromCatalog() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        try await withTestConnection(on: eventLoopGroup.next()) { connection in
            let rows = try await connection.query(
                "SELECT typinput, typinput::oid FROM pg_type WHERE typname = 'bool'",
                logger: .psqlTest
            )

            var iterator = rows.makeAsyncIterator()
            let firstRow = try await iterator.next()
            let (asRegproc, asOID) = try XCTUnwrap(firstRow).decode((PostgresDataType, PostgresDataType).self)
            XCTAssertEqual(asRegproc, asOID)
            XCTAssertNotEqual(asRegproc.rawValue, 0)
        }
    }
}
