//
//  File.swift
//  File
//
//  Created by Fabian Fett on 19.10.21.
//

import XCTest
import NIOCore
import NIOPosix
import Logging
@testable import PostgresNIO

#if swift(>=5.5) && canImport(_Concurrency)
@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
final class PSQLConnection_AsyncIntegrationTests: XCTestCase {

    func testConnectAndClose() { XCTAsyncTest {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        let conn = try await PSQLConnection.test(on: eventLoop)
        try await conn.close()
    } }
    
    func testAuthenticationFailure() { XCTAsyncTest {
        // If the postgres server trusts every connection, it is really hard to create an
        // authentication failure.
        try XCTSkipIf(env("POSTGRES_HOST_AUTH_METHOD") == "trust")
        
        let config = PSQLConnection.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: 5432,
            username: env("POSTGRES_USER") ?? "postgres",
            database: env("POSTGRES_DB"),
            password: "wrong_password",
            tlsConfiguration: nil)
        
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        
        var logger = Logger.psqlTest
        logger.logLevel = .info
        
        do {
            let conn = try await PSQLConnection.connect(configuration: config, logger: logger, on: eventLoopGroup.next())
            XCTFail("Did not expect to create a connection here")
            try await conn.close()
        } catch {
            XCTAssertTrue(error is PSQLError)
        }
    } }
    
    func testQueryVersion() {
        PSQLConnection.withTestConnection { connection in
            let rowSequence = try await connection.query("SELECT version()", logger: .psqlTest)
            let rows = try await rowSequence.collect()
            XCTAssertEqual(rows.count, 1)
            XCTAssert(try rows.first?.decode(column: 0, as: String.self).contains("PostgreSQL") ?? false)
        }
    }
    
    func testQueryVersionDecodingError() {
        PSQLConnection.withTestConnection { connection in
            let rowSequence = try await connection.query("SELECT version()", logger: .psqlTest)
            let rows = try await rowSequence.collect()
            XCTAssertEqual(rows.count, 1)
            XCTAssertThrowsError(try rows.first?.decode(Int?.self)) {
                XCTAssertEqual(($0 as? PSQLCastingError)?.line, #line - 1)
                XCTAssertEqual(($0 as? PSQLCastingError)?.file, #file)
//                XCTAssert(($0 as? PSQLCastingError)?.targetType is Optional<Int>)
                XCTAssertEqual(($0 as? PSQLCastingError)?.postgresType, .text)
                XCTAssertEqual(($0 as? PSQLCastingError)?.columnIndex, 0)
            }
        }
    }

    func testQuery10kItems() {
        PSQLConnection.withTestConnection { connection in
            let rowSequence = try await connection.query("SELECT generate_series(1, 100000);", logger: .psqlTest)
            var received: Int64 = 0
            for try await row in rowSequence {
                var number: Int64?
                XCTAssertNoThrow(number = try row.decode(column: 0, as: Int64.self))
                received += 1
                XCTAssertEqual(number, received)
            }
            XCTAssertEqual(received, 100000)
        }
    }

    func test1kRoundTrips() {
        PSQLConnection.withTestConnection { connection in
            for _ in 0..<1_000 {
                let rows = try await connection.query("SELECT version()", logger: .psqlTest).collect()
                var version: String?
                XCTAssertNoThrow(version = try rows.first?.decode(String.self))
                XCTAssertEqual(version?.contains("PostgreSQL"), true)
            }
        }

    }

//    func testQuerySelectParameter() {
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        var stream: PSQLRowStream?
//        XCTAssertNoThrow(stream = try conn?.query("SELECT $1::TEXT as foo", ["hello"], logger: .psqlTest).wait())
//        var rows: [PSQLRow]?
//        XCTAssertNoThrow(rows = try XCTUnwrap(stream).all().wait())
//        var foo: String?
//        XCTAssertNoThrow(foo = try rows?.first?.decode(column: 0, as: String.self))
//        XCTAssertEqual(foo, "hello")
//    }
//
//    func testDecodeIntegers() {
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        var stream: PSQLRowStream?
//        XCTAssertNoThrow(stream = try conn?.query("""
//        SELECT
//            1::SMALLINT                   as smallint,
//            -32767::SMALLINT              as smallint_min,
//            32767::SMALLINT               as smallint_max,
//            1::INT                        as int,
//            -2147483647::INT              as int_min,
//            2147483647::INT               as int_max,
//            1::BIGINT                     as bigint,
//            -9223372036854775807::BIGINT  as bigint_min,
//            9223372036854775807::BIGINT   as bigint_max
//        """, logger: .psqlTest).wait())
//
//        var rows: [PSQLRow]?
//        XCTAssertNoThrow(rows = try stream?.all().wait())
//        XCTAssertEqual(rows?.count, 1)
//        let row = rows?.first
//
//        XCTAssertEqual(try row?.decode(column: "smallint", as: Int16.self), 1)
//        XCTAssertEqual(try row?.decode(column: "smallint_min", as: Int16.self), -32_767)
//        XCTAssertEqual(try row?.decode(column: "smallint_max", as: Int16.self), 32_767)
//        XCTAssertEqual(try row?.decode(column: "int", as: Int32.self), 1)
//        XCTAssertEqual(try row?.decode(column: "int_min", as: Int32.self), -2_147_483_647)
//        XCTAssertEqual(try row?.decode(column: "int_max", as: Int32.self), 2_147_483_647)
//        XCTAssertEqual(try row?.decode(column: "bigint", as: Int64.self), 1)
//        XCTAssertEqual(try row?.decode(column: "bigint_min", as: Int64.self), -9_223_372_036_854_775_807)
//        XCTAssertEqual(try row?.decode(column: "bigint_max", as: Int64.self), 9_223_372_036_854_775_807)
//    }
//
//    func testEncodeAndDecodeIntArray() {
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        var stream: PSQLRowStream?
//        let array: [Int64] = [1, 2, 3]
//        XCTAssertNoThrow(stream = try conn?.query("SELECT $1::int8[] as array", [array], logger: .psqlTest).wait())
//
//        var rows: [PSQLRow]?
//        XCTAssertNoThrow(rows = try stream?.all().wait())
//        XCTAssertEqual(rows?.count, 1)
//        XCTAssertEqual(try rows?.first?.decode(column: "array", as: [Int64].self), array)
//    }
//
//    func testDecodeEmptyIntegerArray() {
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        var stream: PSQLRowStream?
//        XCTAssertNoThrow(stream = try conn?.query("SELECT '{}'::int[] as array", logger: .psqlTest).wait())
//
//        var rows: [PSQLRow]?
//        XCTAssertNoThrow(rows = try stream?.all().wait())
//        XCTAssertEqual(rows?.count, 1)
//        XCTAssertEqual(try rows?.first?.decode(column: "array", as: [Int64].self), [])
//    }
//
//    func testDoubleArraySerialization() {
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        var stream: PSQLRowStream?
//        let doubles: [Double] = [3.14, 42]
//        XCTAssertNoThrow(stream = try conn?.query("SELECT $1::double precision[] as doubles", [doubles], logger: .psqlTest).wait())
//
//        var rows: [PSQLRow]?
//        XCTAssertNoThrow(rows = try stream?.all().wait())
//        XCTAssertEqual(rows?.count, 1)
//        XCTAssertEqual(try rows?.first?.decode(column: "doubles", as: [Double].self), doubles)
//    }
//
//    func testDecodeDates() {
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        var stream: PSQLRowStream?
//        XCTAssertNoThrow(stream = try conn?.query("""
//            SELECT
//                '2016-01-18 01:02:03 +0042'::DATE         as date,
//                '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
//                '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
//            """, logger: .psqlTest).wait())
//
//        var rows: [PSQLRow]?
//        XCTAssertNoThrow(rows = try stream?.all().wait())
//        XCTAssertEqual(rows?.count, 1)
//        let row = rows?.first
//
//        XCTAssertEqual(try row?.decode(column: "date", as: Date.self).description, "2016-01-18 00:00:00 +0000")
//        XCTAssertEqual(try row?.decode(column: "timestamp", as: Date.self).description, "2016-01-18 01:02:03 +0000")
//        XCTAssertEqual(try row?.decode(column: "timestamptz", as: Date.self).description, "2016-01-18 00:20:03 +0000")
//    }
//
//    func testDecodeUUID() {
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        var stream: PSQLRowStream?
//        XCTAssertNoThrow(stream = try conn?.query("""
//            SELECT '2c68f645-9ca6-468b-b193-ee97f241c2f8'::UUID as uuid
//            """, logger: .psqlTest).wait())
//
//        var rows: [PSQLRow]?
//        XCTAssertNoThrow(rows = try stream?.all().wait())
//        XCTAssertEqual(rows?.count, 1)
//
//        XCTAssertEqual(try rows?.first?.decode(column: "uuid", as: UUID.self), UUID(uuidString: "2c68f645-9ca6-468b-b193-ee97f241c2f8"))
//    }
//
//    func testRoundTripJSONB() {
//        struct Object: Codable, PSQLCodable {
//            let foo: Int
//            let bar: Int
//        }
//
//        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
//        let eventLoop = eventLoopGroup.next()
//
//        var conn: PSQLConnection?
//        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
//        defer { XCTAssertNoThrow(try conn?.close().wait()) }
//
//        do {
//            var stream: PSQLRowStream?
//            XCTAssertNoThrow(stream = try conn?.query("""
//                select $1::jsonb as jsonb
//                """, [Object(foo: 1, bar: 2)], logger: .psqlTest).wait())
//
//            var rows: [PSQLRow]?
//            XCTAssertNoThrow(rows = try stream?.all().wait())
//            XCTAssertEqual(rows?.count, 1)
//            var result: Object?
//            XCTAssertNoThrow(result = try rows?.first?.decode(column: "jsonb", as: Object.self))
//            XCTAssertEqual(result?.foo, 1)
//            XCTAssertEqual(result?.bar, 2)
//        }
//
//        do {
//            var stream: PSQLRowStream?
//            XCTAssertNoThrow(stream = try conn?.query("""
//                select $1::json as json
//                """, [Object(foo: 1, bar: 2)], logger: .psqlTest).wait())
//
//            var rows: [PSQLRow]?
//            XCTAssertNoThrow(rows = try stream?.all().wait())
//            XCTAssertEqual(rows?.count, 1)
//            var result: Object?
//            XCTAssertNoThrow(result = try rows?.first?.decode(column: "json", as: Object.self))
//            XCTAssertEqual(result?.foo, 1)
//            XCTAssertEqual(result?.bar, 2)
//        }
//    }
}


@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
extension PSQLConnection {
    
    static func withTestConnection(_ closure: @escaping (PSQLConnection) async throws -> ()) {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! eventLoopGroup.syncShutdownGracefully() }
        let eventLoop = eventLoopGroup.next()
        
        let dispatchGroup = DispatchGroup()
        dispatchGroup.enter()
        Task {
            defer {
                dispatchGroup.leave()
            }
            
            let conn = try await PSQLConnection.test(on: eventLoop)
            defer { Task { try await conn.close() } }
            
            do {
                try await closure(conn)
            } catch {
                XCTFail("\(error)")
            }
        }
        
        dispatchGroup.wait()
    }
    
    static func test(on eventLoop: EventLoop, logLevel: Logger.Level = .info) async throws -> PSQLConnection {
        var logger = Logger(label: "psql.connection.test")
        logger.logLevel = logLevel
        let config = PSQLConnection.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: 5432,
            username: env("POSTGRES_USER") ?? "postgres",
            database: env("POSTGRES_DB"),
            password: env("POSTGRES_PASSWORD"),
            tlsConfiguration: nil)
        
        return try await PSQLConnection.connect(configuration: config, logger: logger, on: eventLoop)
    }
    
}
#endif

#if swift(>=5.5) && canImport(_Concurrency)
// NOTE: workaround until we have async test support on linux
//         https://github.com/apple/swift-corelibs-xctest/pull/326
extension XCTestCase {
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    func XCTAsyncTest(
        expectationDescription: String = "Async operation",
        timeout: TimeInterval = 3,
        file: StaticString = #file,
        line: Int = #line,
        operation: @escaping () async throws -> Void
    ) {
        let expectation = self.expectation(description: expectationDescription)
        Task {
            do { try await operation() }
            catch {
                XCTFail("Error thrown while executing async function @ \(file):\(line): \(error)")
                Thread.callStackSymbols.forEach { print($0) }
            }
            expectation.fulfill()
        }
        self.wait(for: [expectation], timeout: timeout)
    }
}
#endif

