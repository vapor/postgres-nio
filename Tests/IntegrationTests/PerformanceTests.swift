import XCTest
import Logging
import NIOCore
import NIOPosix
@testable import PostgresNIO
import NIOTestUtils

final class PerformanceTests: XCTestCase {
    private var group: EventLoopGroup!

    private var eventLoop: EventLoop { self.group.next() }
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        try XCTSkipUnless(Self.shouldRunPerformanceTests)

        XCTAssertTrue(isLoggingConfigured)
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }
    
    override func tearDownWithError() throws {
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }
    

    // MARK: Performance
    
    func testPerformanceRangeSelectDecodePerformance() throws {
        struct Series: Decodable {
            var num: Int
        }
        
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        measure {
            do {
                for _ in 0..<5 {
                    try conn.query("SELECT * FROM generate_series(1, 10000) num") { row in
                        _ = try row.decode(Int.self, context: .default)
                    }.wait()
                }
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectTinyModel() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        try prepareTableToMeasureSelectPerformance(
            rowCount: 300_000, batchSize: 5_000,
            schema:
            """
                "int" int8,
            """,
            fixtureData: [PostgresData(int: 1234)],
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = try row.decode(Int.self, context: .default)
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectMediumModel() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let now = Date()
        let uuid = UUID()
        try prepareTableToMeasureSelectPerformance(
            rowCount: 300_000,
            schema:
            // TODO: Also add a `Double` and a `Data` field to this performance test.
            """
                "string" text,
                "int" int8,
                "date" timestamptz,
                "uuid" uuid,
            """,
            fixtureData: [
                PostgresData(string: "foo"),
                PostgresData(int: 0),
                now.postgresData!,
                PostgresData(uuid: uuid)
            ],
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") {
                    let row = $0.makeRandomAccess()
                    _ = row[data: "id"].int
                    _ = row[data: "string"].string
                    _ = row[data: "int"].int
                    _ = row[data: "date"].date
                    _ = row[data: "uuid"].uuid
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectLargeModel() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let now = Date()
        let uuid = UUID()
        try prepareTableToMeasureSelectPerformance(
            rowCount: 100_000,
            schema:
            // TODO: Also add `Double` and `Data` fields to this performance test.
            """
                "string1" text,
                "string2" text,
                "string3" text,
                "string4" text,
                "string5" text,
                "int1" int8,
                "int2" int8,
                "int3" int8,
                "int4" int8,
                "int5" int8,
                "date1" timestamptz,
                "date2" timestamptz,
                "date3" timestamptz,
                "date4" timestamptz,
                "date5" timestamptz,
                "uuid1" uuid,
                "uuid2" uuid,
                "uuid3" uuid,
                "uuid4" uuid,
                "uuid5" uuid,
            """,
            fixtureData: [
                PostgresData(string: "string1"),
                PostgresData(string: "string2"),
                PostgresData(string: "string3"),
                PostgresData(string: "string4"),
                PostgresData(string: "string5"),
                PostgresData(int: 1),
                PostgresData(int: 2),
                PostgresData(int: 3),
                PostgresData(int: 4),
                PostgresData(int: 5),
                now.postgresData!,
                now.postgresData!,
                now.postgresData!,
                now.postgresData!,
                now.postgresData!,
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid),
                PostgresData(uuid: uuid)
            ],
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") {
                    let row = $0.makeRandomAccess()
                    _ = row[data: "id"].int
                    _ = row[data: "string1"].string
                    _ = row[data: "string2"].string
                    _ = row[data: "string3"].string
                    _ = row[data: "string4"].string
                    _ = row[data: "string5"].string
                    _ = row[data: "int1"].int
                    _ = row[data: "int2"].int
                    _ = row[data: "int3"].int
                    _ = row[data: "int4"].int
                    _ = row[data: "int5"].int
                    _ = row[data: "date1"].date
                    _ = row[data: "date2"].date
                    _ = row[data: "date3"].date
                    _ = row[data: "date4"].date
                    _ = row[data: "date5"].date
                    _ = row[data: "uuid1"].uuid
                    _ = row[data: "uuid2"].uuid
                    _ = row[data: "uuid3"].uuid
                    _ = row[data: "uuid4"].uuid
                    _ = row[data: "uuid5"].uuid
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectLargeModelWithLongFieldNames() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let fieldIndices = Array(1...20)
        let fieldNames = fieldIndices.map { "veryLongFieldNameVeryLongFieldName\($0)" }
        try prepareTableToMeasureSelectPerformance(
            rowCount: 50_000, batchSize: 200,
            schema: fieldNames.map { "\"\($0)\" int8" }.joined(separator: ", ") + ",",
            fixtureData: fieldIndices.map { PostgresData(int: $0) },
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") {
                    let row = $0.makeRandomAccess()
                    _ = row[data: "id"].int
                    for fieldName in fieldNames {
                        _ = row[data: fieldName].int
                    }
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testPerformanceSelectHugeModel() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let fieldIndices = Array(1...100)
        let fieldNames = fieldIndices.map { "int\($0)" }
        try prepareTableToMeasureSelectPerformance(
            rowCount: 10_000, batchSize: 200,
            schema: fieldNames.map { "\"\($0)\" int8" }.joined(separator: ", ") + ",",
            fixtureData: fieldIndices.map { PostgresData(int: $0) },
            on: self.eventLoop
        )
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") {
                    let row = $0.makeRandomAccess()
                    _ = row[data: "id"].int
                    for fieldName in fieldNames {
                        _ = row[data: fieldName].int
                    }
                }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

}

private func prepareTableToMeasureSelectPerformance(
    rowCount: Int,
    batchSize: Int = 1_000,
    schema: String,
    fixtureData: [PostgresData],
    on eventLoop: EventLoop,
    file: StaticString = #file,
    line: UInt = #line
) throws {
    XCTAssertEqual(rowCount % batchSize, 0, "`rowCount` must be a multiple of `batchSize`", file: (file), line: line)
    let conn = try PostgresConnection.test(on: eventLoop).wait()
    defer { try! conn.close().wait() }
    
    _ = try conn.simpleQuery("DROP TABLE IF EXISTS \"measureSelectPerformance\"").wait()
    _ = try conn.simpleQuery("""
        CREATE TABLE "measureSelectPerformance" (
        "id" int8 NOT NULL,
        \(schema)
        PRIMARY KEY ("id")
        );
        """).wait()
    
    // Batch `batchSize` inserts into one for better insert performance.
    let totalArgumentsPerRow = fixtureData.count + 1
    let insertArgumentsPlaceholder = (0..<batchSize).map { indexInBatch in
        "("
            + (0..<totalArgumentsPerRow).map { argumentIndex in "$\(indexInBatch * totalArgumentsPerRow + argumentIndex + 1)" }
                .joined(separator: ", ")
            + ")"
        }.joined(separator: ", ")
    let insertQuery = "INSERT INTO \"measureSelectPerformance\" VALUES \(insertArgumentsPlaceholder)"
    var batchedFixtureData = Array(repeating: [PostgresData(int: 0)] + fixtureData, count: batchSize).flatMap { $0 }
    for batchIndex in 0..<(rowCount / batchSize) {
        for indexInBatch in 0..<batchSize {
            let rowIndex = batchIndex * batchSize + indexInBatch
            batchedFixtureData[indexInBatch * totalArgumentsPerRow] = PostgresData(int: rowIndex)
        }
        _ = try conn.query(insertQuery, batchedFixtureData).wait()
    }
}

