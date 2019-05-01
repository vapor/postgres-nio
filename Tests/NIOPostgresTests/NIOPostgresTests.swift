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
        defer { try! conn.close().wait() }
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testQueryVersion() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("SELECT version()", .init()).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
        
    }
    
    func testQuerySelectParameter() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("SELECT $1::TEXT as foo", ["hello"]).wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("foo")?.string
        XCTAssertEqual(version, "hello")
    }
    
    func testSQLError() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        do {
            _ = try conn.simpleQuery("SELECT &").wait()
            XCTFail("An error should have been thrown")
        } catch let error as PostgresError {
            XCTAssertEqual(error.code, .syntax_error)
        }
    }
    
    func testSelectTypes() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let results = try conn.simpleQuery("SELECT * FROM pg_type").wait()
        XCTAssert(results.count >= 350, "Results count not large enough: \(results.count)")
    }
    
    func testSelectType() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
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
            XCTAssertEqual(results[0].column("typname")?.string, "float8")
            XCTAssertEqual(results[0].column("typnamespace")?.int, 11)
            XCTAssertEqual(results[0].column("typowner")?.int, 10)
            XCTAssertEqual(results[0].column("typlen")?.int, 8)
            #warning("TODO: finish adding columns")
        default: XCTFail("Should be only one result")
        }
    }
    
    func testIntegers() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        struct Integers: Decodable {
            let smallint: Int16
            let smallint_min: Int16
            let smallint_max: Int16
            let int: Int32
            let int_min: Int32
            let int_max: Int32
            let bigint: Int64
            let bigint_min: Int64
            let bigint_max: Int64
        }
        let results = try conn.query("""
        SELECT
            1::SMALLINT                   as smallint,
            -32767::SMALLINT              as smallint_min,
            32767::SMALLINT               as smallint_max,
            1::INT                        as int,
            -2147483647::INT              as int_min,
            2147483647::INT               as int_max,
            1::BIGINT                     as bigint,
            -9223372036854775807::BIGINT  as bigint_min,
            9223372036854775807::BIGINT   as bigint_max
        """).wait()
        switch results.count {
        case 1:
            XCTAssertEqual(results[0].column("smallint")?.int16, 1)
            XCTAssertEqual(results[0].column("smallint_min")?.int16, -32_767)
            XCTAssertEqual(results[0].column("smallint_max")?.int16, 32_767)
            XCTAssertEqual(results[0].column("int")?.int32, 1)
            XCTAssertEqual(results[0].column("int_min")?.int32, -2_147_483_647)
            XCTAssertEqual(results[0].column("int_max")?.int32, 2_147_483_647)
            XCTAssertEqual(results[0].column("bigint")?.int64, 1)
            XCTAssertEqual(results[0].column("bigint_min")?.int64, -9_223_372_036_854_775_807)
            XCTAssertEqual(results[0].column("bigint_max")?.int64, 9_223_372_036_854_775_807)
        default: XCTFail("incorrect result count")
        }
    }

    func testPi() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        struct Pi: Decodable {
            let text: String
            let numeric_string: String
            let numeric_decimal: Decimal
            let double: Double
            let float: Float
        }
        let results = try conn.query("""
        SELECT
            pi()::TEXT     as text,
            pi()::NUMERIC  as numeric_string,
            pi()::NUMERIC  as numeric_decimal,
            pi()::FLOAT8   as double,
            pi()::FLOAT4   as float
        """).wait()
        switch results.count {
        case 1:
            print(results[0])
            XCTAssertEqual(results[0].column("text")?.string, "3.14159265358979")
            XCTAssertEqual(results[0].column("numeric_string")?.string, "3.14159265358979")
            XCTAssertTrue(results[0].column("numeric_decimal")?.as(custom: Decimal.self)?.isLess(than: 3.14159265358980) ?? false)
            XCTAssertFalse(results[0].column("numeric_decimal")?.as(custom: Decimal.self)?.isLess(than: 3.14159265358978) ?? true)
            XCTAssertTrue(results[0].column("double")?.double?.description.hasPrefix("3.141592") ?? false)
            XCTAssertTrue(results[0].column("float")?.float?.description.hasPrefix("3.141592") ?? false)
        default: XCTFail("incorrect result count")
        }
    }
    
    func testUUID() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        struct Model: Decodable {
            let id: UUID
            let string: String
        }
        let results = try conn.query("""
        SELECT
            '123e4567-e89b-12d3-a456-426655440000'::UUID as id,
            '123e4567-e89b-12d3-a456-426655440000'::UUID as string
        """).wait()
        switch results.count {
        case 1:
            print(results[0])
            XCTAssertEqual(results[0].column("id")?.uuid, UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
            XCTAssertEqual(UUID(uuidString: results[0].column("id")?.string ?? ""), UUID(uuidString: "123E4567-E89B-12D3-A456-426655440000"))
        default: XCTFail("incorrect result count")
        }
    }
    
    func testDates() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        struct Dates: Decodable {
            var date: Date
            var timestamp: Date
            var timestamptz: Date
        }
        let results = try conn.query("""
        SELECT
            '2016-01-18 01:02:03 +0042'::DATE         as date,
            '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
            '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
        """).wait()
        switch results.count {
        case 1:
            print(results[0])
            XCTAssertEqual(results[0].column("date")?.date?.description, "2016-01-18 00:00:00 +0000")
            XCTAssertEqual(results[0].column("timestamp")?.date?.description, "2016-01-18 01:02:03 +0000")
            XCTAssertEqual(results[0].column("timestamptz")?.date?.description, "2016-01-18 00:20:03 +0000")
        default: XCTFail("incorrect result count")
        }
    }
    
    /// https://github.com/vapor/nio-postgres/issues/20
    func testBindInteger() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        _ = try conn.simpleQuery("drop table if exists person;").wait()
        _ = try conn.simpleQuery("create table person(id serial primary key, first_name text, last_name text);").wait()
        defer { _ = try! conn.simpleQuery("drop table person;").wait() }
        let id = PostgresData(int32: 5)
        _ = try conn.query("SELECT id, first_name, last_name FROM person WHERE id = $1", [id]).wait()
    }

    // https://github.com/vapor/nio-postgres/issues/21
    func testAverageLengthNumeric() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("select avg(length('foo')) as average_length").wait()
        guard let length = rows[0].column("average_length")?.double else {
            XCTFail("could not decode length")
            return
        }
        XCTAssertEqual(length, 3.0)
    }
    
    func testNumericParsing() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let rows = try conn.query("""
        select
            '1234.5678'::numeric as a,
            '-123.456'::numeric as b,
            '123456.789123'::numeric as c,
            '3.14159265358979'::numeric as d
        """).wait()
        XCTAssertEqual(rows[0].column("a")?.string, "1234.5678")
        XCTAssertEqual(rows[0].column("b")?.string, "-123.456")
        XCTAssertEqual(rows[0].column("c")?.string, "123456.789123")
        XCTAssertEqual(rows[0].column("d")?.string, "3.14159265358979")
    }
    
    func testNumericSerialization() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let a = PostgresNumeric(string: "123456.789123")!
        let b = PostgresNumeric(string: "-123456.789123")!
        let c = PostgresNumeric(string: "3.14159265358979")!
        let rows = try conn.query("""
        select
            $1::numeric::text as a,
            $2::numeric::text as b,
            $3::numeric::text as c
        """, [
            .init(numeric: a),
            .init(numeric: b),
            .init(numeric: c)
        ]).wait()
        XCTAssertEqual(rows[0].column("a")?.string, "123456.789123")
        XCTAssertEqual(rows[0].column("b")?.string, "-123456.789123")
        XCTAssertEqual(rows[0].column("c")?.string, "3.14159265358979")
    }
    
    func testRemoteTLSServer() throws {
        let url = "postgres://uymgphwj:7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA@elmer.db.elephantsql.com:5432/uymgphwj"
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! elg.syncShutdownGracefully() }
        
        let conn = try PostgresConnection.connect(
            to: SocketAddress.makeAddressResolvingHost("elmer.db.elephantsql.com", port: 5432),
            on: elg.next()
        ).wait()
        let upgraded = try conn.requestTLS(
            using: .forClient(certificateVerification: .none),
            serverHostname: "elmer.db.elephantsql.com"
        ).wait()
        XCTAssertTrue(upgraded)
        try! conn.authenticate(username: "uymgphwj", database: "uymgphwj", password: "7_tHbREdRwkqAdu4KoIS7hQnNxr8J1LA").wait()
        defer { try? conn.close().wait() }
        let rows = try conn.simpleQuery("SELECT version()").wait()
        XCTAssertEqual(rows.count, 1)
        let version = rows[0].column("version")?.string
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
    }
    
    func testInvalidPassword() throws {
        let conn = try PostgresConnection.testUnauthenticated(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        let auth = conn.authenticate(username: "invalid", database: "invalid", password: "bad")
        do {
            let _ = try auth.wait()
            XCTFail("The authentication should fail")
        } catch let error as PostgresError {
            XCTAssertEqual(error.code, .invalid_password)
        }
    }

    func testRangeSelectDecodePerformance() throws {
        // std deviation too high
        struct Series: Decodable {
            var num: Int
        }

        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }
        measure {
            do {
                for _ in 0..<50 {
                    try conn.query("SELECT * FROM generate_series(1, 10000) num") { row in
                        _ = row.column("num")?.int
                        }.wait()
                }
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testColumnsInJoin() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let dateInTable1 = Date(timeIntervalSince1970: 1234)
        let dateInTable2 = Date(timeIntervalSince1970: 5678)
        _ = try conn.simpleQuery("""
            CREATE TABLE table1 (
            "id" int8 NOT NULL,
            "table2_id" int8,
            "intValue" int8,
            "stringValue" text,
            "dateValue" timestamptz,
            PRIMARY KEY ("id")
            );
            """).wait()
        defer { _ = try! conn.simpleQuery("DROP TABLE \"table1\"").wait() }

        _ = try conn.simpleQuery("""
            CREATE TABLE table2 (
            "id" int8 NOT NULL,
            "intValue" int8,
            "stringValue" text,
            "dateValue" timestamptz,
            PRIMARY KEY ("id")
            );
            """).wait()
        defer { _ = try! conn.simpleQuery("DROP TABLE \"table2\"").wait() }

        _ = try conn.simpleQuery("INSERT INTO table1 VALUES (12, 34, 56, 'stringInTable1', to_timestamp(1234))").wait()
        _ = try conn.simpleQuery("INSERT INTO table2 VALUES (34, 78, 'stringInTable2', to_timestamp(5678))").wait()

        let tableNameToOID = Dictionary(uniqueKeysWithValues: try conn.simpleQuery("SELECT relname, oid FROM pg_class WHERE relname in ('table1', 'table2')")
            .wait()
            .map { row -> (String, UInt32) in (row.column("relname")!.string!, row.column("oid")!.uint32!) })

        let row = try conn.query("SELECT * FROM table1 INNER JOIN table2 ON table1.table2_id = table2.id").wait().first!
        XCTAssertEqual(12, row.column("id", tableOID: tableNameToOID["table1"]!)?.int)
        XCTAssertEqual(34, row.column("table2_id", tableOID: tableNameToOID["table1"]!)?.int)
        XCTAssertEqual(56, row.column("intValue", tableOID: tableNameToOID["table1"]!)?.int)
        XCTAssertEqual("stringInTable1", row.column("stringValue", tableOID: tableNameToOID["table1"]!)?.string)
        XCTAssertEqual(dateInTable1, row.column("dateValue", tableOID: tableNameToOID["table1"]!)?.date)
        XCTAssertEqual(34, row.column("id", tableOID: tableNameToOID["table2"]!)?.int)
        XCTAssertEqual(78, row.column("intValue", tableOID: tableNameToOID["table2"]!)?.int)
        XCTAssertEqual("stringInTable2", row.column("stringValue", tableOID: tableNameToOID["table2"]!)?.string, "stringInTable2")
        XCTAssertEqual(dateInTable2, row.column("dateValue", tableOID: tableNameToOID["table2"]!)?.date)
    }

    private func prepareTableToMeasureSelectPerformance(
        rowCount: Int, batchSize: Int = 1_000, schema: String, fixtureData: [PostgresData],
        file: StaticString = #file, line: UInt = #line) throws {
        XCTAssertEqual(rowCount % batchSize, 0, "`rowCount` must be a multiple of `batchSize`", file: file, line: line)
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

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

    func testSelectTinyModel() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let now = Date()
        let uuid = UUID()
        try prepareTableToMeasureSelectPerformance(
            rowCount: 300_000, batchSize: 5_000,
            schema:
            """
                "int" int8,
            """,
            fixtureData: [PostgresData(int: 1234)])
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("int")?.int
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testSelectMediumModel() throws {
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
            fixtureData: [PostgresData(string: "foo"), PostgresData(int: 0),
                          now.postgresData!, PostgresData(uuid: uuid)])
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    _ = row.column("string")?.string
                    _ = row.column("int")?.int
                    _ = row.column("date")?.date
                    _ = row.column("uuid")?.uuid
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testSelectLargeModel() throws {
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
            fixtureData: [PostgresData(string: "string1"), PostgresData(string: "string2"), PostgresData(string: "string3"), PostgresData(string: "string4"), PostgresData(string: "string5"),
                          PostgresData(int: 1), PostgresData(int: 2), PostgresData(int: 3), PostgresData(int: 4), PostgresData(int: 5),
                          now.postgresData!, now.postgresData!, now.postgresData!, now.postgresData!, now.postgresData!,
                          PostgresData(uuid: uuid), PostgresData(uuid: uuid), PostgresData(uuid: uuid), PostgresData(uuid: uuid), PostgresData(uuid: uuid)])
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    _ = row.column("string1")?.string
                    _ = row.column("string2")?.string
                    _ = row.column("string3")?.string
                    _ = row.column("string4")?.string
                    _ = row.column("string5")?.string
                    _ = row.column("int1")?.int
                    _ = row.column("int2")?.int
                    _ = row.column("int3")?.int
                    _ = row.column("int4")?.int
                    _ = row.column("int5")?.int
                    _ = row.column("date1")?.date
                    _ = row.column("date2")?.date
                    _ = row.column("date3")?.date
                    _ = row.column("date4")?.date
                    _ = row.column("date5")?.date
                    _ = row.column("uuid1")?.uuid
                    _ = row.column("uuid2")?.uuid
                    _ = row.column("uuid3")?.uuid
                    _ = row.column("uuid4")?.uuid
                    _ = row.column("uuid5")?.uuid
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testSelectLargeModelWithLongFieldNames() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let fieldIndices = Array(1...20)
        let fieldNames = fieldIndices.map { "veryLongFieldNameVeryLongFieldName\($0)" }
        try prepareTableToMeasureSelectPerformance(
            rowCount: 50_000, batchSize: 200,
            schema: fieldNames.map { "\"\($0)\" int8" }.joined(separator: ", ") + ",",
            fixtureData: fieldIndices.map { PostgresData(int: $0) })
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    for fieldName in fieldNames {
                        _ = row.column(fieldName)?.int
                    }
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }

    func testSelectHugeModel() throws {
        let conn = try PostgresConnection.test(on: eventLoop).wait()
        defer { try! conn.close().wait() }

        let fieldIndices = Array(1...100)
        let fieldNames = fieldIndices.map { "int\($0)" }
        try prepareTableToMeasureSelectPerformance(
            rowCount: 10_000, batchSize: 200,
            schema: fieldNames.map { "\"\($0)\" int8" }.joined(separator: ", ") + ",",
            fixtureData: fieldIndices.map { PostgresData(int: $0) })
        defer { _ = try! conn.simpleQuery("DROP TABLE \"measureSelectPerformance\"").wait() }

        measure {
            do {
                try conn.query("SELECT * FROM \"measureSelectPerformance\"") { row in
                    _ = row.column("id")?.int
                    for fieldName in fieldNames {
                        _ = row.column(fieldName)?.int
                    }
                    }.wait()
            } catch {
                XCTFail("\(error)")
            }
        }
    }
}
