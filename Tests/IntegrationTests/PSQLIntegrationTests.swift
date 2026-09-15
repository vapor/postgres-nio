import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import Testing

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

@Suite(.serialized)
struct IntegrationTests {

    @Test func connectAndClose() async throws {
        let conn = try await PostgresConnection.test(on: MultiThreadedEventLoopGroup.singleton.any())
        try await conn.close()
    }

    // If the postgres server trusts every connection, it is really hard to create an
    // authentication failure.
    @Test(.disabled(if: env("POSTGRES_HOST_AUTH_METHOD") == "trust"))
    func authenticationFailure() async throws {
        let config = PostgresConnection.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: env("POSTGRES_PORT").flatMap(Int.init(_:)) ?? 5432,
            username: env("POSTGRES_USER") ?? "test_username",
            password: "wrong_password",
            database: env("POSTGRES_DB") ?? "test_database",
            tls: .disable
        )

        var logger = Logger.psqlTest
        logger.logLevel = .info

        await #expect(throws: PSQLError.self) {
            let connection = try await PostgresConnection.connect(
                on: MultiThreadedEventLoopGroup.singleton.any(), configuration: config, id: 1,
                logger: logger
            )
            // In case of a test failure the created connection must be closed.
            try await connection.close()
        }
    }

    @Test func queryVersion() async throws {
        try await withConnection { connection in
            let rows = try await connection.query("SELECT version()", logger: .psqlTest).collect()
            let version = try #require(rows.first).decode(String.self, context: .default)
            #expect(version.contains("PostgreSQL"))
        }
    }

    @Test func query10kItems() async throws {
        try await withConnection { connection in
            var expected: Int64 = 0
            for try await row in try await connection.query("SELECT generate_series(1, 10000);", logger: .psqlTest) {
                expected += 1
                #expect(try row.decode(Int64.self, context: .default) == expected)
            }
            #expect(expected == 10000)
        }
    }

    @Test func oneThousandRoundTrips() async throws {
        try await withConnection { connection in
            for _ in 0..<1_000 {
                let rows = try await connection.query("SELECT version()", logger: .psqlTest).collect()
                let version = try #require(rows.first).decode(String.self, context: .default)
                #expect(version.contains("PostgreSQL"))
            }
        }
    }

    @Test func querySelectParameter() async throws {
        try await withConnection { connection in
            let rows = try await connection.query("SELECT \("hello")::TEXT as foo", logger: .psqlTest).collect()
            let foo = try #require(rows.first).decode(String.self, context: .default)
            #expect(foo == "hello")
        }
    }

    @Test func queryNothing() async throws {
        try await withConnection { connection in
            let result = try await connection.query(
                """
                -- Some comments
                """, logger: .psqlTest
            ).get()

            #expect(result.rows == [])
            #expect(result.metadata.command == "")
        }
    }

    @Test func decodeIntegers() async throws {
        try await withConnection { connection in
            let rows = try await connection.query(
                """
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
                """, logger: .psqlTest
            ).collect()

            #expect(rows.count == 1)
            let cells = try #require(rows.first).decode(
                (Int16, Int16, Int16, Int32, Int32, Int32, Int64, Int64, Int64).self,
                context: .default
            )

            #expect(cells.0 == 1)
            #expect(cells.1 == -32_767)
            #expect(cells.2 == 32_767)
            #expect(cells.3 == 1)
            #expect(cells.4 == -2_147_483_647)
            #expect(cells.5 == 2_147_483_647)
            #expect(cells.6 == 1)
            #expect(cells.7 == -9_223_372_036_854_775_807)
            #expect(cells.8 == 9_223_372_036_854_775_807)
        }
    }

    @Test func encodeAndDecodeIntArray() async throws {
        try await withConnection { connection in
            let array: [Int64] = [1, 2, 3]
            let rows = try await connection.query(
                "SELECT \(array)::int8[] as array", logger: .psqlTest
            ).collect()
            #expect(rows.count == 1)
            #expect(try #require(rows.first).decode([Int64].self, context: .default) == array)
        }
    }

    @Test func decodeEmptyIntegerArray() async throws {
        try await withConnection { connection in
            let rows = try await connection.query(
                "SELECT '{}'::int[] as array", logger: .psqlTest
            ).collect()
            #expect(rows.count == 1)
            #expect(try #require(rows.first).decode([Int64].self, context: .default) == [])
        }
    }

    @Test func doubleArraySerialization() async throws {
        try await withConnection { connection in
            let doubles: [Double] = [3.14, 42]
            let rows = try await connection.query(
                "SELECT \(doubles)::double precision[] as doubles", logger: .psqlTest
            ).collect()
            #expect(rows.count == 1)
            #expect(try #require(rows.first).decode([Double].self, context: .default) == doubles)
        }
    }

    @Test func decodeDates() async throws {
        try await withConnection { connection in
            let rows = try await connection.query(
                """
                SELECT
                    '2016-01-18 01:02:03 +0042'::DATE         as date,
                    '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
                    '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
                """, logger: .psqlTest
            ).collect()

            #expect(rows.count == 1)
            let cells = try #require(rows.first).decode((Date, Date, Date).self, context: .default)

            #expect(cells.0.description == "2016-01-18 00:00:00 +0000")
            #expect(cells.1.description == "2016-01-18 01:02:03 +0000")
            #expect(cells.2.description == "2016-01-18 00:20:03 +0000")
        }
    }

    @Test func decodeDecimals() async throws {
        try await withConnection { connection in
            let rows = try await connection.query(
                """
                SELECT
                    \(Decimal(string: "123456.789123")!)::numeric     as numeric,
                    \(Decimal(string: "-123456.789123")!)::numeric    as numeric_negative
                """, logger: .psqlTest
            ).collect()

            #expect(rows.count == 1)
            let cells = try #require(rows.first).decode((Decimal, Decimal).self, context: .default)

            #expect(cells.0 == Decimal(string: "123456.789123"))
            #expect(cells.1 == Decimal(string: "-123456.789123"))
        }
    }

    @Test func decodeRawRepresentables() async throws {
        enum StringRR: String, PostgresDecodable {
            case a
        }

        enum IntRR: Int, PostgresDecodable {
            case b
        }

        let stringValue = StringRR.a
        let intValue = IntRR.b

        try await withConnection { connection in
            let rows = try await connection.query(
                """
                SELECT
                    \(stringValue.rawValue)::varchar     as string,
                    \(intValue.rawValue)::int8           as int
                """, logger: .psqlTest
            ).collect()

            #expect(rows.count == 1)
            let cells = try #require(rows.first).decode((StringRR, IntRR).self, context: .default)

            #expect(cells.0 == stringValue)
            #expect(cells.1 == intValue)
        }
    }

    @Test func roundTripUUID() async throws {
        try await withConnection { connection in
            let uuidString = "2c68f645-9ca6-468b-b193-ee97f241c2f8"
            let rows = try await connection.query(
                """
                SELECT \(uuidString)::UUID as uuid
                """,
                logger: .psqlTest
            ).collect()

            #expect(rows.count == 1)
            #expect(try #require(rows.first).decode(UUID.self, context: .default) == UUID(uuidString: uuidString))
        }
    }

    @Test(arguments: ["jsonb", "json"])
    func roundTripJSON(type: String) async throws {
        struct Object: Codable, PostgresCodable {
            let foo: Int
            let bar: Int
        }

        try await withConnection { connection in
            let rows = try await connection.query(
                """
                select \(Object(foo: 1, bar: 2))::\(unescaped: type) as \(unescaped: type)
                """, logger: .psqlTest
            ).collect()

            #expect(rows.count == 1)
            let obj = try #require(rows.first).decode(Object.self, context: .default)
            #expect(obj.foo == 1)
            #expect(obj.bar == 2)
        }
    }

    /// Creates an empty `copy_table` for the COPY tests.
    private func createCopyTable(on connection: PostgresConnection) async throws {
        _ = try? await connection.query("DROP TABLE copy_table", logger: .psqlTest)
        try await connection.query(
            "CREATE TABLE copy_table (id INT, name VARCHAR(100))", logger: .psqlTest
        )
    }

    @Test func copyIntoFrom() async throws {
        try await withConnection { connection in
            try await self.createCopyTable(on: connection)

            var options = PostgresCopyFromFormat.TextOptions()
            options.delimiter = ","
            try await connection.copyFrom(
                table: "copy_table", columns: ["id", "name"], format: .text(options),
                logger: .psqlTest
            ) { writer in
                let records: [(id: Int, name: String)] = [
                    (1, "Alice"),
                    (42, "Bob"),
                ]
                for record in records {
                    var buffer = ByteBuffer()
                    buffer.writeString("\(record.id),\(record.name)\n")
                    try await writer.write(buffer)
                }
            }
            let rows = try await connection.query("SELECT id, name FROM copy_table", logger: .psqlTest)
                .collect().map { try $0.decode((Int, String).self) }
            try #require(rows.count == 2)
            #expect(rows[0].0 == 1)
            #expect(rows[0].1 == "Alice")
            #expect(rows[1].0 == 42)
            #expect(rows[1].1 == "Bob")
        }
    }

    @Test func copyIntoFromCSV() async throws {
        try await withConnection { connection in
            _ = try? await connection.query("DROP TABLE copy_table", logger: .psqlTest)
            _ = try await connection.query("CREATE TABLE copy_table (id INT, name VARCHAR(100))", logger: .psqlTest)

            var options = PostgresCopyFromFormat.CSVOptions()
            options.delimiter = ";"
            options.quote = "'"
            options.escape = "\\"
            options.header = .bool(true)
            try await connection.copyFrom(table: "copy_table", columns: ["id", "name"], format: .csv(options), logger: .psqlTest) { writer in
                var buffer = ByteBuffer()
                // Header line, skipped by the backend because of `HEADER true`.
                buffer.writeString("id;name\n")
                // Quoted value containing the delimiter.
                buffer.writeString("1;'Alice; Jr.'\n")
                // Quoted value containing the quote character, escaped with the custom escape character.
                buffer.writeString("42;'Bob \\'The Builder\\''\n")
                // Unquoted empty value is NULL in CSV format.
                buffer.writeString("7;\n")
                try await writer.write(buffer)
            }
            let rows = try await connection.query("SELECT id, name FROM copy_table ORDER BY id").get().rows.map {
                try $0.decode((Int, String?).self)
            }
            guard rows.count == 3 else {
                Issue.record("Expected 3 rows, received \(rows.count)")
                return
            }
            #expect(rows[0].0 == 1)
            #expect(rows[0].1 == "Alice; Jr.")
            #expect(rows[1].0 == 7)
            #expect(rows[1].1 == nil)
            #expect(rows[2].0 == 42)
            #expect(rows[2].1 == "Bob 'The Builder'")
        }
    }

    @Test func copyIntoFromIsTerminatedByThrowingErrorFromClosure() async throws {
        struct MyError: Error, CustomStringConvertible {
            var description: String { "My error" }
        }

        try await withConnection { connection in
            try await self.createCopyTable(on: connection)

            await #expect(throws: MyError.self) {
                try await connection.copyFrom(
                    table: "copy_table", columns: ["id", "name"], logger: .psqlTest
                ) { writer in
                    throw MyError()
                }
            }
        }
    }

    @Test func copyIntoFromHasBadFormat() async throws {
        try await withConnection { connection in
            try await self.createCopyTable(on: connection)

            let error = await #expect(throws: PSQLError.self) {
                try await connection.copyFrom(
                    table: "copy_table", columns: ["id", "name"], logger: .psqlTest
                ) { writer in
                    try await writer.write(ByteBuffer(staticString: "1Alice\n"))
                }
            }
            #expect(error?.serverInfo?[.sqlState] == "22P02")  // invalid_text_representation
        }
    }

    @Test func syntaxErrorInGeneratedQuery() async throws {
        try await withConnection { connection in
            let error = await #expect(throws: PSQLError.self) {
                // Use some form of input that generates an invalid query, the exact manner of its invalidness doesn't matter
                try await connection.copyFrom(table: "", logger: .psqlTest) { writer in
                    Issue.record("Did not expect to call writeData")
                }
            }
            #expect(error?.serverInfo?[.sqlState] == "42601")  // scanner_yyerror
        }
    }

    #if compiler(>=6.2)  // copyFromBinary is only available in Swift 6.2+
        @Test func copyFromBinary() async throws {
            try await withConnection { connection in
                try await self.createCopyTable(on: connection)

                try await connection.copyFromBinary(
                    table: "copy_table", columns: ["id", "name"], logger: .psqlTest
                ) { writer in
                    let records: [(id: Int, name: String)] = [
                        (1, "Alice"),
                        (42, "Bob"),
                    ]
                    for record in records {
                        try await writer.writeRow { columnWriter in
                            try columnWriter.writeColumn(Int32(record.id))
                            try columnWriter.writeColumn(record.name)
                        }
                    }
                }
                let rows = try await connection.query("SELECT id, name FROM copy_table", logger: .psqlTest)
                    .collect().map { try $0.decode((Int, String).self) }
                try #require(rows.count == 2)
                #expect(rows[0].0 == 1)
                #expect(rows[0].1 == "Alice")
                #expect(rows[1].0 == 42)
                #expect(rows[1].1 == "Bob")
            }
        }
    #endif
}
