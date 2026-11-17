import NIOCore
import Testing

@testable import PostgresNIO

@Suite
struct PSQLErrorTests {
    @Test func postgresBindingsDescription() {
        let testBinds1 = PostgresBindings(capacity: 0)
        var testBinds2 = PostgresBindings(capacity: 1)
        var testBinds3 = PostgresBindings(capacity: 2)
        testBinds2.append(1, context: .default)
        testBinds3.appendUnprotected(1, context: .default)
        testBinds3.appendUnprotected("foo", context: .default)
        testBinds3.append("secret", context: .default)

        #expect(String(describing: testBinds1) == "[]")
        #expect(String(reflecting: testBinds1) == "[]")
        #expect(String(describing: testBinds2) == "[****]")
        #expect(String(reflecting: testBinds2) == "[(****; BIGINT; format: binary)]")
        #expect(String(describing: testBinds3) == #"[1, "foo", ****]"#)
        #expect(
            String(reflecting: testBinds3) == #"[(1; BIGINT; format: binary), ("foo"; TEXT; format: binary), (****; TEXT; format: binary)]"#
        )
    }

    @Test func postgresQueryDescription() {
        let testBinds1 = PostgresBindings(capacity: 0)
        var testBinds2 = PostgresBindings(capacity: 1)
        testBinds2.append(1, context: .default)
        let testQuery1 = PostgresQuery(unsafeSQL: "TEST QUERY")
        let testQuery2 = PostgresQuery(unsafeSQL: "TEST QUERY", binds: testBinds1)
        let testQuery3 = PostgresQuery(unsafeSQL: "TEST QUERY", binds: testBinds2)

        #expect(String(describing: testQuery1) == "TEST QUERY []")
        #expect(String(reflecting: testQuery1) == "PostgresQuery(sql: TEST QUERY, binds: [])")
        #expect(String(describing: testQuery2) == "TEST QUERY []")
        #expect(String(reflecting: testQuery2) == "PostgresQuery(sql: TEST QUERY, binds: [])")
        #expect(String(describing: testQuery3) == "TEST QUERY [****]")
        #expect(String(reflecting: testQuery3) == "PostgresQuery(sql: TEST QUERY, binds: [(****; BIGINT; format: binary)])")
    }

    @Test func psqlErrorDescription() {
        var error1 = PSQLError.server(
            .init(fields: [
                .localizedSeverity: "ERROR", .severity: "ERROR", .sqlState: "00000", .message: "Test message", .detail: "More test message",
                .hint: "It's a test, that's your hint", .position: "1", .schemaName: "testsch", .tableName: "testtab",
                .columnName: "testcol", .dataTypeName: "testtyp", .constraintName: "testcon", .file: #fileID, .line: "0",
                .routine: #function,
            ]))
        var testBinds = PostgresBindings(capacity: 1)
        testBinds.append(1, context: .default)
        error1.query = .init(unsafeSQL: "TEST QUERY", binds: testBinds)

        #expect(
            String(describing: error1) == """
                PSQLError – Generic description to prevent accidental leakage of sensitive data. For debugging details, use `String(reflecting: error)`.
                """)
        #expect(
            String(reflecting: error1) == """
                PSQLError(code: server, serverInfo: [sqlState: 00000, detail: More test message, file: PostgresNIOTests/PostgresErrorTests.swift, hint: It's a test, that's your hint, line: 0, message: Test message, position: 1, routine: psqlErrorDescription(), localizedSeverity: ERROR, severity: ERROR, columnName: testcol, dataTypeName: testtyp, constraintName: testcon, schemaName: testsch, tableName: testtab], query: PostgresQuery(sql: TEST QUERY, binds: [(****; BIGINT; format: binary)]))
                """)
    }
}

@Suite
struct PostgresDecodingErrorTests {
    @Test func postgresDecodingErrorEquality() {
        let error1 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: String.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        let error2 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: Int.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        #expect(error1 != error2)
        let error3 = error1
        #expect(error1 == error3)
    }

    @Test func postgresDecodingErrorDescription() {
        let error1 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: String.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        let error2 = PostgresDecodingError(
            code: .missingData,
            columnName: "column",
            columnIndex: 0,
            targetType: [[String: String]].self,
            postgresType: .jsonbArray,
            postgresFormat: .binary,
            postgresData: nil,
            file: "bar.swift",
            line: 123
        )

        // Plain description
        #expect(
            String(describing: error1) == """
                PostgresDecodingError – Generic description to prevent accidental leakage of sensitive data. For debugging details, use `String(reflecting: error)`.
                """)
        #expect(
            String(describing: error2) == """
                PostgresDecodingError – Generic description to prevent accidental leakage of sensitive data. For debugging details, use `String(reflecting: error)`.
                """)

        // Extended debugDescription
        #expect(
            String(reflecting: error1) == """
                PostgresDecodingError(code: typeMismatch,\
                 columnName: "column", columnIndex: 0,\
                 targetType: Swift.String,\
                 postgresType: TEXT, postgresFormat: binary,\
                 postgresData: \(error1.postgresData?.debugDescription ?? "nil"),\
                 file: foo.swift, line: 123\
                )
                """)
        #expect(
            String(reflecting: error2) == """
                PostgresDecodingError(code: missingData,\
                 columnName: "column", columnIndex: 0,\
                 targetType: Swift.Array<Swift.Dictionary<Swift.String, Swift.String>>,\
                 postgresType: JSONB[], postgresFormat: binary,\
                 file: bar.swift, line: 123\
                )
                """)
    }
}
