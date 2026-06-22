@testable import PostgresNIO
import Testing
import NIOCore

@Suite struct PostgresCellTests {
    @Test func testDecodingANonOptionalString() {
        let cell = PostgresCell(
            bytes: ByteBuffer(string: "Hello world"),
            dataType: .text,
            format: .binary,
            columnName: "hello",
            columnIndex: 1
        )

        var result: String?
        #expect(throws: Never.self) {
            result = try cell.decode(String.self, context: .default)
        }
        #expect(result == "Hello world")
    }

    @Test func testDecodingAnOptionalString() {
        let cell = PostgresCell(
            bytes: nil,
            dataType: .text,
            format: .binary,
            columnName: "hello",
            columnIndex: 1
        )

        var result: String? = "test"
        #expect(throws: Never.self) {
            result = try cell.decode(String?.self, context: .default)
        }
        #expect(result == nil)
    }

    @Test func testDecodingFailure() {
        let cell = PostgresCell(
            bytes: ByteBuffer(string: "Hello world"),
            dataType: .text,
            format: .binary,
            columnName: "hello",
            columnIndex: 1
        )

        let error = #expect(throws: PostgresDecodingError.self) {
            try cell.decode(Int?.self, context: .default)
        }
        guard let error else {
            Issue.record("Expected error at this point")
            return
        }

        #expect(error.file == #fileID)
        #expect(error.line == #line - 9)
        #expect(error.code == .typeMismatch)
        #expect(error.columnName == "hello")
        #expect(error.columnIndex == 1)
        let correctType = error.targetType == Int?.self
        #expect(correctType)
        #expect(error.postgresType == .text)
        #expect(error.postgresFormat == .binary)
    }
}
