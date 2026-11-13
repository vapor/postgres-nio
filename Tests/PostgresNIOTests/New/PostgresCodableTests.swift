import Testing
@testable import PostgresNIO
import NIOCore

@Suite
struct PostgresCodableTests {
    @Test func decodeAnOptionalFromARow() throws {
        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: [
                RowDescription.Column(
                    name: "id",
                    tableOID: 1,
                    columnAttributeNumber: 1,
                    dataType: .text,
                    dataTypeSize: 0,
                    dataTypeModifier: 0,
                    format: .binary
                ),
                RowDescription.Column(
                    name: "id",
                    tableOID: 1,
                    columnAttributeNumber: 1,
                    dataType: .text,
                    dataTypeSize: 0,
                    dataTypeModifier: 0,
                    format: .binary
                )
            ]
        )

        let result = try row.decode((String?, String?).self, context: .default)
        #expect(result.0 == nil)
        #expect(result.1 == "Hello world!")
    }

    @Test func decodeMissingValueError() {
        let row = PostgresRow(
            data: .makeTestDataRow(nil),
            lookupTable: ["name": 0],
            columns: [
                RowDescription.Column(
                    name: "id",
                    tableOID: 1,
                    columnAttributeNumber: 1,
                    dataType: .text,
                    dataTypeSize: 0,
                    dataTypeModifier: 0,
                    format: .binary
                )
            ]
        )

        let error = #expect(throws: PostgresDecodingError.self) {
            try row.decode(String.self, context: .default)
        }
        #expect(error?.line == #line - 3)
        #expect(error?.file == #fileID)

        #expect(error?.code == .missingData)
        #expect(error?.targetType == String.self)
        #expect(error?.postgresType == .text)
    }
}
