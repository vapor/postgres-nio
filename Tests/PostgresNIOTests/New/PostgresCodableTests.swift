import XCTest
@testable import PostgresNIO

final class PostgresCodableTests: XCTestCase {

    func testDecodeAnOptionalFromARow() {
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

        var result: (String?, String?)
        XCTAssertNoThrow(result = try row.decode((String?, String?).self, context: .default))
        XCTAssertNil(result.0)
        XCTAssertEqual(result.1, "Hello world!")
    }

    func testDecodeMissingValueError() {
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

        XCTAssertThrowsError(try row.decode(String.self, context: .default)) {
            XCTAssertEqual(($0 as? PostgresCastingError)?.line, #line - 1)
            XCTAssertEqual(($0 as? PostgresCastingError)?.file, #file)

            XCTAssertEqual(($0 as? PostgresCastingError)?.code, .missingData)
            XCTAssert(($0 as? PostgresCastingError)?.targetType == String.self)
            XCTAssertEqual(($0 as? PostgresCastingError)?.postgresType, .text)
        }
    }
}
