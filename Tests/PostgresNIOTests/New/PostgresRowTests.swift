import XCTest
@testable import PostgresNIO
import NIOCore

final class PostgresRowTests: XCTestCase {

    func testSequence() {
        let rowDescription = [
            RowDescription.Column(
                name: "id",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .uuid,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            ),
            RowDescription.Column(
                name: "name",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .text,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            )
        ]

        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: rowDescription
        )

        XCTAssertEqual(row.count, 2)
        var iterator = row.makeIterator()

        XCTAssertEqual(iterator.next(), PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        XCTAssertEqual(iterator.next(), PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))
        XCTAssertNil(iterator.next())
    }

    func testCollection() {
        let rowDescription = [
            RowDescription.Column(
                name: "id",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .uuid,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            ),
            RowDescription.Column(
                name: "name",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .text,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            )
        ]

        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: rowDescription
        )

        XCTAssertEqual(row.count, 2)
        let startIndex = row.startIndex
        let secondIndex = row.index(after: startIndex)
        let endIndex = row.index(after: secondIndex)
        XCTAssertLessThan(startIndex, secondIndex)
        XCTAssertLessThan(secondIndex, endIndex)
        XCTAssertEqual(endIndex, row.endIndex)

        XCTAssertEqual(row[startIndex], PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        XCTAssertEqual(row[secondIndex], PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))
    }

    func testRandomAccessRow() {
        let rowDescription = [
            RowDescription.Column(
                name: "id",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .uuid,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            ),
            RowDescription.Column(
                name: "name",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .text,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            )
        ]

        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: rowDescription
        )

        let randomAccessRow = row.makeRandomAccess()

        XCTAssertEqual(randomAccessRow.count, 2)
        let startIndex = randomAccessRow.startIndex
        let endIndex = randomAccessRow.endIndex
        XCTAssertEqual(startIndex, 0)
        XCTAssertEqual(endIndex, 2)

        XCTAssertEqual(randomAccessRow[0], PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        XCTAssertEqual(randomAccessRow[1], PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))

        XCTAssertEqual(randomAccessRow["id"], PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        XCTAssertEqual(randomAccessRow["name"], PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))
    }

    func testDecoding() {
        let rowDescription = [
            RowDescription.Column(
                name: "id",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .uuid,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            ),
            RowDescription.Column(
                name: "name",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .text,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            )
        ]

        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: rowDescription
        )

        var result: (UUID?, String)?
        XCTAssertNoThrow(result = try row.decode((UUID?, String).self))
        XCTAssertEqual(result?.0, .some(.none))
        XCTAssertEqual(result?.1, "Hello world!")
    }

    func testDecodingTypeMismatch() {
        let rowDescription = [
            RowDescription.Column(
                name: "id",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .uuid,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            ),
            RowDescription.Column(
                name: "name",
                tableOID: 1,
                columnAttributeNumber: 1,
                dataType: .int8,
                dataTypeSize: 0,
                dataTypeModifier: 0,
                format: .binary
            )
        ]

        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(integer: 123)),
            lookupTable: ["id": 0, "name": 1],
            columns: rowDescription
        )

        XCTAssertThrowsError(try row.decode((UUID?, String).self)) { error in
            guard let psqlError = error as? PostgresDecodingError else { return XCTFail("Unexpected error type") }

            XCTAssertEqual(psqlError.columnName, "name")
            XCTAssertEqual(psqlError.columnIndex, 1)
            XCTAssertEqual(psqlError.line, #line - 5)
            XCTAssertEqual(psqlError.file, #file)
            XCTAssertEqual(psqlError.postgresData, ByteBuffer(integer: 123))
            XCTAssertEqual(psqlError.postgresFormat, .binary)
            XCTAssertEqual(psqlError.postgresType, .int8)
            XCTAssert(psqlError.targetType == String.self)
        }
    }
}
