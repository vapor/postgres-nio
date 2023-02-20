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
}
