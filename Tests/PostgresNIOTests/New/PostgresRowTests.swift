@testable import PostgresNIO
import Foundation
import Testing
import NIOCore

@Suite struct PostgresRowTests {
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

    @Test func testSequence() {
        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: self.rowDescription
        )

        #expect(row.count == 2)
        var iterator = row.makeIterator()

        #expect(iterator.next() == PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        #expect(iterator.next() == PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))
        #expect(iterator.next() == nil)
    }

    @Test func testCollection() {
        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: self.rowDescription
        )

        #expect(row.count == 2)
        let startIndex = row.startIndex
        let secondIndex = row.index(after: startIndex)
        let endIndex = row.index(after: secondIndex)
        #expect(startIndex < secondIndex)
        #expect(secondIndex < endIndex)
        #expect(endIndex == row.endIndex)

        #expect(row[startIndex] == PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        #expect(row[secondIndex] == PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))
    }

    @Test func testRandomAccessRow() {
        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: self.rowDescription
        )

        let randomAccessRow = row.makeRandomAccess()

        #expect(randomAccessRow.count == 2)
        #expect(randomAccessRow.startIndex == 0)
        #expect(randomAccessRow.endIndex == 2)

        #expect(randomAccessRow[0] == PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        #expect(randomAccessRow[1] == PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))

        #expect(randomAccessRow["id"] == PostgresCell(bytes: nil, dataType: .uuid, format: .binary, columnName: "id", columnIndex: 0))
        #expect(randomAccessRow["name"] == PostgresCell(bytes: ByteBuffer(string: "Hello world!"), dataType: .text, format: .binary, columnName: "name", columnIndex: 1))
    }

    @Test func testDecoding() throws {
        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(string: "Hello world!")),
            lookupTable: ["id": 0, "name": 1],
            columns: self.rowDescription
        )

        let result = try row.decode((UUID?, String).self)
        #expect(result.0 == .none)
        #expect(result.1 == "Hello world!")
    }

    @Test func testDecodingTypeMismatch() throws {
        let row = PostgresRow(
            data: .makeTestDataRow(nil, ByteBuffer(integer: 123)),
            lookupTable: ["id": 0, "name": 1],
            columns: self.rowDescription
        )

        let error = #expect(throws: PostgresDecodingError.self) {
            try row.decode((UUID?, Int).self)
        }
        guard let error else {
            Issue.record("Expected error at this point")
            return
        }

        #expect(error.columnName == "name")
        #expect(error.columnIndex == 1)
        #expect(error.line == #line - 10)
        #expect(error.file == #fileID)
        #expect(error.postgresData == ByteBuffer(integer: 123))
        #expect(error.postgresFormat == .binary)
        #expect(error.postgresType == .text)
        let correctType = error.targetType == Int.self
        #expect(correctType)
    }
}
