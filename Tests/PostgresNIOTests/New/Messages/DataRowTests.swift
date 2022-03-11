import XCTest
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

class DataRowTests: XCTestCase {
    func testDecode() {
        let buffer = ByteBuffer.backendMessage(id: .dataRow) { buffer in
            // the data row has 3 columns
            buffer.writeInteger(3, as: Int16.self)
            
            // this is a null value
            buffer.writeInteger(-1, as: Int32.self)
            
            // this is an empty value. for example a empty string
            buffer.writeInteger(0, as: Int32.self)
            
            // this is a column with ten bytes
            buffer.writeInteger(10, as: Int32.self)
            buffer.writeBytes([UInt8](repeating: 5, count: 10))
        }

        let rowSlice = buffer.getSlice(at: 7, length: buffer.readableBytes - 7)!

        let expectedInOuts = [
            (buffer, [PostgresBackendMessage.dataRow(.init(columnCount: 3, bytes: rowSlice))]),
        ]

        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: expectedInOuts,
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }))
    }
    
    func testIteratingElements() {
        let dataRow = DataRow.makeTestDataRow(nil, ByteBuffer(), ByteBuffer(repeating: 5, count: 10))
        var iterator = dataRow.makeIterator()
        
        XCTAssertEqual(dataRow.count, 3)
        XCTAssertEqual(iterator.next(), .some(.none))
        XCTAssertEqual(iterator.next(), ByteBuffer())
        XCTAssertEqual(iterator.next(), ByteBuffer(repeating: 5, count: 10))
        XCTAssertEqual(iterator.next(), .none)
    }
    
    func testIndexAfterAndSubscript() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
        
        var index = dataRow.startIndex
        XCTAssertEqual(dataRow[index], .none)
        index = dataRow.index(after: index)
        XCTAssertEqual(dataRow[index], ByteBuffer())
        index = dataRow.index(after: index)
        XCTAssertEqual(dataRow[index], ByteBuffer(repeating: 5, count: 10))
        index = dataRow.index(after: index)
        XCTAssertEqual(dataRow[index], .none)
        index = dataRow.index(after: index)
        XCTAssertEqual(index, dataRow.endIndex)
    }
    
    func testIndexComparison() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
        
        let startIndex = dataRow.startIndex
        let secondIndex = dataRow.index(after: startIndex)
        
        XCTAssertLessThanOrEqual(startIndex, secondIndex)
        XCTAssertLessThan(startIndex, secondIndex)
        
        XCTAssertGreaterThanOrEqual(secondIndex, startIndex)
        XCTAssertGreaterThan(secondIndex, startIndex)
        
        XCTAssertFalse(secondIndex == startIndex)
        XCTAssertEqual(secondIndex, secondIndex)
        XCTAssertEqual(startIndex, startIndex)
    }
    
    func testColumnSubscript() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
    
        XCTAssertEqual(dataRow.count, 4)
        XCTAssertEqual(dataRow[column: 0], .none)
        XCTAssertEqual(dataRow[column: 1], ByteBuffer())
        XCTAssertEqual(dataRow[column: 2], ByteBuffer(repeating: 5, count: 10))
        XCTAssertEqual(dataRow[column: 3], .none)
    }
    
    func testWithContiguousStorageIfAvailable() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
        
        XCTAssertNil(dataRow.withContiguousStorageIfAvailable { _ in
            return XCTFail("DataRow does not have a contiguous storage")
        })
    }
}

extension DataRow: ExpressibleByArrayLiteral {
    public typealias ArrayLiteralElement = PostgresEncodable

    public init(arrayLiteral elements: PostgresEncodable...) {
        
        var buffer = ByteBuffer()
        let encodingContext = PostgresEncodingContext(jsonEncoder: JSONEncoder())
        elements.forEach { element in
            try! element.encodeRaw(into: &buffer, context: encodingContext)
        }
        
        self.init(columnCount: Int16(elements.count), bytes: buffer)
    }
    
    static func makeTestDataRow(_ buffers: ByteBuffer?...) -> DataRow {
        var bytes = ByteBuffer()
        buffers.forEach { column in
            switch column {
            case .none:
                bytes.writeInteger(Int32(-1))
            case .some(var input):
                bytes.writeInteger(Int32(input.readableBytes))
                bytes.writeBuffer(&input)
            }
        }
        
        return DataRow(columnCount: Int16(buffers.count), bytes: bytes)
    }
}

