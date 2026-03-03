import Foundation
import Testing
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

@Suite struct DataRowTests {
    @Test func testDecode() {
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

        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: expectedInOuts,
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
            )
        }
    }
    
    @Test func testIteratingElements() {
        let dataRow = DataRow.makeTestDataRow(nil, ByteBuffer(), ByteBuffer(repeating: 5, count: 10))
        var iterator = dataRow.makeIterator()
        
        #expect(dataRow.count == 3)
        #expect(iterator.next() == .some(.none))
        #expect(iterator.next() == ByteBuffer())
        #expect(iterator.next() == ByteBuffer(repeating: 5, count: 10))
        #expect(iterator.next() == .none)
    }
    
    @Test func testIndexAfterAndSubscript() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
        
        var index = dataRow.startIndex
        #expect(dataRow[index] == .none)
        index = dataRow.index(after: index)
        #expect(dataRow[index] == ByteBuffer())
        index = dataRow.index(after: index)
        #expect(dataRow[index] == ByteBuffer(repeating: 5, count: 10))
        index = dataRow.index(after: index)
        #expect(dataRow[index] == .none)
        index = dataRow.index(after: index)
        #expect(index == dataRow.endIndex)
    }
    
    @Test func testIndexComparison() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
        
        let startIndex = dataRow.startIndex
        let secondIndex = dataRow.index(after: startIndex)
        
        #expect(startIndex <= secondIndex)
        #expect(startIndex < secondIndex)

        #expect(secondIndex >= startIndex)
        #expect(secondIndex > startIndex)

        #expect(secondIndex != startIndex)
        #expect(secondIndex == secondIndex)
        #expect(startIndex == startIndex)
    }
    
    @Test func testColumnSubscript() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
    
        #expect(dataRow.count == 4)
        #expect(dataRow[column: 0] == .none)
        #expect(dataRow[column: 1] == ByteBuffer())
        #expect(dataRow[column: 2] == ByteBuffer(repeating: 5, count: 10))
        #expect(dataRow[column: 3] == .none)
    }
    
    @Test func testWithContiguousStorageIfAvailable() {
        let dataRow = DataRow.makeTestDataRow(
            nil,
            ByteBuffer(),
            ByteBuffer(repeating: 5, count: 10),
            nil
        )
        
        #expect(dataRow.withContiguousStorageIfAvailable { _ in
            Issue.record("DataRow does not have a contiguous storage")
        } == nil)
    }
}

extension PostgresNIO.DataRow: Swift.ExpressibleByArrayLiteral {
    public typealias ArrayLiteralElement = PostgresEncodable

    public init(arrayLiteral elements: (any PostgresEncodable)...) {
        
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

