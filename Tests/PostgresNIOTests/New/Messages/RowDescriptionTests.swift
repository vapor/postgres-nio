import XCTest
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

class RowDescriptionTests: XCTestCase {
    
    func testDecode() {
        let columns: [RowDescription.Column] = [
            .init(name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary),
            .init(name: "Second", tableOID: 123, columnAttributeNumber: 456, dataType: .uuidArray, dataTypeSize: 567, dataTypeModifier: 123, format: .text),
        ]
        
        let expected: [PostgresBackendMessage] = [
            .rowDescription(.init(columns: columns))
        ]
        
        var buffer = ByteBuffer()
        expected.forEach { message in
            guard case .rowDescription(let description) = message else {
                return XCTFail("Expected only to get row descriptions here!")
            }
            
            buffer.writeBackendMessage(id: .rowDescription) { buffer in
                buffer.writeInteger(Int16(description.columns.count))
                
                description.columns.forEach { column in
                    buffer.writeNullTerminatedString(column.name)
                    buffer.writeInteger(column.tableOID)
                    buffer.writeInteger(column.columnAttributeNumber)
                    buffer.writeInteger(column.dataType.rawValue)
                    buffer.writeInteger(column.dataTypeSize)
                    buffer.writeInteger(column.dataTypeModifier)
                    buffer.writeInteger(column.format.rawValue)
                }
            }
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected)],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }))
    }
    
    func testDecodeFailureBecauseOfMissingNullTerminationInColumnName() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary)
        
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeInteger(Int16(1))
            buffer.writeString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(column.format.rawValue)
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }
    
    func testDecodeFailureBecauseOfMissingColumnCount() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary)
        
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeNullTerminatedString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(column.format.rawValue)
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }
    
    func testDecodeFailureBecauseInvalidFormatCode() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary)
        
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeInteger(Int16(1))
            buffer.writeNullTerminatedString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(UInt16(2))
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }
    
    func testDecodeFailureBecauseNegativeColumnCount() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary)
        
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeInteger(Int16(-1))
            buffer.writeNullTerminatedString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(column.format.rawValue)
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }

}
