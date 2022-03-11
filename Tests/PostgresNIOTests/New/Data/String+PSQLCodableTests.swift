import XCTest
import NIOCore
@testable import PostgresNIO

class String_PSQLCodableTests: XCTestCase {
    
    func testEncode() {
        let value = "Hello World"
        var buffer = ByteBuffer()
        
        value.encode(into: &buffer, context: .default)
        
        XCTAssertEqual(String.psqlType, .text)
        XCTAssertEqual(buffer.readString(length: buffer.readableBytes), value)
    }
    
    func testDecodeStringFromTextVarchar() {
        let expected = "Hello World"
        var buffer = ByteBuffer()
        buffer.writeString(expected)
        
        let dataTypes: [PostgresDataType] = [
            .text, .varchar, .name
        ]
        
        for dataType in dataTypes {
            var loopBuffer = buffer
            var result: String?
            XCTAssertNoThrow(result = try String(from: &loopBuffer, type: dataType, format: .binary, context: .default))
            XCTAssertEqual(result, expected)
        }
    }
    
    func testDecodeFailureFromInvalidType() {
        let buffer = ByteBuffer()
        let dataTypes: [PostgresDataType] = [.bool, .float4Array, .float8Array, .bpchar]
        
        for dataType in dataTypes {
            var loopBuffer = buffer
            XCTAssertThrowsError(try String(from: &loopBuffer, type: dataType, format: .binary, context: .default)) {
                XCTAssertEqual($0 as? PostgresCastingError.Code, .typeMismatch)
            }
        }
    }
    
    func testDecodeFromUUID() {
        let uuid = UUID()
        var buffer = ByteBuffer()
        uuid.encode(into: &buffer, context: .default)
        
        var decoded: String?
        XCTAssertNoThrow(decoded = try String(from: &buffer, type: .uuid, format: .binary, context: .default))
        XCTAssertEqual(decoded, uuid.uuidString)
    }
    
    func testDecodeFailureFromInvalidUUID() {
        let uuid = UUID()
        var buffer = ByteBuffer()
        uuid.encode(into: &buffer, context: .default)
        // this makes only 15 bytes readable. this should lead to an error
        buffer.moveReaderIndex(forwardBy: 1)
        
        XCTAssertThrowsError(try String(from: &buffer, type: .uuid, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresCastingError.Code, .failure)
        }
    }
}

