import XCTest
import NIOCore
@testable import PostgresNIO

class Bool_PSQLCodableTests: XCTestCase {
    
    // MARK: - Binary
    
    func testBinaryTrueRoundTrip() {
        let value = true
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .bool)
        XCTAssertEqual(value.psqlFormat, .binary)
        XCTAssertEqual(buffer.readableBytes, 1)
        XCTAssertEqual(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), 1)
        
        var result: Bool?
        XCTAssertNoThrow(result = try Bool.decode(from: &buffer, type: .bool, format: .binary, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testBinaryFalseRoundTrip() {
        let value = false
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .bool)
        XCTAssertEqual(value.psqlFormat, .binary)
        XCTAssertEqual(buffer.readableBytes, 1)
        XCTAssertEqual(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), 0)

        var result: Bool?
        XCTAssertNoThrow(result = try Bool.decode(from: &buffer, type: .bool, format: .binary, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testBinaryDecodeBoolInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(1))

        XCTAssertThrowsError(try Bool.decode(from: &buffer, type: .bool, format: .binary, context: .forTests())) {
            XCTAssert($0 is PSQLCastingError)
        }
    }
    
    func testBinaryDecodeBoolInvalidValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(13))

        XCTAssertThrowsError(try Bool.decode(from: &buffer, type: .bool, format: .binary, context: .forTests())) {
            XCTAssert($0 is PSQLCastingError)
        }
    }

    // MARK: - Text
    
    func testTextTrueDecode() {
        let value = true
        
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "t"))

        var result: Bool?
        XCTAssertNoThrow(result = try Bool.decode(from: &buffer, type: .bool, format: .text, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testTextFalseDecode() {
        let value = false
        
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "f"))

        var result: Bool?
        XCTAssertNoThrow(result = try Bool.decode(from: &buffer, type: .bool, format: .text, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testTextDecodeBoolInvalidValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(13))

        XCTAssertThrowsError(try Bool.decode(from: &buffer, type: .bool, format: .text, context: .forTests())) {
            XCTAssert($0 is PSQLCastingError)
        }
    }
}
