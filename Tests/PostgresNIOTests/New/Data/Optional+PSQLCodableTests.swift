import XCTest
import NIOCore
@testable import PostgresNIO

class Optional_PSQLCodableTests: XCTestCase {
    
    func testRoundTripSomeString() {
        let value: String? = "Hello World"
        
        var buffer = ByteBuffer()
        XCTAssertNoThrow(try value.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(value.psqlType, .text)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), 11)

        var result: String?
        var optBuffer: ByteBuffer? = buffer
        XCTAssertNoThrow(result = try String?.decodeRaw(from: &optBuffer, type: .text, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneString() {
        let value: Optional<String> = .none

        var buffer = ByteBuffer()
        XCTAssertNoThrow(try value.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 4)
        XCTAssertEqual(buffer.getInteger(at: 0, as: Int32.self), -1)
        XCTAssertEqual(value.psqlType, .null)

        var result: String?
        var inBuffer: ByteBuffer? = nil
        XCTAssertNoThrow(result = try String?.decodeRaw(from: &inBuffer, type: .text, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripSomeUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = UUID()
        let encodable: PostgresEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .uuid)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 20)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), 16)

        var result: UUID?
        var optBuffer: ByteBuffer? = buffer
        XCTAssertNoThrow(result = try UUID?.decodeRaw(from: &optBuffer, type: .uuid, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = .none
        let encodable: PostgresEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .null)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 4)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), -1)

        var result: UUID?
        var inBuffer: ByteBuffer? = nil
        XCTAssertNoThrow(result = try UUID?.decodeRaw(from: &inBuffer, type: .text, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
}
