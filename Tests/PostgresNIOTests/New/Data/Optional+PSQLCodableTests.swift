import XCTest
import NIOCore
@testable import PostgresNIO

class Optional_PSQLCodableTests: XCTestCase {
    
    func testRoundTripSomeString() {
        let value: String? = "Hello World"
        
        var buffer = ByteBuffer()
        value?.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .text)

        var result: String?
        XCTAssertNoThrow(result = try String?.decode(from: &buffer, type: .text, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneString() {
        let value: Optional<String> = .none

        var buffer = ByteBuffer()
        value?.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(buffer.readableBytes, 0)
        XCTAssertEqual(value.psqlType, .null)

        var result: String?
        XCTAssertNoThrow(result = try String?.decode(from: &buffer, type: .text, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripSomeUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = UUID()
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .uuid)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 20)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), 16)

        var result: UUID?
        XCTAssertNoThrow(result = try UUID?.decode(from: &buffer, type: .uuid, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = .none
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .null)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 4)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), -1)

        var result: UUID?
        XCTAssertNoThrow(result = try UUID?.decode(from: &buffer, type: .text, format: .binary, context: .forTests()))
        XCTAssertEqual(result, value)
    }
}
