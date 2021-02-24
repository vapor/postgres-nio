import XCTest
@testable import PostgresNIO

class Optional_PSQLCodableTests: XCTestCase {
    
    func testRoundTripSomeString() {
        let value: String? = "Hello World"
        
        var buffer = ByteBuffer()
        value?.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .text)
        let data = PSQLData(bytes: buffer, dataType: .text)
        
        var result: String?
        XCTAssertNoThrow(result = try data.decode(as: String?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneString() {
        let value: Optional<String> = .none
        
        var buffer = ByteBuffer()
        value?.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(buffer.readableBytes, 0)
        XCTAssertEqual(value.psqlType, .null)
        
        let data = PSQLData(bytes: nil, dataType: .text)
        
        var result: String?
        XCTAssertNoThrow(result = try data.decode(as: String?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripSomeUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = UUID()
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .uuid)
        XCTAssertNoThrow(try encodable.encode(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 16)
        
        let data = PSQLData(bytes: buffer, dataType: .uuid)
        
        var result: UUID?
        XCTAssertNoThrow(result = try data.decode(as: UUID?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = .none
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .null)
        XCTAssertNoThrow(try encodable.encode(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 0)
        
        let data = PSQLData(bytes: nil, dataType: .uuid)
        
        var result: UUID?
        XCTAssertNoThrow(result = try data.decode(as: UUID?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
}
