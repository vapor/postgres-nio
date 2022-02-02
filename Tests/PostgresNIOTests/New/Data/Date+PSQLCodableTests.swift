import XCTest
import NIOCore
@testable import PostgresNIO

class Date_PSQLCodableTests: XCTestCase {
    
    func testNowRoundTrip() {
        let value = Date()
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .timestamptz)
        XCTAssertEqual(buffer.readableBytes, 8)

        var result: Date?
        XCTAssertNoThrow(result = try Date.decode(from: &buffer, type: .timestamptz, format: .binary, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testDecodeRandomDate() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        var result: Date?
        XCTAssertNoThrow(result = try Date.decode(from: &buffer, type: .timestamptz, format: .binary, context: .forTests()))
        XCTAssertNotNil(result)
    }
    
    func testDecodeFailureInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        XCTAssertThrowsError(try Date.decode(from: &buffer, type: .timestamptz, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .failure)
        }
    }
    
    func testDecodeDate() {
        var firstDateBuffer = ByteBuffer()
        firstDateBuffer.writeInteger(Int32.min)
        
        var firstDate: Date?
        XCTAssertNoThrow(firstDate = try Date.decode(from: &firstDateBuffer, type: .date, format: .binary, context: .forTests()))
        XCTAssertNotNil(firstDate)
        
        var lastDateBuffer = ByteBuffer()
        lastDateBuffer.writeInteger(Int32.max)

        var lastDate: Date?
        XCTAssertNoThrow(lastDate = try Date.decode(from: &lastDateBuffer, type: .date, format: .binary, context: .forTests()))
        XCTAssertNotNil(lastDate)
    }
    
    func testDecodeDateFromTimestamp() {
        var firstDateBuffer = ByteBuffer()
        firstDateBuffer.writeInteger(Int32.min)
        
        var firstDate: Date?
        XCTAssertNoThrow(firstDate = try Date.decode(from: &firstDateBuffer, type: .date, format: .binary, context: .forTests()))
        XCTAssertNotNil(firstDate)
        
        var lastDateBuffer = ByteBuffer()
        lastDateBuffer.writeInteger(Int32.max)
        
        var lastDate: Date?
        XCTAssertNoThrow(lastDate = try Date.decode(from: &lastDateBuffer, type: .date, format: .binary, context: .forTests()))
        XCTAssertNotNil(lastDate)
    }
    
    func testDecodeDateFailsWithToMuchData() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        XCTAssertThrowsError(try Date.decode(from: &buffer, type: .date, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .failure)
        }
    }
    
    func testDecodeDateFailsWithWrongDataType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        XCTAssertThrowsError(try Date.decode(from: &buffer, type: .int8, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .typeMismatch)
        }
    }
    
}
