import XCTest
import NIOCore
@testable import PostgresNIO

class RawRepresentable_PSQLCodableTests: XCTestCase {
    
    enum MyRawRepresentable: Int16, PostgresCodable {
        case testing = 1
        case staging = 2
        case production = 3
    }
    
    func testRoundTrip() {
        let values: [MyRawRepresentable] = [.testing, .staging, .production]
        
        for value in values {
            var buffer = ByteBuffer()
            XCTAssertNoThrow(try value.encode(into: &buffer, context: .forTests()))
            XCTAssertEqual(value.psqlType, Int16.psqlArrayElementType)
            XCTAssertEqual(buffer.readableBytes, 2)

            var result: MyRawRepresentable?
            XCTAssertNoThrow(result = try MyRawRepresentable.decode(from: &buffer, type: Int16.psqlArrayElementType, format: .binary, context: .forTests()))
            XCTAssertEqual(value, result)
        }
    }
    
    func testDecodeInvalidRawTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int16(4)) // out of bounds

        XCTAssertThrowsError(try MyRawRepresentable.decode(from: &buffer, type: Int16.psqlArrayElementType, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PostgresCastingError.Code, .failure)
        }
    }
    
    func testDecodeInvalidUnderlyingTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // out of bounds

        XCTAssertThrowsError(try MyRawRepresentable.decode(from: &buffer, type: Int32.psqlArrayElementType, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PostgresCastingError.Code, .failure)
        }
    }
    
}
