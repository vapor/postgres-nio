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
            XCTAssertNoThrow(try value.encode(into: &buffer, context: .default))
            XCTAssertEqual(MyRawRepresentable.psqlType, Int16.psqlType)
            XCTAssertEqual(buffer.readableBytes, 2)

            var result: MyRawRepresentable?
            XCTAssertNoThrow(result = try MyRawRepresentable(from: &buffer, type: Int16.psqlType, format: .binary, context: .default))
            XCTAssertEqual(value, result)
        }
    }
    
    func testDecodeInvalidRawTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int16(4)) // out of bounds

        XCTAssertThrowsError(try MyRawRepresentable(from: &buffer, type: Int16.psqlType, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecoingError.Code, .failure)
        }
    }
    
    func testDecodeInvalidUnderlyingTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // out of bounds

        XCTAssertThrowsError(try MyRawRepresentable(from: &buffer, type: Int32.psqlType, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecoingError.Code, .failure)
        }
    }
    
}
