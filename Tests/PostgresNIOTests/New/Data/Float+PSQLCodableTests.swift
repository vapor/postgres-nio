import XCTest
import NIOCore
@testable import PostgresNIO

class Float_PSQLCodableTests: XCTestCase {
    
    func testRoundTripDoubles() {
        let values: [Double] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            XCTAssertEqual(value.psqlType, .float8)
            XCTAssertEqual(buffer.readableBytes, 8)

            var result: Double?
            XCTAssertNoThrow(result = try Double.decode(from: &buffer, type: .float8, format: .binary, context: .default))
            XCTAssertEqual(value, result)
        }
    }
    
    func testRoundTripFloat() {
        let values: [Float] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            XCTAssertEqual(value.psqlType, .float4)
            XCTAssertEqual(buffer.readableBytes, 4)

            var result: Float?
            XCTAssertNoThrow(result = try Float.decode(from: &buffer, type: .float4, format: .binary, context: .default))
            XCTAssertEqual(value, result)
        }
    }
    
    func testRoundTripDoubleNaN() {
        let value: Double = .nan
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(value.psqlType, .float8)
        XCTAssertEqual(buffer.readableBytes, 8)

        var result: Double?
        XCTAssertNoThrow(result = try Double.decode(from: &buffer, type: .float8, format: .binary, context: .default))
        XCTAssertEqual(result?.isNaN, true)
    }
    
    func testRoundTripDoubleInfinity() {
        let value: Double = .infinity
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(value.psqlType, .float8)
        XCTAssertEqual(buffer.readableBytes, 8)

        var result: Double?
        XCTAssertNoThrow(result = try Double.decode(from: &buffer, type: .float8, format: .binary, context: .default))
        XCTAssertEqual(result?.isInfinite, true)
    }
    
    func testRoundTripFromFloatToDouble() {
        let values: [Float] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            XCTAssertEqual(value.psqlType, .float4)
            XCTAssertEqual(buffer.readableBytes, 4)

            var result: Double?
            XCTAssertNoThrow(result = try Double.decode(from: &buffer, type: .float4, format: .binary, context: .default))
            XCTAssertEqual(result, Double(value))
        }
    }
    
    func testRoundTripFromDoubleToFloat() {
        let values: [Double] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            XCTAssertEqual(value.psqlType, .float8)
            XCTAssertEqual(buffer.readableBytes, 8)

            var result: Float?
            XCTAssertNoThrow(result = try Float.decode(from: &buffer, type: .float8, format: .binary, context: .default))
            XCTAssertEqual(result, Float(value))
        }
    }
    
    func testDecodeFailureInvalidLength() {
        var eightByteBuffer = ByteBuffer()
        eightByteBuffer.writeInteger(Int64(0))
        var fourByteBuffer = ByteBuffer()
        fourByteBuffer.writeInteger(Int32(0))

        var toLongBuffer1 = eightByteBuffer
        XCTAssertThrowsError(try Double.decode(from: &toLongBuffer1, type: .float4, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .failure)
        }

        var toLongBuffer2 = eightByteBuffer
        XCTAssertThrowsError(try Float.decode(from: &toLongBuffer2, type: .float4, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .failure)
        }

        var toShortBuffer1 = fourByteBuffer
        XCTAssertThrowsError(try Double.decode(from: &toShortBuffer1, type: .float8, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .failure)
        }

        var toShortBuffer2 = fourByteBuffer
        XCTAssertThrowsError(try Float.decode(from: &toShortBuffer2, type: .float8, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .failure)
        }
    }
    
    func testDecodeFailureInvalidType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        var copy1 = buffer
        XCTAssertThrowsError(try Double.decode(from: &copy1, type: .int8, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .typeMismatch)
        }

        var copy2 = buffer
        XCTAssertThrowsError(try Float.decode(from: &copy2, type: .int8, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .typeMismatch)
        }
    }
}
