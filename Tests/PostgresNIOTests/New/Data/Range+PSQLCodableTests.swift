import XCTest
import NIOCore
@testable import PostgresNIO

class Range_PSQLCodableTests: XCTestCase {
    func testInt32RangeRoundTrip() {
        let value: Range<Int32> = Int32.min..<Int32.max

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(Range<Int32>.psqlType, .int4Range)
        XCTAssertEqual(buffer.readableBytes, 17)

        var result: Range<Int32>?
        XCTAssertNoThrow(result = try Range<Int32>(from: &buffer, type: .int4Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt32ClosedRangeRoundTrip() {
        let value: ClosedRange<Int32> = Int32.min...(Int32.max - 1)

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(ClosedRange<Int32>.psqlType, .int4Range)
        XCTAssertEqual(buffer.readableBytes, 17)

        var result: ClosedRange<Int32>?
        XCTAssertNoThrow(result = try ClosedRange<Int32>(from: &buffer, type: .int4Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt64RangeRoundTrip() {
        let value: Range<Int64> = Int64.min..<Int64.max

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(Range<Int64>.psqlType, .int8Range)
        XCTAssertEqual(buffer.readableBytes, 25)

        var result: Range<Int64>?
        XCTAssertNoThrow(result = try Range<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt64ClosedRangeRoundTrip() {
        let value: ClosedRange<Int64> = Int64.min...(Int64.max - 1)

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(ClosedRange<Int64>.psqlType, .int8Range)
        XCTAssertEqual(buffer.readableBytes, 25)

        var result: ClosedRange<Int64>?
        XCTAssertNoThrow(result = try ClosedRange<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt64RangeDecodeFailureInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        XCTAssertThrowsError(try Range<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecodingError.Code, .failure)
        }
    }

    func testInt64RangeDecodeFailureWrongDataType() {
        var buffer = ByteBuffer()
        (Int64.min...Int64.max).encode(into: &buffer, context: .default)

        XCTAssertThrowsError(try Range<Int64>(from: &buffer, type: .int8, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecodingError.Code, .failure)
        }
    }
}
