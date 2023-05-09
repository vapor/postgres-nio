import XCTest
import NIOCore
@testable import PostgresNIO

class Range_PSQLCodableTests: XCTestCase {
    func testInt32RangeRoundTrip() {
        let lowerBound = Int32.min
        let upperBound = Int32.max
        let value: Range<Int32> = lowerBound..<upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(Range<Int32>.psqlType, .int4Range)
        XCTAssertEqual(buffer.readableBytes, 17)
        XCTAssertEqual(buffer.getInteger(at: 0, as: UInt8.self), 2)
        XCTAssertEqual(buffer.getInteger(at: 1, as: UInt32.self), 4)
        XCTAssertEqual(buffer.getInteger(at: 5, as: Int32.self), lowerBound)
        XCTAssertEqual(buffer.getInteger(at: 9, as: UInt32.self), 4)
        XCTAssertEqual(buffer.getInteger(at: 13, as: Int32.self), upperBound)

        var result: Range<Int32>?
        XCTAssertNoThrow(result = try Range<Int32>(from: &buffer, type: .int4Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt32ClosedRangeRoundTrip() {
        let lowerBound = Int32.min
        let upperBound = Int32.max - 1
        let value: ClosedRange<Int32> = lowerBound...upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(ClosedRange<Int32>.psqlType, .int4Range)
        XCTAssertEqual(buffer.readableBytes, 17)
        XCTAssertEqual(buffer.getInteger(at: 0, as: UInt8.self), 6)
        XCTAssertEqual(buffer.getInteger(at: 1, as: UInt32.self), 4)
        XCTAssertEqual(buffer.getInteger(at: 5, as: Int32.self), lowerBound)
        XCTAssertEqual(buffer.getInteger(at: 9, as: UInt32.self), 4)
        XCTAssertEqual(buffer.getInteger(at: 13, as: Int32.self), upperBound)

        var result: ClosedRange<Int32>?
        XCTAssertNoThrow(result = try ClosedRange<Int32>(from: &buffer, type: .int4Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt64RangeRoundTrip() {
        let lowerBound = Int64.min
        let upperBound = Int64.max
        let value: Range<Int64> = lowerBound..<upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(Range<Int64>.psqlType, .int8Range)
        XCTAssertEqual(buffer.readableBytes, 25)
        XCTAssertEqual(buffer.getInteger(at: 0, as: UInt8.self), 2)
        XCTAssertEqual(buffer.getInteger(at: 1, as: UInt32.self), 8)
        XCTAssertEqual(buffer.getInteger(at: 5, as: Int64.self), lowerBound)
        XCTAssertEqual(buffer.getInteger(at: 13, as: UInt32.self), 8)
        XCTAssertEqual(buffer.getInteger(at: 17, as: Int64.self), upperBound)

        var result: Range<Int64>?
        XCTAssertNoThrow(result = try Range<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt64ClosedRangeRoundTrip() {
        let lowerBound = Int64.min
        let upperBound = Int64.max - 1
        let value: ClosedRange<Int64> = lowerBound...upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(ClosedRange<Int64>.psqlType, .int8Range)
        XCTAssertEqual(buffer.readableBytes, 25)
        XCTAssertEqual(buffer.getInteger(at: 0, as: UInt8.self), 6)
        XCTAssertEqual(buffer.getInteger(at: 1, as: UInt32.self), 8)
        XCTAssertEqual(buffer.getInteger(at: 5, as: Int64.self), lowerBound)
        XCTAssertEqual(buffer.getInteger(at: 13, as: UInt32.self), 8)
        XCTAssertEqual(buffer.getInteger(at: 17, as: Int64.self), upperBound)

        var result: ClosedRange<Int64>?
        XCTAssertNoThrow(result = try ClosedRange<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default))
        XCTAssertEqual(value, result)
    }

    func testInt64RangeDecodeFailureInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(0)
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
