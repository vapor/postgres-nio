import XCTest
import NIOCore
@testable import PostgresNIO

class Range_PSQLCodableTests: XCTestCase {

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

    func testDecodeRandomInt64Range() {
        // generate two different random Int64 values
        var randomBounds: [Int64] = []
        while randomBounds.first == randomBounds.last {
            randomBounds = [
                Int64.random(in: Int64.min...Int64.max),
                Int64.random(in: Int64.min...Int64.max)
            ]
        }
        let randomRange: Range<Int64> = randomBounds.min()!..<randomBounds.max()!

        var buffer = ByteBuffer()
        buffer.writeInteger(2, as: Int8.self)
        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(randomRange.lowerBound)
        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(randomRange.upperBound)

        var result: Range<Int64>?
        XCTAssertNoThrow(result = try Range<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default))
        XCTAssertEqual(randomRange, result)
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
        buffer.writeInteger(2, as: Int8.self)
        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        XCTAssertThrowsError(try Range<Int64>(from: &buffer, type: .int8, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecodingError.Code, .typeMismatch)
        }
    }

}
