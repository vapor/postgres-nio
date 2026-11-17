import Testing
import NIOCore
import PostgresNIO

@Suite
struct Range_PSQLCodableTests {
    @Test func int32RangeRoundTrip() throws {
        let lowerBound = Int32.min
        let upperBound = Int32.max
        let value: Range<Int32> = lowerBound..<upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(Range<Int32>.psqlType == .int4Range)
        #expect(buffer.readableBytes == 17)
        #expect(buffer.getInteger(at: 0, as: UInt8.self) == 2)
        #expect(buffer.getInteger(at: 1, as: UInt32.self) == 4)
        #expect(buffer.getInteger(at: 5, as: Int32.self) == lowerBound)
        #expect(buffer.getInteger(at: 9, as: UInt32.self) == 4)
        #expect(buffer.getInteger(at: 13, as: Int32.self) == upperBound)

        let result = try Range<Int32>(from: &buffer, type: .int4Range, format: .binary, context: .default)
        #expect(value == result)
    }

    @Test func int32ClosedRangeRoundTrip() throws {
        let lowerBound = Int32.min
        let upperBound = Int32.max - 1
        let value: ClosedRange<Int32> = lowerBound...upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(ClosedRange<Int32>.psqlType == .int4Range)
        #expect(buffer.readableBytes == 17)
        #expect(buffer.getInteger(at: 0, as: UInt8.self) == 6)
        #expect(buffer.getInteger(at: 1, as: UInt32.self) == 4)
        #expect(buffer.getInteger(at: 5, as: Int32.self) == lowerBound)
        #expect(buffer.getInteger(at: 9, as: UInt32.self) == 4)
        #expect(buffer.getInteger(at: 13, as: Int32.self) == upperBound)

        let result = try ClosedRange<Int32>(from: &buffer, type: .int4Range, format: .binary, context: .default)
        #expect(value == result)
    }

    @Test func int64RangeRoundTrip() throws {
        let lowerBound = Int64.min
        let upperBound = Int64.max
        let value: Range<Int64> = lowerBound..<upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(Range<Int64>.psqlType == .int8Range)
        #expect(buffer.readableBytes == 25)
        #expect(buffer.getInteger(at: 0, as: UInt8.self) == 2)
        #expect(buffer.getInteger(at: 1, as: UInt32.self) == 8)
        #expect(buffer.getInteger(at: 5, as: Int64.self) == lowerBound)
        #expect(buffer.getInteger(at: 13, as: UInt32.self) == 8)
        #expect(buffer.getInteger(at: 17, as: Int64.self) == upperBound)

        let result = try Range<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default)
        #expect(value == result)
    }

    @Test func int64ClosedRangeRoundTrip() throws {
        let lowerBound = Int64.min
        let upperBound = Int64.max - 1
        let value: ClosedRange<Int64> = lowerBound...upperBound

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(ClosedRange<Int64>.psqlType == .int8Range)
        #expect(buffer.readableBytes == 25)
        #expect(buffer.getInteger(at: 0, as: UInt8.self) == 6)
        #expect(buffer.getInteger(at: 1, as: UInt32.self) == 8)
        #expect(buffer.getInteger(at: 5, as: Int64.self) == lowerBound)
        #expect(buffer.getInteger(at: 13, as: UInt32.self) == 8)
        #expect(buffer.getInteger(at: 17, as: Int64.self) == upperBound)

        let result = try ClosedRange<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default)
        #expect(value == result)
    }

    @Test func int64RangeDecodeFailureInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(0)
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        #expect(throws: PostgresDecodingError.Code.failure) {
            try Range<Int64>(from: &buffer, type: .int8Range, format: .binary, context: .default)
        }
    }

    @Test func int64RangeDecodeFailureWrongDataType() {
        var buffer = ByteBuffer()
        (Int64.min...Int64.max).encode(into: &buffer, context: .default)

        #expect(throws: PostgresDecodingError.Code.failure) {
            try Range<Int64>(from: &buffer, type: .int8, format: .binary, context: .default)
        }
    }
}
