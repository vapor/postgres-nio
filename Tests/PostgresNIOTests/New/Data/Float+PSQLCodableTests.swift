import Testing
import NIOCore
@testable import PostgresNIO

@Suite
struct Float_PSQLCodableTests {
    @Test func roundTripDoubles() throws {
        let values: [Double] = [1.1, .pi, -5e-12]

        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            #expect(Double.psqlType == .float8)
            #expect(buffer.readableBytes == 8)

            let result = try Double(from: &buffer, type: .float8, format: .binary, context: .default)
            #expect(value == result)
        }
    }

    @Test func roundTripFloat() throws {
        let values: [Float] = [1.1, .pi, -5e-12]

        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            #expect(Float.psqlType == .float4)
            #expect(buffer.readableBytes == 4)

            let result = try Float(from: &buffer, type: .float4, format: .binary, context: .default)
            #expect(value == result)
        }
    }

    @Test func roundTripDoubleNaN() throws {
        let value: Double = .nan

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(Double.psqlType == .float8)
        #expect(buffer.readableBytes == 8)

        let result = try Double(from: &buffer, type: .float8, format: .binary, context: .default)
        #expect(result.isNaN == true)
    }

    @Test func roundTripDoubleInfinity() throws {
        let value: Double = .infinity

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(Double.psqlType == .float8)
        #expect(buffer.readableBytes == 8)

        let result = try Double(from: &buffer, type: .float8, format: .binary, context: .default)
        #expect(result.isInfinite == true)
    }

    @Test func roundTripFromFloatToDouble() throws {
        let values: [Float] = [1.1, .pi, -5e-12]

        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            #expect(Float.psqlType == .float4)
            #expect(buffer.readableBytes == 4)

            let result = try Double(from: &buffer, type: .float4, format: .binary, context: .default)
            #expect(result == Double(value))
        }
    }

    @Test func roundTripFromDoubleToFloat() throws {
        let values: [Double] = [1.1, .pi, -5e-12]

        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            #expect(Double.psqlType == .float8)
            #expect(buffer.readableBytes == 8)

            let result = try Float(from: &buffer, type: .float8, format: .binary, context: .default)
            #expect(result == Float(value))
        }
    }

    @Test func decodeFailureInvalidLength() {
        var eightByteBuffer = ByteBuffer()
        eightByteBuffer.writeInteger(Int64(0))
        var fourByteBuffer = ByteBuffer()
        fourByteBuffer.writeInteger(Int32(0))

        var tooLongBuffer1 = eightByteBuffer
        #expect(throws: PostgresDecodingError.Code.failure) {
            try Double(from: &tooLongBuffer1, type: .float4, format: .binary, context: .default)
        }

        var tooLongBuffer2 = eightByteBuffer
        #expect(throws: PostgresDecodingError.Code.failure) {
            try Float(from: &tooLongBuffer2, type: .float4, format: .binary, context: .default)
        }

        var tooShortBuffer1 = fourByteBuffer
        #expect(throws: PostgresDecodingError.Code.failure) {
            try Double(from: &tooShortBuffer1, type: .float8, format: .binary, context: .default)
        }

        var tooShortBuffer2 = fourByteBuffer
        #expect(throws: PostgresDecodingError.Code.failure) {
            try Float(from: &tooShortBuffer2, type: .float8, format: .binary, context: .default)
        }
    }

    @Test func decodeFailureInvalidType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        var copy1 = buffer
        #expect(throws: PostgresDecodingError.Code.typeMismatch) {
            try Double(from: &copy1, type: .int8, format: .binary, context: .default)
        }

        var copy2 = buffer
        #expect(throws: PostgresDecodingError.Code.typeMismatch) {
            try Float(from: &copy2, type: .int8, format: .binary, context: .default)
        }
    }
}
