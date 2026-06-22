import Foundation
import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct Array_PSQLCodableTests {

    @Test func testArrayTypes() {
        #expect(Bool.psqlArrayType == .boolArray)
        #expect(Bool.psqlType == .bool)
        #expect([Bool].psqlType == .boolArray)

        #expect(ByteBuffer.psqlArrayType == .byteaArray)
        #expect(ByteBuffer.psqlType == .bytea)
        #expect([ByteBuffer].psqlType == .byteaArray)

        #expect(UInt8.psqlArrayType == .charArray)
        #expect(UInt8.psqlType == .char)
        #expect([UInt8].psqlType == .charArray)

        #expect(Int16.psqlArrayType == .int2Array)
        #expect(Int16.psqlType == .int2)
        #expect([Int16].psqlType == .int2Array)

        #expect(Int32.psqlArrayType == .int4Array)
        #expect(Int32.psqlType == .int4)
        #expect([Int32].psqlType == .int4Array)

        #expect(Int64.psqlArrayType == .int8Array)
        #expect(Int64.psqlType == .int8)
        #expect([Int64].psqlType == .int8Array)

        #if (arch(i386) || arch(arm))
        #expect(Int.psqlArrayType == .int4Array)
        #expect(Int.psqlType == .int4)
        #expect([Int].psqlType == .int4Array)
        #else
        #expect(Int.psqlArrayType == .int8Array)
        #expect(Int.psqlType == .int8)
        #expect([Int].psqlType == .int8Array)
        #endif

        #expect(Float.psqlArrayType == .float4Array)
        #expect(Float.psqlType == .float4)
        #expect([Float].psqlType == .float4Array)

        #expect(Double.psqlArrayType == .float8Array)
        #expect(Double.psqlType == .float8)
        #expect([Double].psqlType == .float8Array)

        #expect(String.psqlArrayType == .textArray)
        #expect(String.psqlType == .text)
        #expect([String].psqlType == .textArray)

        #expect(UUID.psqlArrayType == .uuidArray)
        #expect(UUID.psqlType == .uuid)
        #expect([UUID].psqlType == .uuidArray)

        #expect(Date.psqlArrayType == .timestamptzArray)
        #expect(Date.psqlType == .timestamptz)
        #expect([Date].psqlType == .timestamptzArray)

        #expect(Range<Int32>.psqlArrayType == .int4RangeArray)
        #expect(Range<Int32>.psqlType == .int4Range)
        #expect([Range<Int32>].psqlType == .int4RangeArray)

        #expect(ClosedRange<Int32>.psqlArrayType == .int4RangeArray)
        #expect(ClosedRange<Int32>.psqlType == .int4Range)
        #expect([ClosedRange<Int32>].psqlType == .int4RangeArray)

        #expect(Range<Int64>.psqlArrayType == .int8RangeArray)
        #expect(Range<Int64>.psqlType == .int8Range)
        #expect([Range<Int64>].psqlType == .int8RangeArray)

        #expect(ClosedRange<Int64>.psqlArrayType == .int8RangeArray)
        #expect(ClosedRange<Int64>.psqlType == .int8Range)
        #expect([ClosedRange<Int64>].psqlType == .int8RangeArray)
    }

    @Test func testStringArrayRoundTrip() {
        let values = ["foo", "bar", "hello", "world"]

        var buffer = ByteBuffer()
        values.encode(into: &buffer, context: .default)

        var result: [String]?
        #expect(throws: Never.self) {
            result = try [String](from: &buffer, type: .textArray, format: .binary, context: .default)
        }
        #expect(values == result)
    }

    @Test func testEmptyStringArrayRoundTrip() {
        let values: [String] = []

        var buffer = ByteBuffer()
        values.encode(into: &buffer, context: .default)

        var result: [String]?
        #expect(throws: Never.self) {
            result = try [String](from: &buffer, type: .textArray, format: .binary, context: .default)
        }
        #expect(values == result)
    }

    @Test func testDecodeFailureIsNotEmptyOutOfScope() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(2)) // invalid value
        buffer.writeInteger(Int32(0))
        buffer.writeInteger(String.psqlType.rawValue)

        #expect(throws: PostgresDecodingError.Code.failure) {
            try [String](from: &buffer, type: .textArray, format: .binary, context: .default)
        }
    }

    @Test func testDecodeFailureSecondValueIsUnexpected() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(0)) // is empty
        buffer.writeInteger(Int32(1)) // invalid value, must always be 0
        buffer.writeInteger(String.psqlType.rawValue)

        #expect(throws: PostgresDecodingError.Code.failure) {
            try [String](from: &buffer, type: .textArray, format: .binary, context: .default)
        }
    }

    @Test func testDecodeFailureTriesDecodeInt8() {
        let value: Int64 = 1 << 32
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)

        #expect(throws: PostgresDecodingError.Code.failure) {
            try [String](from: &buffer, type: .textArray, format: .binary, context: .default)
        }
    }

    @Test func testDecodeFailureInvalidNumberOfArrayElements() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // invalid value
        buffer.writeInteger(Int32(0))
        buffer.writeInteger(String.psqlType.rawValue)
        buffer.writeInteger(Int32(-123)) // expected element count
        buffer.writeInteger(Int32(1)) // dimensions... must be one

        #expect(throws: PostgresDecodingError.Code.failure) {
            try [String](from: &buffer, type: .textArray, format: .binary, context: .default)
        }
    }

    @Test func testDecodeFailureInvalidNumberOfDimensions() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // invalid value
        buffer.writeInteger(Int32(0))
        buffer.writeInteger(String.psqlType.rawValue)
        buffer.writeInteger(Int32(1)) // expected element count
        buffer.writeInteger(Int32(2)) // dimensions... must be one

        #expect(throws: PostgresDecodingError.Code.failure) {
            try [String](from: &buffer, type: .textArray, format: .binary, context: .default)
        }
    }

    @Test func testDecodeUnexpectedEnd() {
        var unexpectedEndInElementLengthBuffer = ByteBuffer()
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(1)) // invalid value
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(0))
        unexpectedEndInElementLengthBuffer.writeInteger(String.psqlType.rawValue)
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(1)) // expected element count
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(1)) // dimensions
        unexpectedEndInElementLengthBuffer.writeInteger(Int16(1)) // length of element, must be Int32

        #expect(throws: PostgresDecodingError.Code.failure) {
            try [String](from: &unexpectedEndInElementLengthBuffer, type: .textArray, format: .binary, context: .default)
        }

        var unexpectedEndInElementBuffer = ByteBuffer()
        unexpectedEndInElementBuffer.writeInteger(Int32(1)) // invalid value
        unexpectedEndInElementBuffer.writeInteger(Int32(0))
        unexpectedEndInElementBuffer.writeInteger(String.psqlType.rawValue)
        unexpectedEndInElementBuffer.writeInteger(Int32(1)) // expected element count
        unexpectedEndInElementBuffer.writeInteger(Int32(1)) // dimensions
        unexpectedEndInElementBuffer.writeInteger(Int32(12)) // length of element, must be Int32
        unexpectedEndInElementBuffer.writeString("Hello World") // only 11 bytes, 12 needed!

        #expect(throws: PostgresDecodingError.Code.failure) {
            try [String](from: &unexpectedEndInElementBuffer, type: .textArray, format: .binary, context: .default)
        }
    }
}
