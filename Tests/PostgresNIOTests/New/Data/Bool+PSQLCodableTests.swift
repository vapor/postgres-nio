import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct Bool_PSQLCodableTests {

    // MARK: - Binary

    @Test func testBinaryTrueRoundTrip() {
        let value = true

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(Bool.psqlType == .bool)
        #expect(Bool.psqlFormat == .binary)
        #expect(buffer.readableBytes == 1)
        #expect(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self) == 1)

        var result: Bool?
        #expect(throws: Never.self) {
            result = try Bool(from: &buffer, type: .bool, format: .binary, context: .default)
        }
        #expect(value == result)
    }

    @Test func testBinaryFalseRoundTrip() {
        let value = false

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(Bool.psqlType == .bool)
        #expect(Bool.psqlFormat == .binary)
        #expect(buffer.readableBytes == 1)
        #expect(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self) == 0)

        var result: Bool?
        #expect(throws: Never.self) {
            result = try Bool(from: &buffer, type: .bool, format: .binary, context: .default)
        }
        #expect(value == result)
    }

    @Test func testBinaryDecodeBoolInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(1))

        #expect(throws: PostgresDecodingError.Code.failure) {
            try Bool(from: &buffer, type: .bool, format: .binary, context: .default)
        }
    }

    @Test func testBinaryDecodeBoolInvalidValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(13))

        #expect(throws: PostgresDecodingError.Code.failure) {
            try Bool(from: &buffer, type: .bool, format: .binary, context: .default)
        }
    }

    // MARK: - Text

    @Test func testTextTrueDecode() {
        let value = true

        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "t"))

        var result: Bool?
        #expect(throws: Never.self) {
            result = try Bool(from: &buffer, type: .bool, format: .text, context: .default)
        }
        #expect(value == result)
    }

    @Test func testTextFalseDecode() {
        let value = false

        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "f"))

        var result: Bool?
        #expect(throws: Never.self) {
            result = try Bool(from: &buffer, type: .bool, format: .text, context: .default)
        }
        #expect(value == result)
    }

    @Test func testTextDecodeBoolInvalidValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(13))

        #expect(throws: PostgresDecodingError.Code.failure) {
            try Bool(from: &buffer, type: .bool, format: .text, context: .default)
        }
    }
}
