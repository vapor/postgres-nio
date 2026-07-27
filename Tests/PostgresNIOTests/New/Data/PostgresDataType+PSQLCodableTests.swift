import NIOCore
import Testing

@testable import PostgresNIO

@Suite struct PostgresDataType_PSQLCodableTests {
    @Test func testRoundTrip() {
        let values: [UInt32] = [0, 1, 26, 42, 12345, UInt32(UInt16.max), UInt32.max]
        for value in values {
            let oid = PostgresDataType(rawValue: value)
            var buffer = ByteBuffer()
            oid?.encode(into: &buffer, context: .default)

            #expect(PostgresDataType.psqlType == .oid)
            #expect(PostgresDataType.psqlFormat == .binary)
            #expect(buffer.readableBytes == 4)
            #expect(buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) == value)

            var decoded: PostgresDataType?
            #expect(throws: Never.self) {
                decoded = try PostgresDataType(from: &buffer, type: .oid, format: .binary, context: .default)
            }
            #expect(decoded == oid)
        }
    }

    @Test func testDecodeFromBinaryOID() {
        let values: [UInt32] = [0, 1, 26, UInt32(UInt16.max), UInt32(Int32.max), UInt32.max]
        for value in values {
            for type in [PostgresDataType.oid, .regproc] {
                var buffer = ByteBuffer()
                buffer.writeInteger(value)

                var decoded: PostgresDataType?
                #expect(throws: Never.self) {
                    decoded = try PostgresDataType(from: &buffer, type: type, format: .binary, context: .default)
                }
                #expect(decoded?.rawValue == value)
            }
        }
    }

    @Test func testDecodeFromBinaryInt4() {
        for value in [Int32(0), 1, 26, Int32(UInt16.max), Int32.max] {
            var buffer = ByteBuffer()
            buffer.writeInteger(value)

            var decoded: PostgresDataType?
            #expect(throws: Never.self) {
                decoded = try PostgresDataType(from: &buffer, type: .int4, format: .binary, context: .default)
            }
            #expect(decoded?.rawValue == UInt32(value))
        }

        for value in [Int32(-1), -26, Int32.min] {
            var buffer = ByteBuffer()
            buffer.writeInteger(value)

            #expect(throws: PostgresDecodingError.Code.failure) {
                try PostgresDataType(from: &buffer, type: .int4, format: .binary, context: .default)
            }
        }
    }

    @Test func testDecodeFromText() {
        for value in [0, 1, 26, UInt32(UInt16.max), UInt32.max] {
            for type in [PostgresDataType.oid, .int4] {
                var buffer = ByteBuffer()
                buffer.writeString(String(value))

                var decoded: PostgresDataType?
                #expect(throws: Never.self) {
                    decoded = try PostgresDataType(from: &buffer, type: type, format: .text, context: .default)
                }
                #expect(decoded?.rawValue == value)
            }
        }
    }

    @Test func testDecodeFailureFromInvalidByteCount() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt32(26))
        // Make only three bytes readable
        buffer.moveReaderIndex(forwardBy: 1)

        #expect(throws: PostgresDecodingError.Code.failure) {
            try PostgresDataType(from: &buffer, type: .oid, format: .binary, context: .default)
        }
    }

    @Test func testDecodeFailureFromInvalidText() {
        for string in ["", "notanumber", "-1", "4294967296"] {
            var buffer = ByteBuffer()
            buffer.writeString(string)

            #expect(throws: PostgresDecodingError.Code.failure) {
                try PostgresDataType(from: &buffer, type: .oid, format: .text, context: .default)
            }
        }
    }

    @Test func testDecodeFailureFromInvalidPostgresType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt32(26))

        for type in [PostgresDataType.bool, .int2, .int8, .uuid, .text, .oidArray] {
            for format in [PostgresFormat.binary, .text] {
                var copy = buffer
                #expect(throws: PostgresDecodingError.Code.typeMismatch) {
                    try PostgresDataType(from: &copy, type: type, format: format, context: .default)
                }
            }
        }

        for type in [PostgresDataType.regproc, .regtype, .regclass] {
            var copy = buffer
            #expect(throws: PostgresDecodingError.Code.typeMismatch) {
                try PostgresDataType(from: &copy, type: type, format: .text, context: .default)
            }
        }
    }
}
