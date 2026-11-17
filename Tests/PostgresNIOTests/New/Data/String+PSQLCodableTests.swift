import NIOCore
import PostgresNIO
import Testing

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

@Suite
class String_PSQLCodableTests {
    @Test func encode() {
        let value = "Hello World"
        var buffer = ByteBuffer()

        value.encode(into: &buffer, context: .default)

        #expect(String.psqlType == .text)
        #expect(buffer.readString(length: buffer.readableBytes) == value)
    }

    @Test func decodeStringFromTextVarchar() throws {
        let expected = "Hello World"
        var buffer = ByteBuffer()
        buffer.writeString(expected)

        let dataTypes: [PostgresDataType] = [
            .text, .varchar, .name, .bpchar
        ]

        for dataType in dataTypes {
            var loopBuffer = buffer
            let result = try String(from: &loopBuffer, type: dataType, format: .binary, context: .default)
            #expect(result == expected)
        }
    }

    @Test func decodeFromUUID() throws {
        let uuid = UUID()
        var buffer = ByteBuffer()
        uuid.encode(into: &buffer, context: .default)

        let decoded = try String(from: &buffer, type: .uuid, format: .binary, context: .default)
        #expect(decoded == uuid.uuidString)
    }

    @Test func decodeFailureFromInvalidUUID() {
        let uuid = UUID()
        var buffer = ByteBuffer()
        uuid.encode(into: &buffer, context: .default)
        // this makes only 15 bytes readable. this should lead to an error
        buffer.moveReaderIndex(forwardBy: 1)

        #expect(throws: PostgresDecodingError.Code.failure) {
            try String(from: &buffer, type: .uuid, format: .binary, context: .default)
        }
    }

    @Test func decodeFromJSONB() throws {
        let json = #"{"hello": "world"}"#
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(1))
        buffer.writeString(json)

        let decoded = try String(from: &buffer, type: .jsonb, format: .binary, context: .default)
        #expect(decoded == json)
    }
}
