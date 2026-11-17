import Atomics
import NIOCore
import PostgresNIO
import Testing

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

@Suite
struct JSON_PSQLCodableTests {
    struct Hello: Equatable, Codable, PostgresCodable {
        let hello: String

        init(name: String) {
            self.hello = name
        }
    }

    @Test func roundTrip() throws {
        var buffer = ByteBuffer()
        let hello = Hello(name: "world")
        try hello.encode(into: &buffer, context: .default)
        #expect(Hello.psqlType == .jsonb)

        // verify jsonb prefix byte
        #expect(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self) == 1)

        let result = try Hello(from: &buffer, type: .jsonb, format: .binary, context: .default)
        #expect(result == hello)
    }

    @Test func decodeFromJSON() throws {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)

        let result = try Hello(from: &buffer, type: .json, format: .binary, context: .default)
        #expect(result == Hello(name: "world"))
    }

    @Test func decodeFromJSONAsText() throws {
        let combinations : [(PostgresFormat, PostgresDataType)] = [
            (.text, .json), (.text, .jsonb),
        ]
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)

        for (format, dataType) in combinations {
            var loopBuffer = buffer
            let result = try Hello(from: &loopBuffer, type: dataType, format: format, context: .default)
            #expect(result == Hello(name: "world"))
        }
    }

    @Test func ecodeFromJSONBWithoutVersionPrefixByte() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)

        #expect(throws: PostgresDecodingError.Code.failure) {
            try Hello(from: &buffer, type: .jsonb, format: .binary, context: .default)
        }
    }

    @Test func decodeFromJSONBWithWrongDataType() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)

        #expect(throws: PostgresDecodingError.Code.typeMismatch) {
            try Hello(from: &buffer, type: .text, format: .binary, context: .default)
        }
    }

    @Test func customEncoderIsUsed() throws {
        final class TestEncoder: PostgresJSONEncoder {
            let encodeHits = ManagedAtomic(0)

            func encode<T>(_ value: T, into buffer: inout ByteBuffer) throws where T : Encodable {
                self.encodeHits.wrappingIncrement(ordering: .relaxed)
            }

            func encode<T>(_ value: T) throws -> Data where T : Encodable {
                preconditionFailure()
            }
        }

        let hello = Hello(name: "world")
        let encoder = TestEncoder()
        var buffer = ByteBuffer()
        try hello.encode(into: &buffer, context: .init(jsonEncoder: encoder))
        #expect(encoder.encodeHits.load(ordering: .relaxed) == 1)
    }
}
