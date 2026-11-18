import NIOCore
import NIOTestUtils
import Testing

@testable import PostgresNIO

@Suite
struct ErrorResponseTests {
    @Test func decode() {
        let fields: [PostgresBackendMessage.Field: String] = [
            .file: "auth.c",
            .routine: "auth_failed",
            .line: "334",
            .localizedSeverity: "FATAL",
            .sqlState: "28P01",
            .severity: "FATAL",
            .message: "password authentication failed for user \"postgre3\"",
        ]

        let buffer = ByteBuffer.backendMessage(id: .error) { buffer in
            fields.forEach { (key, value) in
                buffer.writeInteger(key.rawValue, as: UInt8.self)
                buffer.writeNullTerminatedString(value)
            }
            buffer.writeInteger(0, as: UInt8.self)  // signal done
        }

        let expectedInOuts = [
            (buffer, [PostgresBackendMessage.error(.init(fields: fields))])
        ]

        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: expectedInOuts,
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
            )
        }
    }
}
