import NIOCore
import NIOTestUtils
import Testing

@testable import PostgresNIO

@Suite
struct ParameterStatusTests {
    @Test func decode() {
        var buffer = ByteBuffer()

        let expected: [PostgresBackendMessage] = [
            .parameterStatus(.init(parameter: "DateStyle", value: "ISO, MDY")),
            .parameterStatus(.init(parameter: "application_name", value: "")),
            .parameterStatus(.init(parameter: "server_encoding", value: "UTF8")),
            .parameterStatus(.init(parameter: "integer_datetimes", value: "on")),
            .parameterStatus(.init(parameter: "client_encoding", value: "UTF8")),
            .parameterStatus(.init(parameter: "TimeZone", value: "Etc/UTC")),
            .parameterStatus(.init(parameter: "is_superuser", value: "on")),
            .parameterStatus(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")),
            .parameterStatus(.init(parameter: "session_authorization", value: "postgres")),
            .parameterStatus(.init(parameter: "IntervalStyle", value: "postgres")),
            .parameterStatus(.init(parameter: "standard_conforming_strings", value: "on")),
            .backendKeyData(.init(processID: 1234, secretKey: 5678))
        ]

        for message in expected {
            switch message {
            case .parameterStatus(let parameterStatus):
                buffer.writeBackendMessage(id: .parameterStatus) { buffer in
                    buffer.writeNullTerminatedString(parameterStatus.parameter)
                    buffer.writeNullTerminatedString(parameterStatus.value)
                }
            case .backendKeyData(let backendKeyData):
                buffer.writeBackendMessage(id: .backendKeyData) { buffer in
                    buffer.writeInteger(backendKeyData.processID)
                    buffer.writeInteger(backendKeyData.secretKey)
                }
            default:
                Issue.record("Unexpected message type")
                return
            }
        }

        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, expected)],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeFailureBecauseOfMissingNullTermination() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterStatus) { buffer in
            buffer.writeString("DateStyle")
            buffer.writeString("ISO, MDY")
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeFailureBecauseOfMissingNullTerminationInValue() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterStatus) { buffer in
            buffer.writeNullTerminatedString("DateStyle")
            buffer.writeString("ISO, MDY")
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }
}
