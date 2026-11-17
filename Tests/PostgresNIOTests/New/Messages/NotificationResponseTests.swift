import NIOCore
import NIOTestUtils
import Testing

@testable import PostgresNIO

@Suite
struct NotificationResponseTests {
    @Test func decode() {
        let expected: [PostgresBackendMessage] = [
            .notification(.init(backendPID: 123, channel: "test", payload: "hello")),
            .notification(.init(backendPID: 123, channel: "test", payload: "world")),
            .notification(.init(backendPID: 123, channel: "foo", payload: "bar"))
        ]

        var buffer = ByteBuffer()
        for message in expected {
            guard case .notification(let notification) = message else {
                Issue.record("Expected only to get notifications here!")
                return
            }

            buffer.writeBackendMessage(id: .notificationResponse) { buffer in
                buffer.writeInteger(notification.backendPID)
                buffer.writeNullTerminatedString(notification.channel)
                buffer.writeNullTerminatedString(notification.payload)
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
        buffer.writeBackendMessage(id: .notificationResponse) { buffer in
            buffer.writeInteger(Int32(123))
            buffer.writeString("test")
            buffer.writeString("hello")
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
        buffer.writeBackendMessage(id: .notificationResponse) { buffer in
            buffer.writeInteger(Int32(123))
            buffer.writeNullTerminatedString("hello")
            buffer.writeString("world")
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }
}
