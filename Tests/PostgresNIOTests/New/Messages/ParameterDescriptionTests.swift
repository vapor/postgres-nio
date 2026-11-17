import NIOCore
import NIOTestUtils
import Testing

@testable import PostgresNIO

@Suite
struct ParameterDescriptionTests {
    @Test func decode() {
        let expected: [PostgresBackendMessage] = [
            .parameterDescription(.init(dataTypes: [.bool, .varchar, .uuid, .json, .jsonbArray]))
        ]

        var buffer = ByteBuffer()
        for message in expected {
            guard case .parameterDescription(let description) = message else {
                Issue.record("Expected only to get parameter descriptions here!")
                return
            }

            buffer.writeBackendMessage(id: .parameterDescription) { buffer in
                buffer.writeInteger(Int16(description.dataTypes.count))

                for dataType in description.dataTypes {
                    buffer.writeInteger(dataType.rawValue)
                }
            }
        }

        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, expected)],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeWithNegativeCount() {
        let dataTypes: [PostgresDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            buffer.writeInteger(Int16(-4))

            for dataType in dataTypes {
                buffer.writeInteger(dataType.rawValue)
            }
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeColumnCountDoesntMatchMessageLength() {
        let dataTypes: [PostgresDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            // means three columns comming, but 5 are in the buffer actually.
            buffer.writeInteger(Int16(3))

            for dataType in dataTypes {
                buffer.writeInteger(dataType.rawValue)
            }
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }
}
