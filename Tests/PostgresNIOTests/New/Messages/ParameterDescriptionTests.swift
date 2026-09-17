import NIOCore
import NIOTestUtils
import XCTest

@testable import PostgresNIO

class ParameterDescriptionTests: XCTestCase {

    func testDecode() {
        let expected: [PostgresBackendMessage] = [
            .parameterDescription(.init(dataTypes: [.bool, .varchar, .uuid, .json, .jsonbArray]))
        ]

        var buffer = ByteBuffer()
        for message in expected {
            guard case .parameterDescription(let description) = message else {
                XCTFail("Expected only to get parameter descriptions here!")
                return
            }

            buffer.writeBackendMessage(id: .parameterDescription) { buffer in
                buffer.writeInteger(Int16(description.dataTypes.count))

                for dataType in description.dataTypes {
                    buffer.writeInteger(dataType.rawValue)
                }
            }
        }

        XCTAssertNoThrow(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, expected)],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }))
    }

    func testDecodeWithNegativeCount() {
        let dataTypes: [PostgresDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            buffer.writeInteger(Int16(-4))

            for dataType in dataTypes {
                buffer.writeInteger(dataType.rawValue)
            }
        }

        XCTAssertThrowsError(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })
        ) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }

    func testDecodeColumnCountDoesntMatchMessageLength() {
        let dataTypes: [PostgresDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            // means three columns comming, but 5 are in the buffer actually.
            buffer.writeInteger(Int16(3))

            for dataType in dataTypes {
                buffer.writeInteger(dataType.rawValue)
            }
        }

        XCTAssertThrowsError(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })
        ) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }
}
