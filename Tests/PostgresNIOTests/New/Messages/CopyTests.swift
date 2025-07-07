import XCTest
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

class CopyTests: XCTestCase {
    func testDecodeCopyInResponseMessage() throws {
        let expected: [PostgresBackendMessage] = [
            .copyInResponse(.init(format: .textual, columnFormats: [.textual, .textual])),
            .copyInResponse(.init(format: .binary, columnFormats: [.binary, .binary])),
            .copyInResponse(.init(format: .binary, columnFormats: [.textual, .binary]))
        ]

        var buffer = ByteBuffer()

        for message in expected {
            guard case .copyInResponse(let message) = message else {
                return XCTFail("Expected only to get copyInResponse here!")
            }
            buffer.writeBackendMessage(id: .copyInResponse ) { buffer in
                buffer.writeInteger(Int8(message.format.rawValue))
                buffer.writeInteger(Int16(message.columnFormats.count))
                for columnFormat in message.columnFormats {
                    buffer.writeInteger(UInt16(columnFormat.rawValue))
                }
            }
        }
        try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected)],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
        )
    }

    func testDecodeFailureBecauseOfEmptyMessage() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { _ in}
        
        XCTAssertThrowsError(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        ) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }


    func testDecodeFailureBecauseOfInvalidFormat() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(20))  // Only 0 and 1 are valid formats
        }
        
        XCTAssertThrowsError(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        ) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }

    func testDecodeFailureBecauseOfMissingColumnNumber() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(0))
        }
        
        XCTAssertThrowsError(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        ) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }


    func testDecodeFailureBecauseOfMissingColumns() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(0))
            buffer.writeInteger(Int16(20))  // 20 columns promised, none given
        }
        
        XCTAssertThrowsError(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        ) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }

    func testDecodeFailureBecauseOfInvalidColumnFormat() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(0))
            buffer.writeInteger(Int16(1))
            buffer.writeInteger(Int8(20))  // Only 0 and 1 are valid formats
        }
        
        XCTAssertThrowsError(
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        ) {
            XCTAssert($0 is PostgresMessageDecodingError)
        }
    }

    func testEncodeCopyDataHeader() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.copyDataHeader(dataLength: 3)
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.copyData.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), 7)
    }

    func testEncodeCopyDone() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.copyDone()
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.copyDone.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), 4)
    }

    func testEncodeCopyFail() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.copyFail(message: "Oh, no :(")
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 15)
        XCTAssertEqual(PostgresFrontendMessage.ID.copyFail.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), 14)
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "Oh, no :(")
    }
}
