import Testing
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

@Suite struct CopyTests {
    @Test func testDecodeCopyInResponseMessage() throws {
        let expected: [PostgresBackendMessage] = [
            .copyInResponse(.init(format: .textual, columnFormats: [.textual, .textual])),
            .copyInResponse(.init(format: .binary, columnFormats: [.binary, .binary])),
            .copyInResponse(.init(format: .binary, columnFormats: [.textual, .binary]))
        ]

        var buffer = ByteBuffer()

        for message in expected {
            guard case .copyInResponse(let message) = message else {
                Issue.record("Expected only to get copyInResponse here!")
                return
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

    @Test func testDecodeFailureBecauseOfEmptyMessage() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { _ in}
        
        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }


    @Test func testDecodeFailureBecauseOfInvalidFormat() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(20))  // Only 0 and 1 are valid formats
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func testDecodeFailureBecauseOfMissingColumnNumber() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(0))
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func testDecodeFailureBecauseOfMissingColumns() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(0))
            buffer.writeInteger(Int16(20))  // 20 columns promised, none given
        }
        
        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func testDecodeFailureBecauseOfInvalidColumnFormat() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .copyInResponse) { buffer in
            buffer.writeInteger(Int8(0))
            buffer.writeInteger(Int16(1))
            buffer.writeInteger(Int8(20))  // Only 0 and 1 are valid formats
        }
        
        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func testEncodeCopyDataHeader() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.copyDataHeader(dataLength: 3)
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 5)
        #expect(PostgresFrontendMessage.ID.copyData.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(byteBuffer.readInteger(as: Int32.self) == 7)
    }

    @Test func testEncodeCopyDone() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.copyDone()
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 5)
        #expect(PostgresFrontendMessage.ID.copyDone.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(byteBuffer.readInteger(as: Int32.self) == 4)
    }

    @Test func testEncodeCopyFail() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.copyFail(message: "Oh, no :(")
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 15)
        #expect(PostgresFrontendMessage.ID.copyFail.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(byteBuffer.readInteger(as: Int32.self) == 14)
        #expect(byteBuffer.readNullTerminatedString() == "Oh, no :(")
    }
}
