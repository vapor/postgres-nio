import NIOCore
import Testing

@testable import PostgresNIO

@Suite
struct PSQLFrontendMessageTests {

    // MARK: ID

    @Test func messageIDs() {
        #expect(PostgresFrontendMessage.ID.bind.rawValue == UInt8(ascii: "B"))
        #expect(PostgresFrontendMessage.ID.close.rawValue == UInt8(ascii: "C"))
        #expect(PostgresFrontendMessage.ID.describe.rawValue == UInt8(ascii: "D"))
        #expect(PostgresFrontendMessage.ID.execute.rawValue == UInt8(ascii: "E"))
        #expect(PostgresFrontendMessage.ID.flush.rawValue == UInt8(ascii: "H"))
        #expect(PostgresFrontendMessage.ID.parse.rawValue == UInt8(ascii: "P"))
        #expect(PostgresFrontendMessage.ID.password.rawValue == UInt8(ascii: "p"))
        #expect(PostgresFrontendMessage.ID.saslInitialResponse.rawValue == UInt8(ascii: "p"))
        #expect(PostgresFrontendMessage.ID.saslResponse.rawValue == UInt8(ascii: "p"))
        #expect(PostgresFrontendMessage.ID.sync.rawValue == UInt8(ascii: "S"))
        #expect(PostgresFrontendMessage.ID.terminate.rawValue == UInt8(ascii: "X"))
    }

    // MARK: Encoder

    @Test func encodeFlush() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.flush()
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 5)
        #expect(PostgresFrontendMessage.ID.flush.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(4 == byteBuffer.readInteger(as: Int32.self))  // payload length
    }

    @Test func encodeSync() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.sync()
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 5)
        #expect(PostgresFrontendMessage.ID.sync.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(4 == byteBuffer.readInteger(as: Int32.self))  // payload length
    }

    @Test func encodeTerminate() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.terminate()
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 5)
        #expect(PostgresFrontendMessage.ID.terminate.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(4 == byteBuffer.readInteger(as: Int32.self))  // payload length
    }

}
