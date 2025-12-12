import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct CloseTests {
    @Test func testEncodeClosePortal() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.closePortal("Hello")
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 12)
        #expect(PostgresFrontendMessage.ID.close.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(11 == byteBuffer.readInteger(as: Int32.self))
        #expect(UInt8(ascii: "P") == byteBuffer.readInteger(as: UInt8.self))
        #expect("Hello" == byteBuffer.readNullTerminatedString())
        #expect(byteBuffer.readableBytes == 0)
    }
    
    @Test func testEncodeCloseUnnamedStatement() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.closePreparedStatement("")
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 7)
        #expect(PostgresFrontendMessage.ID.close.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(6 == byteBuffer.readInteger(as: Int32.self))
        #expect(UInt8(ascii: "S") == byteBuffer.readInteger(as: UInt8.self))
        #expect("" == byteBuffer.readNullTerminatedString())
        #expect(byteBuffer.readableBytes == 0)
    }
}
