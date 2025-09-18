import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct DescribeTests {
    @Test func testEncodeDescribePortal() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.describePortal("Hello")
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 12)
        #expect(PostgresFrontendMessage.ID.describe.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(11 == byteBuffer.readInteger(as: Int32.self))
        #expect(UInt8(ascii: "P") == byteBuffer.readInteger(as: UInt8.self))
        #expect("Hello" == byteBuffer.readNullTerminatedString())
        #expect(byteBuffer.readableBytes == 0)
    }

    @Test func testEncodeDescribeUnnamedStatement() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.describePreparedStatement("")
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 7)
        #expect(PostgresFrontendMessage.ID.describe.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(6 == byteBuffer.readInteger(as: Int32.self))
        #expect(UInt8(ascii: "S") == byteBuffer.readInteger(as: UInt8.self))
        #expect("" == byteBuffer.readNullTerminatedString())
        #expect(byteBuffer.readableBytes == 0)
    }

}
