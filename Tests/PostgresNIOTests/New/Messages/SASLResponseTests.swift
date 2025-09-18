import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct SASLResponseTests {

    @Test func testEncodeWithData() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let data: [UInt8] = [0, 1, 2, 3, 4, 5, 6, 7]
        encoder.saslResponse(data)
        var byteBuffer = encoder.flushBuffer()

        let length: Int = 1 + 4 + (data.count)
        
        #expect(byteBuffer.readableBytes == length)
        #expect(byteBuffer.readInteger(as: UInt8.self) == PostgresFrontendMessage.ID.saslResponse.rawValue)
        #expect(byteBuffer.readInteger(as: Int32.self) == Int32(length - 1))
        #expect(byteBuffer.readBytes(length: data.count) == data)
        #expect(byteBuffer.readableBytes == 0)
    }
    
    @Test func testEncodeWithoutData() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let data: [UInt8] = []
        encoder.saslResponse(data)
        var byteBuffer = encoder.flushBuffer()

        let length: Int = 1 + 4
        
        #expect(byteBuffer.readableBytes == length)
        #expect(byteBuffer.readInteger(as: UInt8.self) == PostgresFrontendMessage.ID.saslResponse.rawValue)
        #expect(byteBuffer.readInteger(as: Int32.self) == Int32(length - 1))
        #expect(byteBuffer.readableBytes == 0)
    }
}
