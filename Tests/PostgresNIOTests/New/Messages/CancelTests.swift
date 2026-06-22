import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct CancelTests {
    @Test func testEncodeCancel() {
        let processID: Int32 = 1234
        let secretKey: Int32 = 4567
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.cancel(processID: processID, secretKey: secretKey)
        var byteBuffer = encoder.flushBuffer()
        
        #expect(byteBuffer.readableBytes == 16)
        #expect(16 == byteBuffer.readInteger(as: Int32.self)) // payload length
        #expect(80877102 == byteBuffer.readInteger(as: Int32.self)) // cancel request code
        #expect(processID == byteBuffer.readInteger(as: Int32.self))
        #expect(secretKey == byteBuffer.readInteger(as: Int32.self))
        #expect(byteBuffer.readableBytes == 0)
    }
}
