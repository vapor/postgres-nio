import NIOCore
import Testing

@testable import PostgresNIO

@Suite
struct SSLRequestTests {
    @Test func sslRequest() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.ssl()
        var byteBuffer = encoder.flushBuffer()

        let byteBufferLength = Int32(byteBuffer.readableBytes)
        #expect(byteBufferLength == byteBuffer.readInteger())
        #expect(PostgresFrontendMessage.SSLRequest.requestCode == byteBuffer.readInteger())

        #expect(byteBuffer.readableBytes == 0)
    }
}
