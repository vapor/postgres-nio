import XCTest
import NIOCore
@testable import PostgresNIO

class SSLRequestTests: XCTestCase {
    
    func testSSLRequest() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.ssl()
        var byteBuffer = encoder.flushBuffer()
        
        let byteBufferLength = Int32(byteBuffer.readableBytes)
        XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
        XCTAssertEqual(PostgresFrontendMessage.SSLRequest.requestCode, byteBuffer.readInteger())

        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
}
