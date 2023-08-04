import XCTest
import NIOCore
@testable import PostgresNIO

class CancelTests: XCTestCase {
    
    func testEncodeCancel() {
        let processID: Int32 = 1234
        let secretKey: Int32 = 4567
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.cancel(processID: processID, secretKey: secretKey)
        var byteBuffer = encoder.flushBuffer()
        
        XCTAssertEqual(byteBuffer.readableBytes, 16)
        XCTAssertEqual(16, byteBuffer.readInteger(as: Int32.self)) // payload length
        XCTAssertEqual(80877102, byteBuffer.readInteger(as: Int32.self)) // cancel request code
        XCTAssertEqual(processID, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(secretKey, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}
