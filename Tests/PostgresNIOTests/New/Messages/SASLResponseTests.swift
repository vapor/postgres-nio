import XCTest
import NIOCore
@testable import PostgresNIO

class SASLResponseTests: XCTestCase {

    func testEncodeWithData() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let data: [UInt8] = [0, 1, 2, 3, 4, 5, 6, 7]
        encoder.saslResponse(data)
        var byteBuffer = encoder.flushBuffer()

        let length: Int = 1 + 4 + (data.count)
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PostgresFrontendMessage.ID.saslResponse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readBytes(length: data.count), data)
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
    func testEncodeWithoutData() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let data: [UInt8] = []
        encoder.saslResponse(data)
        var byteBuffer = encoder.flushBuffer()

        let length: Int = 1 + 4
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PostgresFrontendMessage.ID.saslResponse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}
