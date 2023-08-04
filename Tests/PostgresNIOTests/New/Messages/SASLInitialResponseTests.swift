import XCTest
import NIOCore
@testable import PostgresNIO

class SASLInitialResponseTests: XCTestCase {

    func testEncode() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let saslMechanism = "hello"
        let initialData: [UInt8] = [0, 1, 2, 3, 4, 5, 6, 7]
        encoder.saslInitialResponse(mechanism: saslMechanism, bytes: initialData)
        var byteBuffer = encoder.flushBuffer()
        
        let length: Int = 1 + 4 + (saslMechanism.count + 1) + 4 + initialData.count

        //   1 id
        // + 4 length
        // + 6 saslMechanism (5 + 1 null terminator)
        // + 4 initialData length
        // + 8 initialData
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PostgresFrontendMessage.ID.saslInitialResponse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), saslMechanism)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(initialData.count))
        XCTAssertEqual(byteBuffer.readBytes(length: initialData.count), initialData)
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
    func testEncodeWithoutData() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let saslMechanism = "hello"
        let initialData: [UInt8] = []
        encoder.saslInitialResponse(mechanism: saslMechanism, bytes: initialData)
        var byteBuffer = encoder.flushBuffer()
        
        let length: Int = 1 + 4 + (saslMechanism.count + 1) + 4 + initialData.count

        //   1 id
        // + 4 length
        // + 6 saslMechanism (5 + 1 null terminator)
        // + 4 initialData length
        // + 0 initialData
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PostgresFrontendMessage.ID.saslInitialResponse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), saslMechanism)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(-1))
        XCTAssertEqual(byteBuffer.readBytes(length: initialData.count), initialData)
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}
