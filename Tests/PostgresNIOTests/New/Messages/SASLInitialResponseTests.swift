import XCTest
import NIOCore
@testable import PostgresNIO

class SASLInitialResponseTests: XCTestCase {

    func testEncodeWithData() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        let sasl = PSQLFrontendMessage.SASLInitialResponse(
            saslMechanism: "hello", initialData: [0, 1, 2, 3, 4, 5, 6, 7])
        let message = PSQLFrontendMessage.saslInitialResponse(sasl)
        encoder.encode(data: message, out: &byteBuffer)
        
        let length: Int = 1 + 4 + (sasl.saslMechanism.count + 1) + 4 + sasl.initialData.count

        //   1 id
        // + 4 length
        // + 6 saslMechanism (5 + 1 null terminator)
        // + 4 initialData length
        // + 8 initialData
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PSQLFrontendMessage.ID.saslInitialResponse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), sasl.saslMechanism)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(sasl.initialData.count))
        XCTAssertEqual(byteBuffer.readBytes(length: sasl.initialData.count), sasl.initialData)
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
    func testEncodeWithoutData() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        let sasl = PSQLFrontendMessage.SASLInitialResponse(
            saslMechanism: "hello", initialData: [])
        let message = PSQLFrontendMessage.saslInitialResponse(sasl)
        encoder.encode(data: message, out: &byteBuffer)
        
        let length: Int = 1 + 4 + (sasl.saslMechanism.count + 1) + 4 + sasl.initialData.count

        //   1 id
        // + 4 length
        // + 6 saslMechanism (5 + 1 null terminator)
        // + 4 initialData length
        // + 0 initialData
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PSQLFrontendMessage.ID.saslInitialResponse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), sasl.saslMechanism)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(-1))
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}
