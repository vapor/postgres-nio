import XCTest
import NIOCore
@testable import PostgresNIO

class CloseTests: XCTestCase {
    func testEncodeClosePortal() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.closePortal("Hello")
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 12)
        XCTAssertEqual(PostgresFrontendMessage.ID.close.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(11, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "P"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("Hello", byteBuffer.readNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
    func testEncodeCloseUnnamedStatement() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.closePreparedStatement("")
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 7)
        XCTAssertEqual(PostgresFrontendMessage.ID.close.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(6, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "S"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("", byteBuffer.readNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}
