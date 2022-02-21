import XCTest
import NIOCore
@testable import PostgresNIO

class CloseTests: XCTestCase {
    
    func testEncodeClosePortal() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        let message = PSQLFrontendMessage.close(.portal("Hello"))
        encoder.encode(data: message, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 12)
        XCTAssertEqual(PSQLFrontendMessage.ID.close.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(11, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "P"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("Hello", byteBuffer.readNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
    func testEncodeCloseUnnamedStatement() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        let message = PSQLFrontendMessage.close(.preparedStatement(""))
        encoder.encode(data: message, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 7)
        XCTAssertEqual(PSQLFrontendMessage.ID.close.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(6, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "S"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("", byteBuffer.readNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
}
