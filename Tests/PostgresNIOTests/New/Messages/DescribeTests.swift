import XCTest
import NIOCore
@testable import PostgresNIO

class DescribeTests: XCTestCase {
    
    func testEncodeDescribePortal() {
        let encoder = PSQLFrontendMessageEncoder.forTests
        var byteBuffer = ByteBuffer()
        let message = PSQLFrontendMessage.describe(.portal("Hello"))
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        XCTAssertEqual(byteBuffer.readableBytes, 12)
        XCTAssertEqual(PSQLFrontendMessage.ID.describe.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(11, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "P"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("Hello", byteBuffer.psqlReadNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
    func testEncodeDescribeUnnamedStatement() {
        let encoder = PSQLFrontendMessageEncoder.forTests
        var byteBuffer = ByteBuffer()
        let message = PSQLFrontendMessage.describe(.preparedStatement(""))
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        XCTAssertEqual(byteBuffer.readableBytes, 7)
        XCTAssertEqual(PSQLFrontendMessage.ID.describe.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(6, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "S"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("", byteBuffer.psqlReadNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }

}
