import XCTest
import NIOCore
@testable import PostgresNIO

class PSQLFrontendMessageTests: XCTestCase {
    
    // MARK: ID
    
    func testMessageIDs() {
        XCTAssertEqual(PostgresFrontendMessage.ID.bind.rawValue, UInt8(ascii: "B"))
        XCTAssertEqual(PostgresFrontendMessage.ID.close.rawValue, UInt8(ascii: "C"))
        XCTAssertEqual(PostgresFrontendMessage.ID.describe.rawValue, UInt8(ascii: "D"))
        XCTAssertEqual(PostgresFrontendMessage.ID.execute.rawValue, UInt8(ascii: "E"))
        XCTAssertEqual(PostgresFrontendMessage.ID.flush.rawValue, UInt8(ascii: "H"))
        XCTAssertEqual(PostgresFrontendMessage.ID.parse.rawValue, UInt8(ascii: "P"))
        XCTAssertEqual(PostgresFrontendMessage.ID.password.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PostgresFrontendMessage.ID.saslInitialResponse.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PostgresFrontendMessage.ID.saslResponse.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PostgresFrontendMessage.ID.sync.rawValue, UInt8(ascii: "S"))
        XCTAssertEqual(PostgresFrontendMessage.ID.terminate.rawValue, UInt8(ascii: "X"))
    }
    
    // MARK: Encoder
    
    func testEncodeFlush() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        encoder.encode(data: .flush, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.flush.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }
    
    func testEncodeSync() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        encoder.encode(data: .sync, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.sync.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }
    
    func testEncodeTerminate() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        encoder.encode(data: .terminate, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PostgresFrontendMessage.ID.terminate.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }

}
