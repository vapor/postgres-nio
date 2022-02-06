import XCTest
import NIOCore
@testable import PostgresNIO

class PSQLFrontendMessageTests: XCTestCase {
    
    // MARK: ID
    
    func testMessageIDs() {
        XCTAssertEqual(PSQLFrontendMessage.ID.bind.rawValue, UInt8(ascii: "B"))
        XCTAssertEqual(PSQLFrontendMessage.ID.close.rawValue, UInt8(ascii: "C"))
        XCTAssertEqual(PSQLFrontendMessage.ID.describe.rawValue, UInt8(ascii: "D"))
        XCTAssertEqual(PSQLFrontendMessage.ID.execute.rawValue, UInt8(ascii: "E"))
        XCTAssertEqual(PSQLFrontendMessage.ID.flush.rawValue, UInt8(ascii: "H"))
        XCTAssertEqual(PSQLFrontendMessage.ID.parse.rawValue, UInt8(ascii: "P"))
        XCTAssertEqual(PSQLFrontendMessage.ID.password.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PSQLFrontendMessage.ID.saslInitialResponse.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PSQLFrontendMessage.ID.saslResponse.rawValue, UInt8(ascii: "p"))
        XCTAssertEqual(PSQLFrontendMessage.ID.sync.rawValue, UInt8(ascii: "S"))
        XCTAssertEqual(PSQLFrontendMessage.ID.terminate.rawValue, UInt8(ascii: "X"))
    }
    
    // MARK: Encoder
    
    func testEncodeFlush() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        encoder.encode(data: .flush, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PSQLFrontendMessage.ID.flush.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }
    
    func testEncodeSync() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        encoder.encode(data: .sync, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PSQLFrontendMessage.ID.sync.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }
    
    func testEncodeTerminate() {
        let encoder = PSQLFrontendMessageEncoder()
        var byteBuffer = ByteBuffer()
        encoder.encode(data: .terminate, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 5)
        XCTAssertEqual(PSQLFrontendMessage.ID.terminate.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(4, byteBuffer.readInteger(as: Int32.self)) // payload length
    }

}

extension PSQLBindings: ExpressibleByArrayLiteral {
    public typealias ArrayLiteralElement = PSQLEncodable

    public init(arrayLiteral elements: PSQLEncodable...) {
        self = PSQLBindings()
        do {
            try elements.forEach {
                try self._append($0, context: .default)
            }
        } catch {
            preconditionFailure("Can not create PSQLBindings")
        }
    }
}

