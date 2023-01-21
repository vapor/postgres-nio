import XCTest
import NIOCore
@testable import PostgresNIO

class BindTests: XCTestCase {
    
    func testEncodeBind() {
        let encoder = PSQLFrontendMessageEncoder()
        var bindings = PostgresBindings()
        bindings.append("Hello", context: .default)
        bindings.append("World", context: .default)
        var byteBuffer = ByteBuffer()
        let bind = PostgresFrontendMessage.Bind(portalName: "", preparedStatementName: "", bind: bindings)
        let message = PostgresFrontendMessage.bind(bind)
        encoder.encode(data: message, out: &byteBuffer)
        
        XCTAssertEqual(byteBuffer.readableBytes, 37)
        XCTAssertEqual(PostgresFrontendMessage.ID.bind.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), 36)
        XCTAssertEqual("", byteBuffer.readNullTerminatedString())
        XCTAssertEqual("", byteBuffer.readNullTerminatedString())
        // the number of parameters
        XCTAssertEqual(2, byteBuffer.readInteger(as: Int16.self))
        // all (two) parameters have the same format (binary)
        XCTAssertEqual(1, byteBuffer.readInteger(as: Int16.self))
        XCTAssertEqual(1, byteBuffer.readInteger(as: Int16.self))
        
        // read number of parameters
        XCTAssertEqual(2, byteBuffer.readInteger(as: Int16.self))
        
        // hello length
        XCTAssertEqual(5, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual("Hello", byteBuffer.readString(length: 5))
        
        // world length
        XCTAssertEqual(5, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual("World", byteBuffer.readString(length: 5))
        
        // all response values have the same format: therefore one format byte is next
        XCTAssertEqual(1, byteBuffer.readInteger(as: Int16.self))
        // all response values have the same format (binary)
        XCTAssertEqual(1, byteBuffer.readInteger(as: Int16.self))
        
        // nothing left to read
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}
