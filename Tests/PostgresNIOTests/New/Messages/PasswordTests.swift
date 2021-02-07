import XCTest
@testable import PostgresNIO

class PasswordTests: XCTestCase {
    
    func testEncodePassword() {
        let encoder = PSQLFrontendMessage.Encoder.forTests
        var byteBuffer = ByteBuffer()
        // md522d085ed8dc3377968dc1c1a40519a2a = "abc123" with salt 1, 2, 3, 4
        let message = PSQLFrontendMessage.password(.init(value: "md522d085ed8dc3377968dc1c1a40519a2a"))
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        let expectedLength = 41 // 1 (id) + 4 (length) + 35 (string) + 1 (null termination)
        
        XCTAssertEqual(byteBuffer.readableBytes, expectedLength)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PSQLFrontendMessage.ID.password.byte)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(expectedLength - 1)) // length
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "md522d085ed8dc3377968dc1c1a40519a2a")
    }
}
