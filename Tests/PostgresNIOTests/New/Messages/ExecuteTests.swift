import XCTest
import NIOCore
@testable import PostgresNIO

class ExecuteTests: XCTestCase {
    
    func testEncodeExecute() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.execute(portalName: "", maxNumberOfRows: 0)
        var byteBuffer = encoder.flushBuffer()

        XCTAssertEqual(byteBuffer.readableBytes, 10) // 1 (id) + 4 (length) + 1 (empty null terminated string) + 4 (count)
        XCTAssertEqual(PostgresFrontendMessage.ID.execute.rawValue, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(9, byteBuffer.readInteger(as: Int32.self)) // length
        XCTAssertEqual("", byteBuffer.readNullTerminatedString())
        XCTAssertEqual(0, byteBuffer.readInteger(as: Int32.self))
    }
}
