import XCTest
import NIOCore
@testable import PostgresNIO

class StartupTests: XCTestCase {
    
    func testStartupMessageWithDatabase() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()

        let user = "test"
        let database = "abc123"

        encoder.startup(user: user, database: database)
        byteBuffer = encoder.flushBuffer()

        let byteBufferLength = Int32(byteBuffer.readableBytes)
        XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
        XCTAssertEqual(PostgresFrontendMessage.Startup.versionThree, byteBuffer.readInteger())
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "user")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "test")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "database")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "abc123")
        XCTAssertEqual(byteBuffer.readInteger(), UInt8(0))

        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }

    func testStartupMessageWithoutDatabase() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()

        let user = "test"

        encoder.startup(user: user, database: nil)
        byteBuffer = encoder.flushBuffer()

        let byteBufferLength = Int32(byteBuffer.readableBytes)
        XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
        XCTAssertEqual(PostgresFrontendMessage.Startup.versionThree, byteBuffer.readInteger())
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "user")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "test")
        XCTAssertEqual(byteBuffer.readInteger(), UInt8(0))

        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}
