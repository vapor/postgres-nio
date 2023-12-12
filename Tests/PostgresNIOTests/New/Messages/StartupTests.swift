import XCTest
import NIOCore
@testable import PostgresNIO

class StartupTests: XCTestCase {
    
    func testStartupMessageWithDatabase() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()

        let user = "test"
        let database = "abc123"

        encoder.startup(user: user, database: database, options: [])
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

        encoder.startup(user: user, database: nil, options: [])
        byteBuffer = encoder.flushBuffer()

        let byteBufferLength = Int32(byteBuffer.readableBytes)
        XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
        XCTAssertEqual(PostgresFrontendMessage.Startup.versionThree, byteBuffer.readInteger())
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "user")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "test")
        XCTAssertEqual(byteBuffer.readInteger(), UInt8(0))

        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }

    func testStartupMessageWithAdditionalOptions() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()
        
        let user = "test"
        let database = "abc123"
        
        encoder.startup(user: user, database: database, options: [("some", "options")])
        byteBuffer = encoder.flushBuffer()
        
        let byteBufferLength = Int32(byteBuffer.readableBytes)
        XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
        XCTAssertEqual(PostgresFrontendMessage.Startup.versionThree, byteBuffer.readInteger())
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "user")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "test")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "database")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "abc123")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "some")
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), "options")
        XCTAssertEqual(byteBuffer.readInteger(), UInt8(0))
        
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
}

extension PostgresFrontendMessage.Startup.Parameters.Replication {
    var stringValue: String {
        switch self {
        case .true:
            return "true"
        case .false:
            return "false"
        case .database:
            return "replication"
        }
    }
}
