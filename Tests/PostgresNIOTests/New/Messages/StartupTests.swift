import XCTest
import NIOCore
@testable import PostgresNIO

class StartupTests: XCTestCase {
    
    func testStartupMessage() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()
        
        let replicationValues: [PostgresFrontendMessage.Startup.Parameters.Replication] = [
            .`true`,
            .`false`,
            .database
        ]
        
        for replication in replicationValues {
            let parameters = PostgresFrontendMessage.Startup.Parameters(
                user: "test",
                database: "abc123",
                options: "some options",
                replication: replication
            )
            
            encoder.startup(parameters)
            byteBuffer = encoder.flushBuffer()

            let byteBufferLength = Int32(byteBuffer.readableBytes)
            XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
            XCTAssertEqual(PostgresFrontendMessage.Startup.versionThree, byteBuffer.readInteger())
            XCTAssertEqual(byteBuffer.readNullTerminatedString(), "user")
            XCTAssertEqual(byteBuffer.readNullTerminatedString(), "test")
            XCTAssertEqual(byteBuffer.readNullTerminatedString(), "database")
            XCTAssertEqual(byteBuffer.readNullTerminatedString(), "abc123")
            XCTAssertEqual(byteBuffer.readNullTerminatedString(), "options")
            XCTAssertEqual(byteBuffer.readNullTerminatedString(), "some options")
            if replication != .false {
                XCTAssertEqual(byteBuffer.readNullTerminatedString(), "replication")
                XCTAssertEqual(byteBuffer.readNullTerminatedString(), replication.stringValue)
            }
            XCTAssertEqual(byteBuffer.readInteger(), UInt8(0))
            
            XCTAssertEqual(byteBuffer.readableBytes, 0)
        }
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
