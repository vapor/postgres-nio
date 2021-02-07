import XCTest
@testable import PostgresNIO

class StartupTests: XCTestCase {
    
    func testStartupMessage() {
        let encoder = PSQLFrontendMessage.Encoder.forTests
        var byteBuffer = ByteBuffer()
        
        let replicationValues: [PSQLFrontendMessage.Startup.Parameters.Replication] = [
            .`true`,
            .`false`,
            .database
        ]
        
        for replication in replicationValues {
            let parameters = PSQLFrontendMessage.Startup.Parameters(
                user: "test",
                database: "abc123",
                options: "some options",
                replication: replication
            )
            
            let startup = PSQLFrontendMessage.Startup.versionThree(parameters: parameters)
            let message = PSQLFrontendMessage.startup(startup)
            XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
            
            let byteBufferLength = Int32(byteBuffer.readableBytes)
            XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
            XCTAssertEqual(startup.protocolVersion, byteBuffer.readInteger())
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

extension PSQLFrontendMessage.Startup.Parameters.Replication {
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
