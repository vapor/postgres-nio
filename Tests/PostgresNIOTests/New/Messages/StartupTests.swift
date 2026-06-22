import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct StartupTests {
    @Test func testStartupMessageWithDatabase() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()

        let user = "test"
        let database = "abc123"

        encoder.startup(user: user, database: database, options: [])
        byteBuffer = encoder.flushBuffer()

        let byteBufferLength = Int32(byteBuffer.readableBytes)
        #expect(byteBufferLength == byteBuffer.readInteger())
        #expect(PostgresFrontendMessage.Startup.versionThree == byteBuffer.readInteger())
        #expect(byteBuffer.readNullTerminatedString() == "user")
        #expect(byteBuffer.readNullTerminatedString() == "test")
        #expect(byteBuffer.readNullTerminatedString() == "database")
        #expect(byteBuffer.readNullTerminatedString() == "abc123")
        #expect(byteBuffer.readInteger() == UInt8(0))

        #expect(byteBuffer.readableBytes == 0)
    }

    @Test func testStartupMessageWithoutDatabase() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()

        let user = "test"

        encoder.startup(user: user, database: nil, options: [])
        byteBuffer = encoder.flushBuffer()

        let byteBufferLength = Int32(byteBuffer.readableBytes)
        #expect(byteBufferLength == byteBuffer.readInteger())
        #expect(PostgresFrontendMessage.Startup.versionThree == byteBuffer.readInteger())
        #expect(byteBuffer.readNullTerminatedString() == "user")
        #expect(byteBuffer.readNullTerminatedString() == "test")
        #expect(byteBuffer.readInteger() == UInt8(0))

        #expect(byteBuffer.readableBytes == 0)
    }

    @Test func testStartupMessageWithAdditionalOptions() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        var byteBuffer = ByteBuffer()
        
        let user = "test"
        let database = "abc123"
        
        encoder.startup(user: user, database: database, options: [("some", "options")])
        byteBuffer = encoder.flushBuffer()
        
        let byteBufferLength = Int32(byteBuffer.readableBytes)
        #expect(byteBufferLength == byteBuffer.readInteger())
        #expect(PostgresFrontendMessage.Startup.versionThree == byteBuffer.readInteger())
        #expect(byteBuffer.readNullTerminatedString() == "user")
        #expect(byteBuffer.readNullTerminatedString() == "test")
        #expect(byteBuffer.readNullTerminatedString() == "database")
        #expect(byteBuffer.readNullTerminatedString() == "abc123")
        #expect(byteBuffer.readNullTerminatedString() == "some")
        #expect(byteBuffer.readNullTerminatedString() == "options")
        #expect(byteBuffer.readInteger() == UInt8(0))

        #expect(byteBuffer.readableBytes == 0)
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
