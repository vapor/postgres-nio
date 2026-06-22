import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct ExecuteTests {
    @Test func testEncodeExecute() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.execute(portalName: "", maxNumberOfRows: 0)
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 10) // 1 (id) + 4 (length) + 1 (empty null terminated string) + 4 (count)
        #expect(PostgresFrontendMessage.ID.execute.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(9 == byteBuffer.readInteger(as: Int32.self)) // length
        #expect("" == byteBuffer.readNullTerminatedString())
        #expect(0 == byteBuffer.readInteger(as: Int32.self))
    }
}
