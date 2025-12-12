import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct BindTests {

    @Test func testEncodeBind() {
        var bindings = PostgresBindings()
        bindings.append("Hello", context: .default)
        bindings.append("World", context: .default)

        var encoder = PostgresFrontendMessageEncoder(buffer: .init())

        encoder.bind(portalName: "", preparedStatementName: "", bind: bindings)
        var byteBuffer = encoder.flushBuffer()

        #expect(byteBuffer.readableBytes == 37)
        #expect(PostgresFrontendMessage.ID.bind.rawValue == byteBuffer.readInteger(as: UInt8.self))
        #expect(byteBuffer.readInteger(as: Int32.self) == 36)
        #expect("" == byteBuffer.readNullTerminatedString())
        #expect("" == byteBuffer.readNullTerminatedString())
        // the number of parameters
        #expect(2 == byteBuffer.readInteger(as: Int16.self))
        // all (two) parameters have the same format (binary)
        #expect(1 == byteBuffer.readInteger(as: Int16.self))
        #expect(1 == byteBuffer.readInteger(as: Int16.self))

        // read number of parameters
        #expect(2 == byteBuffer.readInteger(as: Int16.self))

        // hello length
        #expect(5 == byteBuffer.readInteger(as: Int32.self))
        #expect("Hello" == byteBuffer.readString(length: 5))

        // world length
        #expect(5 == byteBuffer.readInteger(as: Int32.self))
        #expect("World" == byteBuffer.readString(length: 5))

        // all response values have the same format: therefore one format byte is next
        #expect(1 == byteBuffer.readInteger(as: Int16.self))
        // all response values have the same format (binary)
        #expect(1 == byteBuffer.readInteger(as: Int16.self))

        // nothing left to read
        #expect(byteBuffer.readableBytes == 0)
    }
}
