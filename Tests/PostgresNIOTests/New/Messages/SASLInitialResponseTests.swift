import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct SASLInitialResponseTests {

    @Test func testEncode() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let saslMechanism = "hello"
        let initialData: [UInt8] = [0, 1, 2, 3, 4, 5, 6, 7]
        encoder.saslInitialResponse(mechanism: saslMechanism, bytes: initialData)
        var byteBuffer = encoder.flushBuffer()
        
        let length: Int = 1 + 4 + (saslMechanism.count + 1) + 4 + initialData.count

        //   1 id
        // + 4 length
        // + 6 saslMechanism (5 + 1 null terminator)
        // + 4 initialData length
        // + 8 initialData
        
        #expect(byteBuffer.readableBytes == length)
        #expect(byteBuffer.readInteger(as: UInt8.self) == PostgresFrontendMessage.ID.saslInitialResponse.rawValue)
        #expect(byteBuffer.readInteger(as: Int32.self) == Int32(length - 1))
        #expect(byteBuffer.readNullTerminatedString() == saslMechanism)
        #expect(byteBuffer.readInteger(as: Int32.self) == Int32(initialData.count))
        #expect(byteBuffer.readBytes(length: initialData.count) == initialData)
        #expect(byteBuffer.readableBytes == 0)
    }
    
    @Test func testEncodeWithoutData() {
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        let saslMechanism = "hello"
        let initialData: [UInt8] = []
        encoder.saslInitialResponse(mechanism: saslMechanism, bytes: initialData)
        var byteBuffer = encoder.flushBuffer()
        
        let length: Int = 1 + 4 + (saslMechanism.count + 1) + 4 + initialData.count

        //   1 id
        // + 4 length
        // + 6 saslMechanism (5 + 1 null terminator)
        // + 4 initialData length
        // + 0 initialData
        
        #expect(byteBuffer.readableBytes == length)
        #expect(byteBuffer.readInteger(as: UInt8.self) == PostgresFrontendMessage.ID.saslInitialResponse.rawValue)
        #expect(byteBuffer.readInteger(as: Int32.self) == Int32(length - 1))
        #expect(byteBuffer.readNullTerminatedString() == saslMechanism)
        #expect(byteBuffer.readInteger(as: Int32.self) == Int32(-1))
        #expect(byteBuffer.readBytes(length: initialData.count) == initialData)
        #expect(byteBuffer.readableBytes == 0)
    }
}
