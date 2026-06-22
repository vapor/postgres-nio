import Testing
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

@Suite struct BackendKeyDataTests {
    @Test func testDecode() {
        let buffer = ByteBuffer.backendMessage(id: .backendKeyData) { buffer in
            buffer.writeInteger(Int32(1234))
            buffer.writeInteger(Int32(4567))
        }
        
        let expectedInOuts = [
            (buffer, [PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 4567))]),
        ]
        
        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: expectedInOuts,
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
            )
        }
    }
    
    @Test func testDecodeInvalidLength() {
        var buffer = ByteBuffer()
        buffer.psqlWriteBackendMessageID(.backendKeyData)
        buffer.writeInteger(Int32(11))
        buffer.writeInteger(Int32(1234))
        buffer.writeInteger(Int32(4567))
        
        let expected = [
            (buffer, [PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 4567))]),
        ]
        
        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: expected,
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
            )
        }
    }
}
