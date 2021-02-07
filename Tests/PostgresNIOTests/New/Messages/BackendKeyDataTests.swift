import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class BackendKeyDataTests: XCTestCase {
    func testDecode() {
        let buffer = ByteBuffer.backendMessage(id: .backendKeyData) { buffer in
            buffer.writeInteger(Int32(1234))
            buffer.writeInteger(Int32(4567))
        }
        
        let expectedInOuts = [
            (buffer, [PSQLBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 4567))]),
        ]
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: expectedInOuts,
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) }))
    }
    
    func testDecodeInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessageID(.backendKeyData)
        buffer.writeInteger(Int32(11))
        buffer.writeInteger(Int32(1234))
        buffer.writeInteger(Int32(4567))
        
        let expected = [
            (buffer, [PSQLBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 4567))]),
        ]
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: expected,
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
}
