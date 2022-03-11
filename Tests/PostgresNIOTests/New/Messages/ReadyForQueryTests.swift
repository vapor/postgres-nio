import XCTest
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

class ReadyForQueryTests: XCTestCase {

    func testDecode() {
        var buffer = ByteBuffer()
        
        let states: [PostgresBackendMessage.TransactionState] = [
            .idle,
            .inFailedTransaction,
            .inTransaction,
        ]
        
        states.forEach { state in
            buffer.writeBackendMessage(id: .readyForQuery) { buffer in
                switch state {
                case .idle:
                    buffer.writeInteger(UInt8(ascii: "I"))
                case .inTransaction:
                    buffer.writeInteger(UInt8(ascii: "T"))
                case .inFailedTransaction:
                    buffer.writeInteger(UInt8(ascii: "E"))
                }
            }
        }
        
        let expected = states.map { state -> PostgresBackendMessage in
            .readyForQuery(state)
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected)],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }))

    }
    
    func testDecodeInvalidLength() {
        var buffer = ByteBuffer()
        
        buffer.writeBackendMessage(id: .readyForQuery) { buffer in
            buffer.writeInteger(UInt8(ascii: "I"))
            buffer.writeInteger(UInt8(ascii: "I"))
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLDecodingError)
        }
    }
    
    func testDecodeUnexpectedAscii() {
        var buffer = ByteBuffer()
        
        buffer.writeBackendMessage(id: .readyForQuery) { buffer in
            buffer.writeInteger(UInt8(ascii: "F"))
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLDecodingError)
        }
    }
    
    func testDebugDescription() {
        XCTAssertEqual(String(reflecting: PostgresBackendMessage.TransactionState.idle), ".idle")
        XCTAssertEqual(String(reflecting: PostgresBackendMessage.TransactionState.inTransaction), ".inTransaction")
        XCTAssertEqual(String(reflecting: PostgresBackendMessage.TransactionState.inFailedTransaction), ".inFailedTransaction")
    }
}
