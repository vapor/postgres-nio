import Testing
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

@Suite
struct ReadyForQueryTests {
    @Test func decode() {
        var buffer = ByteBuffer()
        
        let states: [PostgresBackendMessage.TransactionState] = [
            .idle,
            .inFailedTransaction,
            .inTransaction,
        ]

        for state in states {
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
        
        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, expected)],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }
    
    @Test func decodeInvalidLength() {
        var buffer = ByteBuffer()
        
        buffer.writeBackendMessage(id: .readyForQuery) { buffer in
            buffer.writeInteger(UInt8(ascii: "I"))
            buffer.writeInteger(UInt8(ascii: "I"))
        }
        
        #expect(throws: PostgresMessageDecodingError.self) { 
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }
    
    @Test func decodeUnexpectedAscii() {
        var buffer = ByteBuffer()
        
        buffer.writeBackendMessage(id: .readyForQuery) { buffer in
            buffer.writeInteger(UInt8(ascii: "F"))
        }

        #expect(throws: PostgresMessageDecodingError.self) { 
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }
    
    @Test func debugDescription() {
        #expect(String(reflecting: PostgresBackendMessage.TransactionState.idle) == ".idle")
        #expect(String(reflecting: PostgresBackendMessage.TransactionState.inTransaction) == ".inTransaction")
        #expect(String(reflecting: PostgresBackendMessage.TransactionState.inFailedTransaction) == ".inFailedTransaction")
    }
}
