import NIOCore

extension PSQLBackendMessage {
    
    struct BackendKeyData: PayloadDecodable, Equatable {
        let processID: Int32
        let secretKey: Int32
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            try buffer.psqlEnsureExactNBytesRemaining(8)
            
            // We have verified the correct length before, this means we have exactly eight bytes
            // to read. If we have enough readable bytes, a read of Int32 should always succeed.
            // Therefore we can force unwrap here.
            let processID = buffer.readInteger(as: Int32.self)!
            let secretKey = buffer.readInteger(as: Int32.self)!
            
            return .init(processID: processID, secretKey: secretKey)
        }
    }
}

extension PSQLBackendMessage.BackendKeyData: CustomDebugStringConvertible {
    var debugDescription: String {
        "processID: \(processID), secretKey: \(secretKey)"
    }
}
