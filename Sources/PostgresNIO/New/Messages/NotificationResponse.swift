import NIOCore

extension PSQLBackendMessage {
    
    struct NotificationResponse: PayloadDecodable, Equatable {
        let backendPID: Int32
        let channel: String
        let payload: String
        
        static func decode(from buffer: inout ByteBuffer) throws -> PSQLBackendMessage.NotificationResponse {
            try buffer.psqlEnsureAtLeastNBytesRemaining(6)
            let backendPID = buffer.readInteger(as: Int32.self)!
            
            guard let channel = buffer.psqlReadNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            guard let payload = buffer.psqlReadNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            return NotificationResponse(backendPID: backendPID, channel: channel, payload: payload)
        }
    }
}
