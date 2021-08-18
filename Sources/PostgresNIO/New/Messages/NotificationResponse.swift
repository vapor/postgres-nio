import NIOCore

extension PSQLBackendMessage {
    
    struct NotificationResponse: PayloadDecodable, Equatable {
        let backendPID: Int32
        let channel: String
        let payload: String
        
        static func decode(from buffer: inout ByteBuffer) throws -> PSQLBackendMessage.NotificationResponse {
            try PSQLBackendMessage.ensureAtLeastNBytesRemaining(6, in: buffer)
            let backendPID = buffer.readInteger(as: Int32.self)!
            
            guard let channel = buffer.readNullTerminatedString() else {
                throw PartialDecodingError.fieldNotDecodable(type: String.self)
            }
            guard let payload = buffer.readNullTerminatedString() else {
                throw PartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            return NotificationResponse(backendPID: backendPID, channel: channel, payload: payload)
        }
    }
}
