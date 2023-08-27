import NIOCore

extension PostgresBackendMessage {
    
    struct NotificationResponse: PayloadDecodable, Hashable {
        let backendPID: Int32
        let channel: String
        let payload: String
        
        static func decode(from buffer: inout ByteBuffer) throws -> PostgresBackendMessage.NotificationResponse {
            let backendPID = try buffer.throwingReadInteger(as: Int32.self)
            
            guard let channel = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            guard let payload = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            return NotificationResponse(backendPID: backendPID, channel: channel, payload: payload)
        }
    }
}
