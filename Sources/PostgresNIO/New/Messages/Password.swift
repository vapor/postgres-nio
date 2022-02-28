import NIOCore

extension PostgresFrontendMessage {
    
    struct Password: PSQLMessagePayloadEncodable, Equatable {
        let value: String
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(value)
        }
    }
    
}
