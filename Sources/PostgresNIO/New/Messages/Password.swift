import NIOCore

extension PSQLFrontendMessage {
    
    struct Password: PSQLMessagePayloadEncodable, Equatable {
        let value: String
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.psqlWriteNullTerminatedString(value)
        }
    }
    
}
