import NIOCore

extension PSQLFrontendMessage {
    
    struct Password: PayloadEncodable, Equatable {
        let value: String
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(value)
        }
    }
    
}
