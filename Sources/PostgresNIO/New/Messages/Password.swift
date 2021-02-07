extension PSQLFrontendMessage {
    
    struct Password: PayloadEncodable, Equatable {
        let value: String
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeString(value)
            buffer.writeInteger(UInt8(0))
        }
    }
    
}
