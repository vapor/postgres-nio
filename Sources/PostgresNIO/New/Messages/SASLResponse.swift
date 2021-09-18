import NIOCore

extension PSQLFrontendMessage {
    
    struct SASLResponse: PSQLMessagePayloadEncodable, Equatable {
        
        let data: [UInt8]
        
        /// Creates a new `SSLRequest`.
        init(data: [UInt8]) {
            self.data = data
        }
        
        /// Serializes this message into a byte buffer.
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeBytes(self.data)
        }
    }
}
