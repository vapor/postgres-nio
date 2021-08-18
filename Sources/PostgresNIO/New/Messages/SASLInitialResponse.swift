import NIOCore

extension PSQLFrontendMessage {
    
    struct SASLInitialResponse: PayloadEncodable, Equatable {
        
        let saslMechanism: String
        let initialData: [UInt8]
        
        /// Creates a new `SSLRequest`.
        init(saslMechanism: String, initialData: [UInt8]) {
            self.saslMechanism = saslMechanism
            self.initialData = initialData
        }
        
        /// Serializes this message into a byte buffer.
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(self.saslMechanism)
            
            if self.initialData.count > 0 {
                buffer.writeInteger(Int32(self.initialData.count))
                buffer.writeBytes(self.initialData)
            } else {
                buffer.writeInteger(Int32(-1))
            }
        }
    }
}
