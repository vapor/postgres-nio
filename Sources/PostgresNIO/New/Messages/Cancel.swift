import NIOCore

extension PSQLFrontendMessage {
    
    struct Cancel: PSQLMessagePayloadEncodable, Equatable {
        /// The cancel request code. The value is chosen to contain 1234 in the most significant 16 bits,
        /// and 5678 in the least significant 16 bits. (To avoid confusion, this code must not be the same
        /// as any protocol version number.)
        let cancelRequestCode: Int32 = 80877102
        
        /// The process ID of the target backend.
        let processID: Int32
        
        /// The secret key for the target backend.
        let secretKey: Int32
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeMultipleIntegers(self.cancelRequestCode, self.processID, self.secretKey)
        }
    }
}
