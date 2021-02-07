import NIO

extension PSQLBackendMessage {
    
    struct DataRow: PayloadDecodable, Equatable {
        
        var columns: [ByteBuffer?]
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            try PSQLBackendMessage.ensureAtLeastNBytesRemaining(2, in: buffer)
            let columnCount = buffer.readInteger(as: Int16.self)!
            
            var result = [ByteBuffer?]()
            result.reserveCapacity(Int(columnCount))
            
            for _ in 0..<columnCount {
                try PSQLBackendMessage.ensureAtLeastNBytesRemaining(2, in: buffer)
                let bufferLength = Int(buffer.readInteger(as: Int32.self)!)
                
                guard bufferLength > 0 else {
                    result.append(nil)
                    continue
                }
                
                try PSQLBackendMessage.ensureAtLeastNBytesRemaining(bufferLength, in: buffer)
                let columnBuffer = buffer.readSlice(length: Int(bufferLength))!
                
                result.append(columnBuffer)
            }
            
            return DataRow(columns: result)
        }
    }
}
