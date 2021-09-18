import NIOCore

extension PSQLBackendMessage {
    
    struct DataRow: PayloadDecodable, Equatable {
        
        var columns: [ByteBuffer?]
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            try buffer.ensureAtLeastNBytesRemaining(2)
            let columnCount = buffer.readInteger(as: Int16.self)!
            
            var result = [ByteBuffer?]()
            result.reserveCapacity(Int(columnCount))
            
            for _ in 0..<columnCount {
                try buffer.ensureAtLeastNBytesRemaining(2)
                let bufferLength = Int(buffer.readInteger(as: Int32.self)!)
                
                guard bufferLength >= 0 else {
                    result.append(nil)
                    continue
                }
                
                try buffer.ensureAtLeastNBytesRemaining(bufferLength)
                let columnBuffer = buffer.readSlice(length: Int(bufferLength))!
                
                result.append(columnBuffer)
            }
            
            return DataRow(columns: result)
        }
    }
}
