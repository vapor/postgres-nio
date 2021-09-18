import NIOCore

extension PSQLBackendMessage {
    
    struct ParameterDescription: PayloadDecodable, Equatable {
        /// Specifies the object ID of the parameter data type.
        var dataTypes: [PSQLDataType]
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            try buffer.ensureAtLeastNBytesRemaining(2)
            
            let parameterCount = buffer.readInteger(as: Int16.self)!
            guard parameterCount >= 0 else {
                throw PSQLPartialDecodingError.integerMustBePositiveOrNull(parameterCount)
            }
            
            try buffer.ensureExactNBytesRemaining(Int(parameterCount) * 4)
            
            var result = [PSQLDataType]()
            result.reserveCapacity(Int(parameterCount))
            
            for _ in 0..<parameterCount {
                let rawValue = buffer.readInteger(as: Int32.self)!
                result.append(PSQLDataType(rawValue))
            }
            
            return ParameterDescription(dataTypes: result)
        }
    }
}
