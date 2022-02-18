import NIOCore

extension PSQLBackendMessage {
    
    struct ParameterDescription: PayloadDecodable, Equatable {
        /// Specifies the object ID of the parameter data type.
        var dataTypes: [PostgresDataType]
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let parameterCount = try buffer.throwingReadInteger(as: Int16.self)
            guard parameterCount >= 0 else {
                throw PSQLPartialDecodingError.integerMustBePositiveOrNull(parameterCount)
            }
            
            var result = [PostgresDataType]()
            result.reserveCapacity(Int(parameterCount))
            
            for _ in 0..<parameterCount {
                let rawValue = try buffer.throwingReadInteger(as: UInt32.self)
                result.append(PostgresDataType(rawValue))
            }
            
            return ParameterDescription(dataTypes: result)
        }
    }
}
