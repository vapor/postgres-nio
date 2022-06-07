import NIOCore

extension PostgresBackendMessage {
    
    struct ParameterDescription: PayloadDecodable, Equatable {
        /// Specifies the object ID of the parameter data type.
        var dataTypes: [PostgresDataType]
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let parameterCount = try buffer.throwingReadInteger(as: Int32.self)
            guard parameterCount >= 0, parameterCount < Int(UInt16.max) else {
                throw PSQLPartialDecodingError.integerMustBePositiveAndLessThanOrNull(
                  parameterCount, lessThan: Int(UInt16.max))
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
