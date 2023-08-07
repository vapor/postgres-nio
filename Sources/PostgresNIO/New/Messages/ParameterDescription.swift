import NIOCore

extension PostgresBackendMessage {
    
    struct ParameterDescription: PayloadDecodable, Hashable {
        /// Specifies the object ID of the parameter data type.
        var dataTypes: [PostgresDataType]
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let parameterCount = try buffer.throwingReadInteger(as: UInt16.self)
            
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
