import NIOCore

extension PostgresBackendMessage {
    
    struct ParameterStatus: PayloadDecodable, Hashable {
        /// The name of the run-time parameter being reported.
        var parameter: String
        
        /// The current value of the parameter.
        var value: String
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {            
            guard let name = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            guard let value = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            return ParameterStatus(parameter: name, value: value)
        }
    }
}

extension PostgresBackendMessage.ParameterStatus: CustomDebugStringConvertible {
    var debugDescription: String {
        "parameter: \(String(reflecting: self.parameter)), value: \(String(reflecting: self.value))"
    }
}

