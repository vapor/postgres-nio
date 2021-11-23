import NIOCore

extension PSQLBackendMessage {
    
    struct ParameterStatus: PayloadDecodable, Equatable {
        /// The name of the run-time parameter being reported.
        var parameter: String
        
        /// The current value of the parameter.
        var value: String
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {            
            guard let name = buffer.psqlReadNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            guard let value = buffer.psqlReadNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            return ParameterStatus(parameter: name, value: value)
        }
    }
}

extension PSQLBackendMessage.ParameterStatus: CustomDebugStringConvertible {
    var debugDescription: String {
        "parameter: \(String(reflecting: self.parameter)), value: \(String(reflecting: self.value))"
    }
}

