import NIO

extension PostgresMessage {
    struct ParameterStatus: CustomStringConvertible {
        /// Parses an instance of this message type from a byte buffer.
        static func parse(from buffer: inout ByteBuffer) throws -> ParameterStatus {
            guard let parameter = buffer.readNullTerminatedString() else {
                throw PostgresError(.protocol("Could not read parameter from parameter status message"))
            }
            guard let value = buffer.readNullTerminatedString() else {
                throw PostgresError(.protocol("Could not read value from parameter status message"))
            }
            return .init(parameter: parameter, value: value)
        }
        
        /// The name of the run-time parameter being reported.
        var parameter: String
        
        /// The current value of the parameter.
        var value: String
        
        /// See `CustomStringConvertible`.
        var description: String {
            return "\(parameter): \(value)"
        }
    }
}
