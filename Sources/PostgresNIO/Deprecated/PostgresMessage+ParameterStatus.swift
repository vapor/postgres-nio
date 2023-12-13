import NIOCore

extension PostgresMessage {
    @available(*, deprecated, message: "Will be removed from public API")
    public struct ParameterStatus: CustomStringConvertible {
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> ParameterStatus {
            guard let parameter = buffer.readNullTerminatedString() else {
                throw PostgresError.protocol("Could not read parameter from parameter status message")
            }
            guard let value = buffer.readNullTerminatedString() else {
                throw PostgresError.protocol("Could not read value from parameter status message")
            }
            return .init(parameter: parameter, value: value)
        }
        
        /// The name of the run-time parameter being reported.
        public var parameter: String
        
        /// The current value of the parameter.
        public var value: String
        
        /// See `CustomStringConvertible`.
        public var description: String {
            return "\(parameter): \(value)"
        }
    }
}
