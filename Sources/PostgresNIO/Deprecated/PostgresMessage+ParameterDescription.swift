import NIOCore

extension PostgresMessage {
    /// Identifies the message as a parameter description.
    @available(*, deprecated, message: "Will be removed from public API")
    public struct ParameterDescription {
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> ParameterDescription {
            guard let dataTypes = try buffer.read(array: PostgresDataType.self, { buffer in
                guard let dataType = buffer.readInteger(as: PostgresDataType.self) else {
                    throw PostgresError.protocol("Could not parse data type integer in parameter description message.")
                }
                return dataType
            }) else {
                throw PostgresError.protocol("Could not parse data types in parameter description message.")
            }
            return .init(dataTypes: dataTypes)
        }
        
        /// Specifies the object ID of the parameter data type.
        public var dataTypes: [PostgresDataType]
    }
}
