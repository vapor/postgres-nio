import NIO

extension PostgresMessage {
    /// Identifies the message as a parameter description.
    struct ParameterDescription {
        /// Parses an instance of this message type from a byte buffer.
        static func parse(from buffer: inout ByteBuffer) throws -> ParameterDescription {
            guard let dataTypes = try buffer.read(array: PostgresDataType.self, { buffer in
                guard let raw = buffer.readInteger(as: Int32.self) else {
                    throw PostgresError(.protocol("Could not parse data type integer in parameter description message."))
                }
                return .init(raw)
            }) else {
                throw PostgresError(.protocol("Could not parse data types in parameter description message."))
            }
            return .init(dataTypes: dataTypes)
        }
        
        /// Specifies the object ID of the parameter data type.
        var dataTypes: [PostgresDataType]
    }
}
