import NIO

extension PostgresMessage {
    /// Identifies the message as a Parse command.
    public struct Parse: PostgresMessageType {
        public static func parse(from buffer: inout ByteBuffer) throws -> PostgresMessage.Parse {
            fatalError()
        }
        
        public static var identifier: PostgresMessage.Identifier {
            return .parse
        }
        
        public var description: String {
            return "Parse(\(self.query))"
        }
        
        /// The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
        public var statementName: String
        
        /// The query string to be parsed.
        public var query: String
        
        /// The number of parameter data types specified (can be zero).
        /// Note that this is not an indication of the number of parameters that might appear in the
        /// query string, only the number that the frontend wants to prespecify types for.
        /// Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
        public var parameterTypes: [PostgresFormatCode]
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.write(string: statementName + "\0")
            buffer.write(string: query + "\0")
            buffer.write(integer: numericCast(self.parameterTypes.count), as: Int16.self)
            self.parameterTypes.forEach { buffer.write(integer: $0.rawValue) }
        }
    }
}
