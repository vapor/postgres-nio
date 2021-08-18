import NIOCore

extension PostgresMessage {
    /// First message sent from the frontend during startup.
    public struct Startup: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .none
        }
        
        public var description: String {
            return "Startup()"
        }
        
        /// Creates a `Startup` with "3.0" as the protocol version.
        public static func versionThree(parameters: [String: String]) -> Startup {
            return .init(protocolVersion: 0x00_03_00_00, parameters: parameters)
        }
        
        /// The protocol version number. The most significant 16 bits are the major
        /// version number (3 for the protocol described here). The least significant
        /// 16 bits are the minor version number (0 for the protocol described here).
        public var protocolVersion: Int32
        
        /// The protocol version number is followed by one or more pairs of parameter
        /// name and value strings. A zero byte is required as a terminator after
        /// the last name/value pair. Parameters can appear in any order. user is required,
        /// others are optional. Each parameter is specified as:
        public var parameters: [String: String]
        
        /// Creates a new `PostgreSQLStartupMessage`.
        public init(protocolVersion: Int32, parameters: [String: String]) {
            self.protocolVersion = protocolVersion
            self.parameters = parameters
        }
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.writeInteger(self.protocolVersion)
            for (key, val) in parameters {
                buffer.writeString(key + "\0")
                buffer.writeString(val + "\0")
            }
            // terminator
            buffer.writeString("\0")
        }
    }
}
