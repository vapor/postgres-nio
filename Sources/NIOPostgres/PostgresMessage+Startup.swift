import NIO

extension PostgresMessage {
    /// First message sent from the frontend during startup.
    struct Startup {
        /// Creates a `Startup` with "3.0" as the protocol version.
        static func versionThree(parameters: [String: String]) -> Startup {
            return .init(protocolVersion: 0x00_03_00_00, parameters: parameters)
        }
        
        /// The protocol version number. The most significant 16 bits are the major
        /// version number (3 for the protocol described here). The least significant
        /// 16 bits are the minor version number (0 for the protocol described here).
        var protocolVersion: Int32
        
        /// The protocol version number is followed by one or more pairs of parameter
        /// name and value strings. A zero byte is required as a terminator after
        /// the last name/value pair. Parameters can appear in any order. user is required,
        /// others are optional. Each parameter is specified as:
        var parameters: [String: String]
        
        /// Creates a new `PostgreSQLStartupMessage`.
        init(protocolVersion: Int32, parameters: [String: String]) {
            self.protocolVersion = protocolVersion
            self.parameters = parameters
        }
        
        /// Serializes this message into a byte buffer.
        func serialize(into buffer: inout ByteBuffer) {
            buffer.write(integer: protocolVersion)
            for (key, val) in parameters {
                buffer.write(string: key + "\0")
                buffer.write(string: val + "\0")
            }
            // terminator
            buffer.write(string: "\0")
        }
    }
}
