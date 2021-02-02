import NIO

extension PSQLFrontendMessage {
    struct Startup: PayloadEncodable, Equatable {

        /// Creates a `Startup` with "3.0" as the protocol version.
        static func versionThree(parameters: Parameters) -> Startup {
            return .init(protocolVersion: 0x00_03_00_00, parameters: parameters)
        }
        
        /// The protocol version number. The most significant 16 bits are the major
        /// version number (3 for the protocol described here). The least significant
        /// 16 bits are the minor version number (0 for the protocol described here).
        var protocolVersion: Int32
        
        /// The protocol version number is followed by one or more pairs of parameter
        /// name and value strings. A zero byte is required as a terminator after
        /// the last name/value pair. `user` is required, others are optional.
        struct Parameters: Equatable {
            enum Replication {
                case `true`
                case `false`
                case database
            }
            
            /// The database user name to connect as. Required; there is no default.
            var user: String
            
            /// The database to connect to. Defaults to the user name.
            var database: String?
            
            /// Command-line arguments for the backend. (This is deprecated in favor
            /// of setting individual run-time parameters.) Spaces within this string are
            /// considered to separate arguments, unless escaped with a
            /// backslash (\); write \\ to represent a literal backslash.
            var options: String?
            
            /// Used to connect in streaming replication mode, where a small set of
            /// replication commands can be issued instead of SQL statements. Value
            /// can be true, false, or database, and the default is false.
            var replication: Replication
        }
        var parameters: Parameters
        
        /// Creates a new `PostgreSQLStartupMessage`.
        init(protocolVersion: Int32, parameters: Parameters) {
            self.protocolVersion = protocolVersion
            self.parameters = parameters
        }
        
        /// Serializes this message into a byte buffer.
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeInteger(self.protocolVersion)
            buffer.writeNullTerminatedString("user")
            buffer.writeString(self.parameters.user)
            buffer.writeInteger(UInt8(0))
            
            if let database = self.parameters.database {
                buffer.writeNullTerminatedString("database")
                buffer.writeString(database)
                buffer.writeInteger(UInt8(0))
            }
            
            if let options = self.parameters.options {
                buffer.writeNullTerminatedString("options")
                buffer.writeString(options)
                buffer.writeInteger(UInt8(0))
            }
            
            switch self.parameters.replication {
            case .database:
                buffer.writeNullTerminatedString("replication")
                buffer.writeNullTerminatedString("replication")
            case .true:
                buffer.writeNullTerminatedString("replication")
                buffer.writeNullTerminatedString("true")
            case .false:
                break
            }
            
            buffer.writeInteger(UInt8(0))
        }
    }

}
