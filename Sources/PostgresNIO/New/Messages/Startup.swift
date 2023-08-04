import NIOCore

extension PostgresFrontendMessage {
    struct Startup: Hashable {
        static let versionThree: Int32 = 0x00_03_00_00

        /// Creates a `Startup` with "3.0" as the protocol version.
        static func versionThree(parameters: Parameters) -> Startup {
            return .init(protocolVersion: Self.versionThree, parameters: parameters)
        }

        /// The protocol version number. The most significant 16 bits are the major
        /// version number (3 for the protocol described here). The least significant
        /// 16 bits are the minor version number (0 for the protocol described here).
        var protocolVersion: Int32
        
        /// The protocol version number is followed by one or more pairs of parameter
        /// name and value strings. A zero byte is required as a terminator after
        /// the last name/value pair. `user` is required, others are optional.
        struct Parameters: Hashable {
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
    }
}
