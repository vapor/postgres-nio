import NIOCore

extension PostgresMessage {
    /// Identifies the message as a Close Command
    @available(*, deprecated, message: "Will be removed from public API")
    public struct Close {
        public static var identifier: PostgresMessage.Identifier {
            return .close
        }

        /// Close Target. Determines if the Close command should close a prepared statement
        /// or portal.
        public enum Target: Int8 {
            case preparedStatement = 0x53 // 'S' - prepared statement
            case portal = 0x50 // 'P' - portal
        }

        /// Determines if the `name` identifes a portal or a prepared statement
        public var target: Target

        /// The name of the prepared statement or portal to describe
        /// (an empty string selects the unnamed prepared statement or portal).
        public var name: String


        /// See `CustomStringConvertible`.
        public var description: String {
            switch target {
                case .preparedStatement: return "Statement(\(name))"
                case .portal: return "Portal(\(name))"
            }
        }

        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(target.rawValue)
            buffer.writeNullTerminatedString(name)
        }
    }
}
