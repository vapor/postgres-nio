import NIO

extension PostgresMessage {
    /// Authentication request returned by the server.
    public enum Authentication: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .authentication
        }
        
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> Authentication {
            guard let type = buffer.readInteger(as: Int32.self) else {
                throw PostgresError.protocol("Could not read authentication message type")
            }
            switch type {
            case 0: return .ok
            case 3: return .plaintext
            case 5:
                guard let salt = buffer.readBytes(length: 4) else {
                    throw PostgresError.protocol("Could not parse MD5 salt from authentication message")
                }
                return .md5(salt)
            default:
                throw PostgresError.protocol("Unkonwn authentication request type: \(type)")
            }
        }
        
        /// AuthenticationOk
        /// Specifies that the authentication was successful.
        case ok
        
        /// AuthenticationCleartextPassword
        /// Specifies that a clear-text password is required.
        case plaintext
        
        /// AuthenticationMD5Password
        /// Specifies that an MD5-encrypted password is required.
        case md5([UInt8])
        
        /// See `CustomStringConvertible`.
        public var description: String {
            switch self {
            case .ok: return "Ok"
            case .plaintext: return "CleartextPassword"
            case .md5(let salt): return "MD5Password(salt: 0x\(salt.hexdigest()))"
            }
        }
        
        public func serialize(into buffer: inout ByteBuffer) {
            fatalError()
        }
    }
}
