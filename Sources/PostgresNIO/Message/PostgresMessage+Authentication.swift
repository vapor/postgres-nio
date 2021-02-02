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
            case 10:
                var mechanisms: [String] = []
                while buffer.readableBytes > 0 {
                    guard let nextString = buffer.readNullTerminatedString() else {
                        throw PostgresError.protocol("Could not parse SASL mechanisms from authentication message")
                    }
                    if nextString.isEmpty {
                        break
                    }
                    mechanisms.append(nextString)
                }
                guard buffer.readableBytes == 0 else {
                    throw PostgresError.protocol("Trailing data at end of SASL mechanisms authentication message")
                }
                return .saslMechanisms(mechanisms)
            case 11:
                guard let challengeData = buffer.readBytes(length: buffer.readableBytes) else {
                    throw PostgresError.protocol("Could not parse SASL challenge from authentication message")
                }
                return .saslContinue(challengeData)
            case 12:
                guard let finalData = buffer.readBytes(length: buffer.readableBytes) else {
                    throw PostgresError.protocol("Could not parse SASL final data from authentication message")
                }
                return .saslFinal(finalData)
            
            case 2, 7...9:
                throw PostgresError.protocol("Support for KRBv5, GSSAPI, and SSPI authentication are not implemented")
            case 6:
                throw PostgresError.protocol("Support for SCM credential authentication is obsolete")

            default:
                throw PostgresError.protocol("Unknown authentication request type: \(type)")
            }
        }

        public func serialize(into buffer: inout ByteBuffer) throws {
            switch self {
            case .ok:
                buffer.writeInteger(0, as: Int32.self)
            case .plaintext:
                buffer.writeInteger(3, as: Int32.self)
            case .md5(let salt):
                buffer.writeInteger(5, as: Int32.self)
                buffer.writeBytes(salt)
            case .saslMechanisms(let mechanisms):
                buffer.writeInteger(10, as: Int32.self)
                mechanisms.forEach {
                    buffer.writeNullTerminatedString($0)
                }
            case .saslContinue(let challenge):
                buffer.writeInteger(11, as: Int32.self)
                buffer.writeBytes(challenge)
            case .saslFinal(let data):
                buffer.writeInteger(12, as: Int32.self)
                buffer.writeBytes(data)
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
        
        /// AuthenticationSASL
        /// Specifies the start of SASL mechanism negotiation.
        case saslMechanisms([String])
        
        /// AuthenticationSASLContinue
        /// Specifies SASL mechanism-specific challenge data.
        case saslContinue([UInt8])
        
        /// AuthenticationSASLFinal
        /// Specifies mechanism-specific post-authentication client data.
        case saslFinal([UInt8])
        
        /// See `CustomStringConvertible`.
        public var description: String {
            switch self {
            case .ok: return "Ok"
            case .plaintext: return "CleartextPassword"
            case .md5(let salt): return "MD5Password(salt: 0x\(salt.hexdigest()))"
            case .saslMechanisms(let mech): return "SASLMechanisms(\(mech))"
            case .saslContinue(let data): return "SASLChallenge(\(data))"
            case .saslFinal(let data): return "SASLFinal(\(data))"
            }
        }
    }
}
