import NIO

extension PSQLBackendMessage {
    
    enum Authentication: PayloadDecodable {
        case ok
        case kerberosV5
        case md5(salt: (UInt8, UInt8, UInt8, UInt8))
        case plaintext
        case scmCredential
        case gss
        case sspi
        case gssContinue(data: ByteBuffer)
        case sasl(names: [String])
        case saslContinue(data: ByteBuffer)
        case saslFinal(data: ByteBuffer)
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            try PSQLBackendMessage.ensureAtLeastNBytesRemaining(2, in: buffer)
            
            // we have at least two bytes remaining, therefore we can force unwrap this read.
            let authID = buffer.readInteger(as: Int32.self)!
            
            switch authID {
            case 0:
                return .ok
            case 2:
                return .kerberosV5
            case 3:
                return .plaintext
            case 5:
                try PSQLBackendMessage.ensureExactNBytesRemaining(4, in: buffer)
                let salt1 = buffer.readInteger(as: UInt8.self)!
                let salt2 = buffer.readInteger(as: UInt8.self)!
                let salt3 = buffer.readInteger(as: UInt8.self)!
                let salt4 = buffer.readInteger(as: UInt8.self)!
                return .md5(salt: (salt1, salt2, salt3, salt4))
            case 6:
                return .scmCredential
            case 7:
                return .gss
            case 8:
                let data = buffer.readSlice(length: buffer.readableBytes)!
                return .gssContinue(data: data)
            case 9:
                return .sspi
            case 10:
                var names = [String]()
                let endIndex = buffer.readerIndex + buffer.readableBytes
                while buffer.readerIndex < endIndex, let next = buffer.readNullTerminatedString() {
                    names.append(next)
                }
                
                return .sasl(names: names)
            case 11:
                let data = buffer.readSlice(length: buffer.readableBytes)!
                return .saslContinue(data: data)
            case 12:
                let data = buffer.readSlice(length: buffer.readableBytes)!
                return .saslFinal(data: data)
            default:
                throw PartialDecodingError.unexpectedValue(value: authID)
            }
        }
        
    }
}

extension PSQLBackendMessage.Authentication: Equatable {
    static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.ok, .ok):
            return true
        case (.kerberosV5, .kerberosV5):
            return true
        case (.md5(let lhs), .md5(let rhs)):
            return lhs == rhs
        case (.plaintext, .plaintext):
            return true
        case (.scmCredential, .scmCredential):
            return true
        case (.gss, .gss):
            return true
        case (.sspi, .sspi):
            return true
        case (.gssContinue(let lhs), .gssContinue(let rhs)):
            return lhs == rhs
        case (.sasl(let lhs), .sasl(let rhs)):
            return lhs == rhs
        case (.saslContinue(let lhs), .saslContinue(let rhs)):
            return lhs == rhs
        case (.saslFinal(let lhs), .saslFinal(let rhs)):
            return lhs == rhs
        default:
            return false
        }
    }
}

extension PSQLBackendMessage.Authentication: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .ok:
            return ".ok"
        case .kerberosV5:
            return ".kerberosV5"
        case .md5(salt: let salt):
            return ".md5(salt: \(String(reflecting: salt)))"
        case .plaintext:
            return ".plaintext"
        case .scmCredential:
            return ".scmCredential"
        case .gss:
            return ".gss"
        case .sspi:
            return ".sspi"
        case .gssContinue(data: let data):
            return ".gssContinue(data: \(String(reflecting: data)))"
        case .sasl(names: let names):
            return ".sasl(names: \(String(reflecting: names)))"
        case .saslContinue(data: let data):
            return ".saslContinue(salt: \(String(reflecting: data)))"
        case .saslFinal(data: let data):
            return ".saslFinal(salt: \(String(reflecting: data)))"
        }
    }
}
