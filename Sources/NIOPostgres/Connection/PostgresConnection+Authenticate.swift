import CMD5
import NIO

extension PostgresConnection {
    public func authenticate(username: String, database: String? = nil, password: String? = nil) -> EventLoopFuture<Void> {
        var auth: PostgresMessage.Authentication?
        return handler.send([.startup(.versionThree(parameters: [
            "user": username,
            "database": database ?? username
        ]))]) { message in
            switch message {
            case .authentication(let r): auth = r
            default: throw PostgresError(.protocol("Unexpected response to start message: \(message)"))
            }
            return true
        }.then {
            guard let r = auth else {
                // should be unhittable
                fatalError("No authentication request.")
            }
            let res: [PostgresMessage]
            switch r {
            case .md5(let salt):
                let pwdhash = md5((password ?? "") + username).hexdigest()
                let hash = "md5" + md5(pwdhash.bytes() + salt).hexdigest()
                res = [.password(.init(string: hash))]
            case .plaintext:
                res = [.password(.init(string: password ?? ""))]
            case .ok:
                res = []
            }
            
            return self.handler.send(res) { message in
                switch message {
                case .authentication(let r): 
                    switch r {
                    case .ok: return false
                    default: throw PostgresError(.protocol("Unexpected response to password authentication: \(r)"))
                    }
                case .parameterStatus(let status):
                    self.status[status.parameter] = status.value
                    return false
                case .backendKeyData(let data):
                    self.processID = data.processID
                    self.secretKey = data.secretKey
                    return false
                case .readyForQuery:
                    return true
                default: throw PostgresError(.protocol("Unexpected response to password authentication: \(message)"))
                }
            }
        }
    }
}

// MARK: MD5

private extension String {
    func bytes() -> [UInt8] {
        return withCString { ptr in
            return UnsafeBufferPointer(start: ptr, count: count).withMemoryRebound(to: UInt8.self) { buffer in
                return [UInt8](buffer)
            }
        }
    }
}

private func md5(_ string: String) -> [UInt8] {
    return md5(string.bytes())
}

private func md5(_ message: [UInt8]) -> [UInt8] {
    var message = message
    var ctx = MD5_CTX()
    MD5_Init(&ctx)
    MD5_Update(&ctx, &message, numericCast(message.count))
    var digest = [UInt8](repeating: 0, count: 16)
    MD5_Final(&digest, &ctx)
    return digest
}
