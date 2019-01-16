import CMD5
import NIO

extension PostgresConnection {
    public func authenticate(username: String, database: String? = nil, password: String? = nil) -> EventLoopFuture<Void> {
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let handler = AuthenticationHandler(username: username, database: database, password: password, promise: promise)
        return self.channel.pipeline.add(handler: handler).then {
            return promise.futureResult
        }
    }
    
    // MARK: Private
    
    private final class AuthenticationHandler: PostgresConnectionHandler {
        typealias InboundIn = PostgresMessage
        typealias OutboundOut = PostgresMessage
        
        enum State {
            case ready
            case done
        }
        
        let username: String
        let database: String?
        let password: String?
        var state: State
        var promise: EventLoopPromise<Void>
        
        init(username: String, database: String?, password: String?, promise: EventLoopPromise<Void>) {
            self.state = .ready
            self.username = username
            self.database = database
            self.password = password
            self.promise = promise
        }
        
        func read(message: inout PostgresMessage, ctx: ChannelHandlerContext) throws {
            switch self.state {
            case .ready:
                switch message.identifier {
                case .authentication:
                    let auth = try PostgresMessage.Authentication.parse(from: &message)
                    switch auth {
                    case .md5(let salt):
                        let pwdhash = self.md5((self.password ?? "") + self.username).hexdigest()
                        let hash = "md5" + self.md5(self.bytes(pwdhash) + salt).hexdigest()
                        ctx.write(message: PostgresMessage.Password(string: hash), promise: nil)
                        ctx.flush()
                    case .plaintext:
                        ctx.write(message: PostgresMessage.Password(string: self.password ?? ""), promise: nil)
                        ctx.flush()
                    case .ok:
                        self.state = .done
                    }
                default: throw PostgresError(.protocol("Unexpected response to start message: \(message)"))
                }
            case .done:
                switch message.identifier {
                case .parameterStatus: break
                    // self.status[status.parameter] = status.value
                case .backendKeyData: break
                    // self.processID = data.processID
                    // self.secretKey = data.secretKey
                case .readyForQuery:
                    ctx.channel.pipeline.remove(handler: self, promise: nil)
                default: throw PostgresError(.protocol("Unexpected response to password authentication: \(message)"))
                }
            }
            
        }
        
        func errorCaught(ctx: ChannelHandlerContext, error: Error) {
            ctx.close(mode: .all, promise: nil)
            self.promise.fail(error: error)
        }
        
        func handlerAdded(ctx: ChannelHandlerContext) {
            ctx.write(message: PostgresMessage.Startup.versionThree(parameters: [
                "user": self.username,
                "database": self.database ?? username
            ]), promise: nil)
            ctx.flush()
        }
        
        func handlerRemoved(ctx: ChannelHandlerContext) {
            self.promise.succeed(result: ())
        }
        
        // MARK: Private
        
        private func md5(_ string: String) -> [UInt8] {
            return md5(self.bytes(string))
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

        func bytes(_ string: String) -> [UInt8] {
            return string.withCString { ptr in
                return UnsafeBufferPointer(start: ptr, count: string.count).withMemoryRebound(to: UInt8.self) { buffer in
                    return [UInt8](buffer)
                }
            }
        }
    }
}
