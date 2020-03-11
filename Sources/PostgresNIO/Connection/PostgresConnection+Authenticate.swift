import Crypto
import NIO
import Logging

extension PostgresConnection {
    public func authenticate(
        username: String,
        database: String? = nil,
        password: String? = nil,
        logger: Logger = .init(label: "codes.vapor.postgres")
    ) -> EventLoopFuture<Void> {
        let auth = PostgresAuthenticationRequest(
            username: username,
            database: database,
            password: password
        )
        return self.send(auth, logger: self.logger)
    }
}

// MARK: Private

private final class PostgresAuthenticationRequest: PostgresRequest {
    enum State {
        case ready
        case saslInitialSent(SASLAuthenticationManager<SASLMechanism_SCRAM_SHA256>)
        case saslChallengeResponse(SASLAuthenticationManager<SASLMechanism_SCRAM_SHA256>)
        case saslWaitOkay
        case done
    }
    
    let username: String
    let database: String?
    let password: String?
    var state: State

    init(username: String, database: String?, password: String?) {
        self.state = .ready
        self.username = username
        self.database = database
        self.password = password
    }
    
    func log(to logger: Logger) {
        logger.debug("Logging into Postgres db \(self.database ?? "nil") as \(self.username)")
    }
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        if case .error = message.identifier {
            // terminate immediately on error
            return nil
        }
        
        switch self.state {
        case .ready:
            switch message.identifier {
            case .authentication:
                let auth = try PostgresMessage.Authentication(message: message)
                switch auth {
                case .md5(let salt):
                    let pwdhash = self.md5((self.password ?? "") + self.username).hexdigest()
                    let hash = "md5" + self.md5(self.bytes(pwdhash) + salt).hexdigest()
                    return try [PostgresMessage.Password(string: hash).message()]
                case .plaintext:
                    return try [PostgresMessage.Password(string: self.password ?? "").message()]
                case .saslMechanisms(let saslMechanisms):
                    if saslMechanisms.contains("SCRAM-SHA-256") && self.password != nil {
                        let saslManager = SASLAuthenticationManager(asClientSpeaking:
                            SASLMechanism_SCRAM_SHA256(username: self.username, password: { self.password! }))
                        var message: PostgresMessage?
                        
                        if (try saslManager.handle(message: nil, sender: { bytes in
                            message = try PostgresMessage.SASLInitialResponse(mechanism: "SCRAM-SHA-256", initialData: bytes).message()
                        })) {
                            self.state = .saslWaitOkay
                        } else {
                            self.state = .saslInitialSent(saslManager)
                        }
                        return [message].compactMap { $0 }
                    } else {
                        throw PostgresError.protocol("Unable to authenticate with any available SASL mechanism: \(saslMechanisms)")
                    }
                case .saslContinue, .saslFinal:
                    throw PostgresError.protocol("Unexpected SASL response to start message: \(message)")
                case .ok:
                    self.state = .done
                    return []
                }
            default: throw PostgresError.protocol("Unexpected response to start message: \(message)")
            }
        case .saslInitialSent(let manager),
             .saslChallengeResponse(let manager):
            switch message.identifier {
            case .authentication:
                let auth = try PostgresMessage.Authentication(message: message)
                switch auth {
                case .saslContinue(let data), .saslFinal(let data):
                    print(auth)
                    var message: PostgresMessage?
                    if try manager.handle(message: data, sender: { bytes in
                        message = try PostgresMessage.SASLResponse(responseData: bytes).message()
                    }) {
                        self.state = .saslWaitOkay
                    } else {
                        self.state = .saslChallengeResponse(manager)
                    }
                    return [message].compactMap { $0 }
                default: throw PostgresError.protocol("Unexpected response during SASL negotiation: \(message)")
                }
            default: throw PostgresError.protocol("Unexpected response during SASL negotiation: \(message)")
            }
        case .saslWaitOkay:
            switch message.identifier {
            case .authentication:
                let auth = try PostgresMessage.Authentication(message: message)
                switch auth {
                case .ok:
                    self.state = .done
                    return []
                default: throw PostgresError.protocol("Unexpected response while waiting for post-SASL ok: \(message)")
                }
            default: throw PostgresError.protocol("Unexpected response while waiting for post-SASL ok: \(message)")
            }
        case .done:
            switch message.identifier {
            case .parameterStatus:
                // self.status[status.parameter] = status.value
                return []
            case .backendKeyData:
                // self.processID = data.processID
                // self.secretKey = data.secretKey
                return []
            case .readyForQuery:
                return nil
            default: throw PostgresError.protocol("Unexpected response to password authentication: \(message)")
            }
        }
        
    }
    
    func start() throws -> [PostgresMessage] {
        return try [
            PostgresMessage.Startup.versionThree(parameters: [
                "user": self.username,
                "database": self.database ?? username
            ]).message()
        ]
    }
    
    // MARK: Private
    
    private func md5(_ string: String) -> [UInt8] {
        return md5(self.bytes(string))
    }
    
    private func md5(_ message: [UInt8]) -> [UInt8] {
        let digest = Insecure.MD5.hash(data: message)
        return .init(digest)
    }
    
    func bytes(_ string: String) -> [UInt8] {
        return Array(string.utf8)
    }
}
