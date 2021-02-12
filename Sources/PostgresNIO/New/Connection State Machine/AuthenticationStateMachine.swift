import NIO

struct AuthenticationStateMachine {
    
    enum State {
        case initialized
        case startupMessageSent
        case passwordAuthenticationSent
        
        case saslInitialResponseSent(SASLAuthenticationManager<SASLMechanism.SCRAM.SHA256>)
        case saslChallengeResponseSent(SASLAuthenticationManager<SASLMechanism.SCRAM.SHA256>)
        case saslFinalReceived
        
        case error(PSQLError)
        case authenticated
    }
    
    enum Action {
        case sendStartupMessage(AuthContext)
        case sendPassword(PasswordAuthencationMode, AuthContext)
        case sendSaslInitialResponse(name: String, initialResponse: [UInt8])
        case sendSaslResponse([UInt8])
        case wait
        case authenticated
        
        case reportAuthenticationError(PSQLError)
    }
    
    let authContext: AuthContext
    var state: State
    
    init(authContext: AuthContext) {
        self.authContext = authContext
        self.state = .initialized
    }
    
    mutating func start() -> Action {
        guard case .initialized = self.state else {
            preconditionFailure("Unexpected state")
        }
        self.state = .startupMessageSent
        return .sendStartupMessage(self.authContext)
    }
    
    mutating func authenticationMessageReceived(_ message: PSQLBackendMessage.Authentication) -> Action {
        switch self.state {
        case .startupMessageSent:
            switch message {
            case .ok:
                self.state = .authenticated
                return .authenticated
            case .md5(let salt):
                guard self.authContext.password != nil else {
                    return self.setAndFireError(.authMechanismRequiresPassword)
                }
                self.state = .passwordAuthenticationSent
                return .sendPassword(.md5(salt: salt), self.authContext)
            case .plaintext:
                self.state = .passwordAuthenticationSent
                return .sendPassword(.cleartext, authContext)
            case .kerberosV5:
                return self.setAndFireError(.unsupportedAuthMechanism(.kerberosV5))
            case .scmCredential:
                return self.setAndFireError(.unsupportedAuthMechanism(.scmCredential))
            case .gss:
                return self.setAndFireError(.unsupportedAuthMechanism(.gss))
            case .sspi:
                return self.setAndFireError(.unsupportedAuthMechanism(.sspi))
            case .sasl(let mechanisms):
                guard mechanisms.contains("SCRAM-SHA-256") else {
                    return self.setAndFireError(.unsupportedAuthMechanism(.sasl(mechanisms: mechanisms)))
                }
                
                guard let password = self.authContext.password else {
                    return self.setAndFireError(.authMechanismRequiresPassword)
                }
                
                let saslManager = SASLAuthenticationManager(asClientSpeaking:
                    SASLMechanism.SCRAM.SHA256(username: self.authContext.username, password: { password }))
                
                do {
                    var bytes: [UInt8]?
                    let done = try saslManager.handle(message: nil, sender: { bytes = $0 })
                    
                    guard let output = bytes, done == false else {
                        preconditionFailure("TODO: SASL auth is always a three step process in Postgres.")
                    }
                    
                    self.state = .saslInitialResponseSent(saslManager)
                    return .sendSaslInitialResponse(name: "SCRAM-SHA-256", initialResponse: output)
                } catch {
                    return self.setAndFireError(.sasl(underlying: error))
                }
            case .gssContinue,
                 .saslContinue,
                 .saslFinal:
                return self.setAndFireError(.unexpectedBackendMessage(.authentication(message)))
            }
        case .passwordAuthenticationSent, .saslFinalReceived:
            guard case .ok = message else {
                return self.setAndFireError(.unexpectedBackendMessage(.authentication(message)))
            }
            
            self.state = .authenticated
            return .authenticated
        
        case .saslInitialResponseSent(let saslManager):
            guard case .saslContinue(data: var data) = message else {
                return self.setAndFireError(.unexpectedBackendMessage(.authentication(message)))
            }
            
            let input = data.readBytes(length: data.readableBytes)
            
            do {
                var bytes: [UInt8]?
                let done = try saslManager.handle(message: input, sender: { bytes = $0 })
                
                guard let output = bytes, done == false else {
                    preconditionFailure("TODO: SASL auth is always a three step process in Postgres.")
                }
                
                self.state = .saslChallengeResponseSent(saslManager)
                return .sendSaslResponse(output)
            } catch {
                return self.setAndFireError(.sasl(underlying: error))
            }
            
        case .saslChallengeResponseSent(let saslManager):
            guard case .saslFinal(data: var data) = message else {
                return self.setAndFireError(.unexpectedBackendMessage(.authentication(message)))
            }
            
            let input = data.readBytes(length: data.readableBytes)
            
            do {
                var bytes: [UInt8]?
                let done = try saslManager.handle(message: input, sender: { bytes = $0 })
                
                guard bytes == nil, done == true else {
                    preconditionFailure("TODO: SASL auth is always a three step process in Postgres.")
                }
                
                self.state = .saslFinalReceived
                return .wait
            } catch {
                return self.setAndFireError(.sasl(underlying: error))
            }
        
        case .initialized:
            preconditionFailure("Invalid state")
            
        case .authenticated, .error:
            preconditionFailure("This state machine must not receive messages, after authenticate or error")
        }
    }
    
    mutating func errorReceived(_ message: PSQLBackendMessage.ErrorResponse) -> Action {
        return self.setAndFireError(.server(message))
    }

    private mutating func setAndFireError(_ error: PSQLError) -> Action {
        self.state = .error(error)
        return .reportAuthenticationError(error)
    }
}

extension AuthenticationStateMachine.State: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .initialized:
            return ".initialized"
        case .startupMessageSent:
            return ".startupMessageSent"
        case .passwordAuthenticationSent:
            return ".passwordAuthenticationSent"
        
        case .saslInitialResponseSent(let saslManager):
            return ".saslInitialResponseSent(\(String(reflecting: saslManager)))"
        case .saslChallengeResponseSent(let saslManager):
            return ".saslChallengeResponseSent(\(String(reflecting: saslManager)))"
        case .saslFinalReceived:
            return ".saslFinalReceived"
        
        case .error(let error):
            return ".error(\(String(reflecting: error)))"
        case .authenticated:
            return ".authenticated"
        }
    }
}
