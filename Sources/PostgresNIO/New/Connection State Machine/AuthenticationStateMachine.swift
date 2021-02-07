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
                self.state = .passwordAuthenticationSent
                return .sendPassword(.md5(salt: salt), authContext)
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
            case .sasl:
                return self.setAndFireError(.unsupportedAuthMechanism(.sasl))
            case .gssContinue,
                 .saslContinue,
                 .saslFinal:
                return self.setAndFireError(.unexpectedBackendMessage(.authentication(message)))
            }
        case .passwordAuthenticationSent:
            guard case .ok = message else {
                return self.setAndFireError(.unexpectedBackendMessage(.authentication(message)))
            }
            
            self.state = .authenticated
            return .authenticated
        
        case .saslInitialResponseSent:
            preconditionFailure("Unreachable state as of today!")
            
        case .saslChallengeResponseSent:
            preconditionFailure("Unreachable state as of today!")
        
        case .saslFinalReceived:
            preconditionFailure("Unreachable state as of today!")
            
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
