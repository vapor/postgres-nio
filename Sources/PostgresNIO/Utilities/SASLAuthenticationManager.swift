import Crypto

public final class SASLAuthenticationManager<M: SASLAuthenticationMechanism> {

    private enum Role {
        case client, server
    }
    
    private enum State {
        /// Client: Waiting to send initial request. May transition to `waitingNextStep`.
        /// Server: Waiting for initial request. May transition to `waitingNextStep`, `done`.
        case waitingForInitial
        
        /// Client: Initial request sent, waiting for next challenge. May transition to `done`.
        /// Server: Latest challenge sent, waiting for next response. May transition to `done`.
        case waitingNextStep
        
        /// Client: Received success or failure. No more operations permitted.
        /// Server: Sent success or failure. No more operations permitted.
        case done
    }
    
    private let mechanism: M
    private let role: Role
    private var state: State = .waitingForInitial
    
    public init(asClientSpeaking mechanism: M) {
        self.mechanism = mechanism
        self.role = .client
    }

    public init(asServerAccepting mechanism: M) {
        self.mechanism = mechanism
        self.role = .server
    }
    
    /// Handle an incoming message via the provided mechanism. The `sender`
    /// closure will be invoked with any data that should be transmitted to the
    /// other side of the negotiation. An error thrown from the closure will
    /// immediately result in an authentication failure state. The closure may
    /// be invoked even if authentication otherwise fails (such as for
    /// mechanisms which send failure responses). On authentication failure, an
    /// error is thrown. Otherwise, `true` is returned to indicate that
    /// authentication has successfully completed. `false` is returned to
    /// indicate that further steps are required by the current mechanism.
    ///
    /// Pass a `nil` message to start the initial request from a client. It is
    /// invalid to do this for a server.
    public func handle(message: [UInt8]?, sender: ([UInt8]) throws -> Void) throws -> Bool {
        guard self.state != .done else {
            // Already did whatever we were gonna do.
            throw SASLAuthenticationError.resultAlreadyDelivered
        }
        
        if message == nil {
            guard self.role == .client else {
                // Can't respond to `nil` as server
                self.state = .done
                throw SASLAuthenticationError.serverRoleRequiresMessage
            }
            guard self.state == .waitingForInitial else {
                // Can't respond to `nil` as client twice.
                self.state = .done
                throw SASLAuthenticationError.initialRequestAlreadySent
            }
        } else if self.role == .client && state == .waitingForInitial {
            // Must respond to `nil` as client first and exactly once.
            self.state = .done
            throw SASLAuthenticationError.initialRequestNotSent
        }
        
        switch self.mechanism.step(message: message) {
            case .continue(let response):
                if let response = response {
                    try sender(response)
                }
                self.state = .waitingNextStep
                return false
            case .succeed(let response):
                if let response = response {
                    try sender(response)
                }
                self.state = .done
                return true
            case .fail(let response, let error):
                if let response = response {
                    try sender(response)
                }
                self.state = .done
                if let error = error {
                    throw error
                } else {
                    throw SASLAuthenticationError.genericAuthenticationFailure
                }
        }
    }

}

/// Various errors that can occur during SASL negotiation that are not specific
/// to the particular SASL mechanism in use.
public enum SASLAuthenticationError: Error {
    /// A server can not handle a nonexistent message. Only an initial-state
    /// client can do that, and even then it's really just a proxy for the API
    /// having difficulty expressing "this must be done once and then never
    /// again" clearly.
    case serverRoleRequiresMessage
    
    /// A client may only receive a nonexistent message once during the initial
    /// state. This is a proxy for the API not being good at expressing a "must
    /// do this first and only once."
    case initialRequestAlreadySent
    
    /// A client must receive a nonexistent message exactly once before doing
    /// anything else. This is ALSO a proxy for the API just being bad at
    /// expressing the requirement.
    case initialRequestNotSent
    
    /// Authentication failed, and the underlying mechanism declined to provide
    /// a more specific error message.
    case genericAuthenticationFailure
    
    /// This `SASLAuthenticationManager` has already delivered a success or
    /// failure result (which may include a fatal state management error). It
    /// can not be reused.
    case resultAlreadyDelivered
}

/// Signifies an action to be taken as the result of a single step of a SASL
/// mechanism.
public enum SASLAuthenticationStepResult {

    /// More steps are needed. Assume neither success nor failure. If data is
    /// provided, send it. A value of `nil` signifies sending no response at
    /// all, whereas a value of `[]` signifies sending an empty response, which
    /// may not be the same action depending on the underlying protocol
    case `continue`(response: [UInt8]? = nil)
    
    /// Signal authentication success. If data is provided, send it. A value of
    /// `nil` signifies sending no response at all, whereas a value of `[]`
    /// signifies sending an empty response, which may not be the same action
    /// depending on the underlying protocol.
    case succeed(response: [UInt8]? = nil)

    /// Signal authentication failure. If data is provided, send it. A value of
    /// `nil` signifies sending no response at all, whereas a value of `[]`
    /// signifies sending an empty response, which may not be the same action
    /// depending on the underlying protocol. The provided error, if any, is
    /// surfaced. If none is provided, a generic failure is surfaced instead.
    case fail(response: [UInt8]? = nil, error: Error? = nil)
    
}

/// The protocol to which all SASL mechanism implementations must conform. It is
/// the responsibility of each individual implementation to provide an API for
/// creating instances of itself which are able to retrieve information from the
/// caller (such as usernames and passwords) by some mechanism.
public protocol SASLAuthenticationMechanism {
    
    /// The IANA-registered SASL mechanism name. This may be a family prefix or
    /// a specific mechanism name. It is explicitly suitable for use in
    /// negotiation via whatever underlying application-specific protocol is in
    /// use for the purpose.
    static var name: String { get }
    
    /// Single-step the mechanism. The message may be `nil` in particular when
    /// the local side of the negotiation is a client starting its initial
    /// authentication request.
    func step(message: [UInt8]?) -> SASLAuthenticationStepResult

}
