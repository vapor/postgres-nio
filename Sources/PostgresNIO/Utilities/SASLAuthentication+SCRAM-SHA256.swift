import Crypto
import Foundation

extension UInt8 {
    fileprivate static var NUL: UInt8 { return 0x00 /* yeah, just U+0000 man */ }
    fileprivate static var comma: UInt8 { return 0x2c /* .init(ascii: ",") */ }
    fileprivate static var equals: UInt8 { return 0x3d /* .init(ascii: "=") */ }
}

fileprivate extension String {
    /**
     ````
     The characters ',' or '=' in usernames are sent as '=2C' and
     '=3D' respectively.  If the server receives a username that
     contains '=' not followed by either '2C' or '3D', then the
     server MUST fail the authentication.
     ````
    */
    var decodedAsSaslName: String? {
        guard !self.contains(",") else { return nil }
        
        let partial = self.replacingOccurrences(of: "=2C", with: ",")
                          .replacingOccurrences(of: "=3D", with: "@@@TEMPORARY_REPLACEMENT_MARKER_EQUALS@@@")
        
        guard !partial.contains("=") else { return nil }
        return partial.replacingOccurrences(of: "@@@TEMPORARY_REPLACEMENT_MARKER_EQUALS@@@", with: "=")
    }
    
    var encodedAsSaslName: String {
        return self.replacingOccurrences(of: ",", with: "=2C").replacingOccurrences(of: "=", with: "=3D")
    }
    
    init?(printableAscii: [UInt8]) {
        // `isdigit()` is (bad) libc, `CharacterSet` is Foundation. Rather than pull in either one
        // directly, just hardcode it. Not a great ideal in general, mind you. UTF-8 is designed so
        // all non-ASCII scalars will have high bits in their encoding somewhere, so this check will
        // catch those too. Plus we ask `String` to explicitly accept ASCII only for good measure.
        // `printable       = %x21-2B / %x2D-7E`
        guard !printableAscii.contains(where: { $0 < 0x21 || $0 == 0x2c || $0 > 0x7e }) else { return nil }
        self.init(bytes: printableAscii, encoding: .ascii)
    }
    
    init?(asciiAlphanumericMorse: [UInt8]) {
        guard asciiAlphanumericMorse.isAsciiAlphanumericMorse else { return nil }
        self.init(bytes: asciiAlphanumericMorse, encoding: .ascii)
    }
}

fileprivate extension Array where Element == UInt8 {

    // TODO: Use the Base64 coder from NIOWebSocket or Crypto rather than yanking in Foundation
    func decodingBase64() -> [UInt8]? {
        var actual = self
        if actual.count % 4 != 0 {
            actual.append(contentsOf: Array<UInt8>.init(repeating: .equals, count: 4 - (actual.count % 4)))
        }
        return actual.withUnsafeBytes({ (buf: UnsafeRawBufferPointer) -> Data? in
            Data(base64Encoded: Data(bytesNoCopy: .init(mutating: buf.baseAddress!), count: buf.count, deallocator: .none))
        }).map { .init($0) }
    }
    
    func encodingBase64() -> [UInt8] {
        return Array(self.withUnsafeBytes {
            Data(bytesNoCopy: .init(mutating: $0.baseAddress!), count: $0.count, deallocator: .none).base64EncodedData()
        })
    }

    /// `1*(ALPHA / DIGIT / "." / "-")`
    var isAsciiAlphanumericMorse: Bool {
        // This is dumb. Match if we don't contain not containing any of the valid ranges. Yep.
        return !self.contains(where: { c in ![0x30...0x39, 0x41...0x51, 0x61...0x7a].reduce(false, { $0 || $1.contains(c) }) })
    }
    
    var isAllDigits: Bool {
        return !self.contains(where: { !(0x30...0x39).contains($0) })
    }
    
    /**
    ````
    value           = 1*value-char
    value-safe-char = %x01-2B / %x2D-3C / %x3E-7F / UTF8-2 / UTF8-3 / UTF8-4
    value-char      = value-safe-char / "="
    ````
    */
    var isValidScramValue: Bool {
        // TODO: FInd a better way than doing a whole construction of String...
        return self.count > 0 && !(String(decoding: self, as: Unicode.UTF8.self).contains(","))
    }

}

fileprivate enum SCRAMServerError: Error, RawRepresentable {
    // Really could just use a string for this...
    case invalidEncoding, extensionsNotSupported, invalidProof, channelBindingsDontMatch,
         serverDoesSupportChannelBinding, channelBindingNotSupported, unsupportedChannelBindingType,
         unknownUser, invalidUsernameEncoding, noResources, otherError, serverErrorValueExt(String)
    
    init?(rawValue: String) {
        switch rawValue {
            case "invalid-encoding": self = .invalidEncoding
            case "extensions-not-supported": self = .extensionsNotSupported
            case "invalid-proof": self = .invalidProof
            case "channel-bindings-dont-match": self = .channelBindingsDontMatch
            case "server-does-support-channel-binding": self = .serverDoesSupportChannelBinding
            case "channel-binding-not-supported": self = .channelBindingNotSupported
            case "unsupported-channel-binding-type": self = .unsupportedChannelBindingType
            case "unknown-user": self = .unknownUser
            case "invalid-username-encoding": self = .invalidUsernameEncoding
            case "no-resources": self = .noResources
            case "other-error": self = .otherError
            default: self = .serverErrorValueExt(rawValue)
        }
    }
    
    var rawValue: String {
        switch self {
            case .invalidEncoding: return "invalid-encoding"
            case .extensionsNotSupported: return "extensions-not-supported"
            case .invalidProof: return "invalid-proof"
            case .channelBindingsDontMatch: return "channel-bindings-dont-match"
            case .serverDoesSupportChannelBinding: return "server-does-support-channel-binding"
            case .channelBindingNotSupported: return "channel-binding-not-supported"
            case .unsupportedChannelBindingType: return "unsupported-channel-binding-type"
            case .unknownUser: return "unknown-user"
            case .invalidUsernameEncoding: return "invalid-username-encoding"
            case .noResources: return "no-resources"
            case .otherError: return "other-error"
            case .serverErrorValueExt(let raw): return raw
        }
    }
}

fileprivate enum SCRAMAttribute {
    enum GS2ChannelBinding {
        case unsupported // client lacks support: `"n"`
        case unused // client thinks server lacks support: `"y"`
        case bind(String, [UInt8]?) // explicit channel binding: `"p=" 1*(ALPHA / DIGIT / "." / "-")`, `cbind-data`, per RFC 5056§7
    }
    /// authorization identity: `"a=" saslname`
    case a(String?)
    /// authentication identity: `"n=" saslname`
    case n(String)
    /// reserved for mandatory extension signaling: `"m=" 1*(value-char)`
    case m([UInt8])
    /// nonce: `"r=" printable`
    case r(String)
    /// GS2 header and channel binding: `"c=" base64(cbind-input)`
    case c(binding: GS2ChannelBinding = .unsupported, authIdentity: String? = nil)
    /// salt: `"s=" base64`
    case s([UInt8])
    /// iteration count: `"i=" posit-number`
    case i(UInt32)
    /// computed proof: `"p=" base64` (notably slightly conflicts with GS2 header's channel binding)
    case p([UInt8])
    /// verifier (computed server signature): `"v=" base64`
    case v([UInt8])
    /// server error: `"e=" server-error-value`
    case e(SCRAMServerError)
    /// unknown optional extension: `attr-val` ... `ALPHA "=" 1*value-char`
    case optional(name: CChar, value: [UInt8])
    /// partial GS2 header signal (binding type, no data)
    case gp(GS2ChannelBinding)
    /// partial GS2 header signal (binding data)
    case gm([UInt8])
}

fileprivate struct SCRAMMessageParser {
    static func parseAttributePair(name: [UInt8], value: [UInt8], isGS2Header: Bool = false) -> SCRAMAttribute? {
        guard name.count == 1 || isGS2Header else { return nil }
        switch name.first {
        case UInt8(ascii: "m") where !isGS2Header: return .m(value)
        case UInt8(ascii: "r") where !isGS2Header: return String(printableAscii: value).map { .r($0) }
        case UInt8(ascii: "c") where !isGS2Header:
            guard let parsedAttrs = value.decodingBase64().flatMap({ parse(raw: $0, isGS2Header: true) }) else { return nil }
            guard (1...3).contains(parsedAttrs.count) else { return nil }
            switch (parsedAttrs.first, parsedAttrs.dropFirst(1).first, parsedAttrs.dropFirst(2).first) {
                case let (.gp(.bind(name, .none)), .a(ident), .gm(data)): return .c(binding: .bind(name, data), authIdentity: ident)
                case let (.gp(.bind(name, .none)), .gm(data),     .none): return .c(binding: .bind(name, data))
                case let (.gp(bind),               .a(ident),     .none): return .c(binding: bind, authIdentity: ident)
                case let (.gp(bind),               .none,         .none): return .c(binding: bind)
                default: return nil
            }
        case UInt8(ascii: "n") where !isGS2Header: return String(decoding: value, as: Unicode.UTF8.self).decodedAsSaslName.map { .n($0) }
        case UInt8(ascii: "s") where !isGS2Header: return value.decodingBase64().map { .s($0) }
        case UInt8(ascii: "i") where !isGS2Header: return String(printableAscii: value).flatMap { UInt32.init($0) }.map { .i($0) }
        case UInt8(ascii: "p") where !isGS2Header: return value.decodingBase64().map { .p($0) }
        case UInt8(ascii: "v") where !isGS2Header: return value.decodingBase64().map { .v($0) }
        case UInt8(ascii: "e") where !isGS2Header: // TODO: actually map the specific enum string values
            guard value.isValidScramValue else { return nil }
            return SCRAMServerError(rawValue: String(decoding: value, as: Unicode.UTF8.self)).flatMap { .e($0) }

        case UInt8(ascii: "y") where isGS2Header && value.count == 0: return .gp(.unused)
        case UInt8(ascii: "n") where isGS2Header && value.count == 0: return .gp(.unsupported)
        case UInt8(ascii: "p") where isGS2Header: return String(asciiAlphanumericMorse: value).map { .gp(.bind($0, nil)) }
        case UInt8(ascii: "a") where isGS2Header: return String(decoding: value, as: Unicode.UTF8.self).decodedAsSaslName.map { .a($0) }
        case .none where isGS2Header: return .a(nil)

        default:
            if isGS2Header {
                return .gm(name + value)
            } else {
                guard value.count > 0, value.isValidScramValue else { return nil }
                return .optional(name: CChar(name[0]), value: value)
            }
        }
    }
    
    static func parse(raw: [UInt8], isGS2Header: Bool = false) -> [SCRAMAttribute]? {
        // There are two ways to implement this parse:
        //  1. All-at-once: Split on comma, split each on equals, validate
        //     each results in a valid attribute.
        //  2. Sequential: State machine lookahead parse.
        // The former is simpler. The latter provides better validation.
        let likelyAttributeSets = raw.split(separator: .comma, maxSplits: isGS2Header ? 2 : Int.max, omittingEmptySubsequences: false)
        let likelyAttributePairs = likelyAttributeSets.map { $0.split(separator: .equals, maxSplits: 1, omittingEmptySubsequences: false) }
        
        let results = likelyAttributePairs.map { parseAttributePair(name: Array($0[0]), value: $0.dropFirst().first.map { Array($0) } ?? [], isGS2Header: isGS2Header) }
        let validResults = results.compactMap { $0 }
        guard validResults.count == results.count else { return nil }
        
        return validResults
    }
    
    static func serialize(_ attributes: [SCRAMAttribute], isInitialGS2Header: Bool = true) -> [UInt8]? {
        var result: [UInt8] = []
        
        for attribute in attributes {
            switch attribute {
                case .m(let value):
                    result.append(UInt8(ascii: "m")); result.append(.equals); result.append(contentsOf: value)
                case .r(let nonce):
                    result.append(UInt8(ascii: "r")); result.append(.equals); result.append(contentsOf: nonce.utf8.map { UInt8($0) })
                case .n(let name):
                    result.append(UInt8(ascii: "n")); result.append(.equals); result.append(contentsOf: name.encodedAsSaslName.utf8.map { UInt8($0) })
                case .s(let salt):
                    result.append(UInt8(ascii: "s")); result.append(.equals); result.append(contentsOf: salt.encodingBase64())
                case .i(let count):
                    result.append(UInt8(ascii: "i")); result.append(.equals); result.append(contentsOf: "\(count)".utf8.map { UInt8($0) })
                case .p(let proof):
                    result.append(UInt8(ascii: "p")); result.append(.equals); result.append(contentsOf: proof.encodingBase64())
                case .v(let signature):
                    result.append(UInt8(ascii: "v")); result.append(.equals); result.append(contentsOf: signature.encodingBase64())
                case .e(let error):
                    result.append(UInt8(ascii: "e")); result.append(.equals); result.append(contentsOf: error.rawValue.utf8.map { UInt8($0) })
                case .c(let binding, let identity):
                    if isInitialGS2Header {
                        switch binding {
                        case .unsupported: result.append(UInt8(ascii: "n"))
                        case .unused: result.append(UInt8(ascii: "y"))
                        case .bind(let name, _): result.append(UInt8(ascii: "p")); result.append(.equals); result.append(contentsOf: name.utf8.map { UInt8($0) })
                        }
                        result.append(.comma)
                        if let identity = identity {
                            result.append(UInt8(ascii: "a")); result.append(.equals); result.append(contentsOf: identity.encodedAsSaslName.utf8.map { UInt8($0) })
                        }
                        result.append(.comma)
                    } else {
                        guard var partial = serialize([attribute], isInitialGS2Header: true) else { return nil }
                        if case let .bind(_, data) = binding {
                            guard let data = data else { return nil }
                            partial.append(contentsOf: data)
                        }
                        result.append(UInt8(ascii: "c")); result.append(.equals); result.append(contentsOf: partial.encodingBase64())
                    }
                default:
                    return nil
            }
            result.append(.comma)
        }
        return result.dropLast()
    }
}

internal enum SASLMechanism {
internal enum SCRAM {

/// Implementation of `SCRAM-SHA-256` as a `SASLAuthenticationMechanism`
///
/// Implements SCRAM-SHA-256 as described by:
/// - [RFC 7677 (SCRAM-SHA-256 and SCRAM-SHA-256-PLUS SASL Mechanisms)](https://tools.ietf.org/html/rfc7677)
/// - [RFC 5802 (SCRAM SASL and GSS-API Mechanisms)](https://tools.ietf.org/html/rfc5802)
/// - [RFC 4422 (Simple Authentication and Security Layer)](https://tools.ietf.org/html/rfc4422)
internal struct SHA256: SASLAuthenticationMechanism {

    static internal var name: String { return "SCRAM-SHA-256" }
    
    /// Set up a client-side `SCRAM-SHA-256` authentication.
    ///
    /// - Parameters:
    ///   - username: The username to authenticate as.
    ///   - password: A closure which returns the plaintext password for the
    ///               authenticating user. If the closure throws, authentication
    ///               immediately fails with the thrown error.
    internal init(username: String, password: @escaping () throws -> String) {
        self._impl = .init(username: username, passwordGrabber: { _ in try (Array(password().utf8), []) }, bindingInfo: .unsupported)
    }
    
    /// Set up a server-side `SCRAM-SHA-256` authentication.
    ///
    /// - Parameters:
    ///   - iterations: The number of iterations applied to salted passwords.
    ///                 Must be at least 4096.
    ///   - saltedPassword: A closure which receives the username of the user
    ///                     attempting to authentication and must return the
    ///                     salted password for that user, as well as the salt
    ///                     itself. If the closure throw, authentication
    ///                     immediately fails with the thrown error.
    internal init(serveWithIterations iterations: UInt32 = 4096, saltedPassword: @escaping (String) throws -> ([UInt8], [UInt8])) {
        self._impl = .init(iterationCount: iterations, passwordGrabber: saltedPassword, requireBinding: false)
    }
    
    internal func step(message: [UInt8]?) -> SASLAuthenticationStepResult {
        return _impl.step(message: message)
    }
    
    private let _impl: SASLMechanism_SCRAM_SHA256_Common
}

/// Implementation of `SCRAM-SHA-256-PLUS` as a `SASLAuthenticationMechanism`
///
/// Implements SCRAM-SHA-256-PLUS as described by:
/// - [RFC 7677 (SCRAM-SHA-256 and SCRAM-SHA-256-PLUS SASL Mechanisms)](https://tools.ietf.org/html/rfc7677)
/// - [RFC 5802 (SCRAM SASL and GSS-API Mechanisms)](https://tools.ietf.org/html/rfc5802)
/// - [RFC 4422 (Simple Authentication and Security Layer)](https://tools.ietf.org/html/rfc4422)
internal struct SHA256_PLUS: SASLAuthenticationMechanism {

    static internal var name: String { return "SCRAM-SHA-256-PLUS" }
    
    /// Set up a client-side `SCRAM-SHA-256-PLUS` authentication.
    ///
    /// - Parameters:
    ///   - username: The username to authenticate as.
    ///   - password: A closure which returns the plaintext password for the
    ///               authenticating user. If the closure throws, authentication
    ///               immediately fails with the thrown error.
    ///   - channelBindingName: The RFC5056 channel binding to apply to the
    ///                         authentication.
    ///   - channelBindingData: The appropriate data associated with the RFC5056
    ///                         channel binding specified.
    internal init(username: String, password: @escaping () throws -> String, channelBindingName: String, channelBindingData: [UInt8]) {
        self._impl = .init(username: username, passwordGrabber: { _ in try (Array(password().utf8), []) }, bindingInfo: .bind(channelBindingName, channelBindingData))
    }
    
    /// Set up a server-side `SCRAM-SHA-256` authentication.
    ///
    /// - Parameters:
    ///   - iterations: The number of iterations applied to salted passwords.
    ///                 Must be at least 4096.
    ///   - saltedPassword: A closure which receives the username of the user
    ///                     attempting to authentication and must return the
    ///                     salted password for that user, as well as the salt
    ///                     itself. If the closure throw, authentication
    ///                     immediately fails with the thrown error.
    internal init(serveWithIterations iterations: UInt32 = 4096, saltedPassword: @escaping (String) throws -> ([UInt8], [UInt8])) {
        self._impl = .init(iterationCount: iterations, passwordGrabber: saltedPassword, requireBinding: true)
    }
    
    internal func step(message: [UInt8]?) -> SASLAuthenticationStepResult {
        return _impl.step(message: message)
    }
    
    private let _impl: SASLMechanism_SCRAM_SHA256_Common
}

} // enum SCRAM
} // enum SASLMechanism

/// Common implementation of SCRAM-SHA-256 and SCRAM-SHA-256-PLUS
fileprivate final class SASLMechanism_SCRAM_SHA256_Common {

    /// Initialized with initial client state
    init(username: String, passwordGrabber: @escaping (String) throws -> ([UInt8], [UInt8]), bindingInfo: SCRAMAttribute.GS2ChannelBinding) {
        let nonce = Data((0..<18).map { _ in UInt8.random(in: .min...(.max)) }).base64EncodedString()
        self.state = .clientInitial(username: username, nonce: nonce, binding: bindingInfo)
        self.passwordGrabber = passwordGrabber
    }
    
    /// Initialized with initial server state
    init(iterationCount: UInt32, passwordGrabber: @escaping (String) throws -> ([UInt8], [UInt8]), requireBinding: Bool) {
        let nonce = Data((0..<18).map { _ in UInt8.random(in: .min...(.max)) }).base64EncodedString()
        self.state = .serverInitial(extraNonce: nonce, iterationCount: iterationCount, bindingRequired: requireBinding)
        self.passwordGrabber = passwordGrabber
    }
    
    private var state: State
    private let passwordGrabber: (String) throws -> ([UInt8], [UInt8])
    
    private enum State {
        case clientInitial(username: String, nonce: String, binding: SCRAMAttribute.GS2ChannelBinding)
        case clientSentFirstMessage(username: String, nonce: String, binding: SCRAMAttribute.GS2ChannelBinding, bareMessage: [UInt8])
        case clientSentFinalMessage(saltedPassword: [UInt8], authMessage: [UInt8])
        case clientDone
        
        case serverInitial(extraNonce: String, iterationCount: UInt32, bindingRequired: Bool)
        case serverSentFirstMessage(clientBareFirstMessage: [UInt8], nonce: String, binding: SCRAMAttribute.GS2ChannelBinding, saltedPassword: [UInt8], serverFirstMessage: [UInt8])
        case serverDone
    }
    
    public func step(message: [UInt8]?) -> SASLAuthenticationStepResult {
        do {
            switch state {
                case .clientInitial(let username, let nonce, let binding):
                    guard message == nil else { throw SASLAuthenticationError.initialRequestNotSent }
                    return try self.handleClientInitial(username: username, nonce: nonce, binding: binding)
                case .clientSentFirstMessage(let username, let nonce, let binding, let firstMessageBare):
                    guard let serverFirstMessage = message else { throw SASLAuthenticationError.initialRequestAlreadySent }
                    return try self.handleClientSentFirst(message: serverFirstMessage, username: username, nonce: nonce, binding: binding, firstMessageBare: firstMessageBare)
                case .clientSentFinalMessage(let saltedPassword, let authMessage):
                    guard let serverFinalMessage = message else { throw SASLAuthenticationError.initialRequestAlreadySent }
                    return try self.handleClientSentFinal(message: serverFinalMessage, saltedPassword: saltedPassword, authMessage: authMessage)
                case .clientDone:
                    throw SASLAuthenticationError.resultAlreadyDelivered

                case .serverInitial(let extraNonce, let iterationCount, let bindingRequired):
                    return try self.handleServerInitial(message!, extraNonce: extraNonce, iterationCount: iterationCount, bindingRequired: bindingRequired)
                case .serverSentFirstMessage(let clientBareFirstMessage, let nonce, let previousBinding, let saltedPassword, let serverFirstMessage):
                    return try self.handleServerSentFirst(message!, clientBareFirstMessage: clientBareFirstMessage, nonce: nonce, previousBinding: previousBinding, saltedPassword: saltedPassword, serverFirstMessage: serverFirstMessage)
                case .serverDone:
                    throw SASLAuthenticationError.resultAlreadyDelivered
            }
        } catch {
            return .fail(response: nil, error: error)
        }
    }

    private func handleClientInitial(username: String, nonce: String, binding: SCRAMAttribute.GS2ChannelBinding) throws -> SASLAuthenticationStepResult {
        // Generate a `client-first-message-bare`
        guard let clientFirstMessageBare = SCRAMMessageParser.serialize([.n(username), .r(nonce)]) else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Generate a `gs2-header`
        guard let clientGs2Header = SCRAMMessageParser.serialize([.c(binding: binding, authIdentity: nil)]) else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Paste them together to make a `client-first-message`
        let clientFirstMessage = clientGs2Header + clientFirstMessageBare
        
        // Save state and send
        self.state = .clientSentFirstMessage(username: username, nonce: nonce, binding: binding, bareMessage: clientFirstMessageBare)
        return .continue(response: clientFirstMessage)
    }
    
    private func handleClientSentFirst(message: [UInt8], username: String, nonce: String,
                                       binding: SCRAMAttribute.GS2ChannelBinding, firstMessageBare: [UInt8]) throws -> SASLAuthenticationStepResult {
        // Parse incoming
        guard let incomingAttributes = SCRAMMessageParser.parse(raw: message) else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Validate as `server-first-message` and extract data
        guard incomingAttributes.count >= 3 else { throw SASLAuthenticationError.genericAuthenticationFailure }
        guard case let .r(serverNonce) = incomingAttributes.dropFirst(0).first else { throw SASLAuthenticationError.genericAuthenticationFailure }
        guard case let .s(serverSalt) = incomingAttributes.dropFirst(1).first else { throw SASLAuthenticationError.genericAuthenticationFailure }
        guard case let .i(serverIterations) = incomingAttributes.dropFirst(2).first else { throw SASLAuthenticationError.genericAuthenticationFailure }
        
        // Generate a `client-final-message-no-proof`
        guard let clientFinalNoProof = SCRAMMessageParser.serialize([.c(binding: binding), .r(serverNonce)], isInitialGS2Header: false) else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Retrieve the authentication credential
        let (password, _) = try self.passwordGrabber("")
        
        // TODO: Perform `Normalize(password)`, aka the SASLprep profile (RFC4013) of stringprep (RFC3454)
        
        // Calculate `AuthMessage`, `ClientSignature`, and `ClientProof`
        let saltedPassword = Hi(string: password, salt: serverSalt, iterations: serverIterations)
        let clientKey = HMAC<SHA256>.authenticationCode(for: Data("Client Key".utf8), using: .init(data: saltedPassword))
        let storedKey = SHA256.hash(data: Data(clientKey))
        var authMessage = firstMessageBare; authMessage.append(.comma); authMessage.append(contentsOf: message); authMessage.append(.comma); authMessage.append(contentsOf: clientFinalNoProof)
        let clientSignature = HMAC<SHA256>.authenticationCode(for: authMessage, using: .init(data: storedKey))
        var clientProof = Array(clientKey)
        
        clientProof.withUnsafeMutableBytes { proofBuf in
            clientSignature.withUnsafeBytes { signatureBuf in
                for i in 0..<proofBuf.count {
                    proofBuf[i] ^= signatureBuf[i]
                }
            }
        }
        
        // Generate a `client-final-message`
        var clientFinalMessage = clientFinalNoProof; clientFinalMessage.append(.comma)
        guard let proofPart = SCRAMMessageParser.serialize([.p(Array(clientProof))]) else { throw SASLAuthenticationError.genericAuthenticationFailure }
        clientFinalMessage.append(contentsOf: proofPart)
        
        // Save state and send
        self.state = .clientSentFinalMessage(saltedPassword: saltedPassword, authMessage: authMessage)
        return .continue(response: clientFinalMessage)
    }
    
    private func handleClientSentFinal(message: [UInt8], saltedPassword: [UInt8], authMessage: [UInt8]) throws -> SASLAuthenticationStepResult {
        // Parse incoming
        guard let incomingAttributes = SCRAMMessageParser.parse(raw: message) else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Validate as `server-final-message` and extract data
        switch incomingAttributes.first {
            case .v(let verifier):
                // Verify server signature
                let serverKey = HMAC<SHA256>.authenticationCode(for: Data("Server Key".utf8), using: .init(data: saltedPassword))
                let serverSignature = HMAC<SHA256>.authenticationCode(for: authMessage, using: .init(data: serverKey))
                
                guard Array(serverSignature) == verifier else {
                    return .fail(response: nil, error: SASLAuthenticationError.genericAuthenticationFailure)
                }
             case .e(let error):
                return .fail(response: nil, error: error)
            default: throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Mark done and return success
        self.state = .clientDone
        return .succeed(response: nil)
    }
    
    private func handleServerInitial(_ message: [UInt8], extraNonce: String,
                                     iterationCount: UInt32, bindingRequired: Bool) throws -> SASLAuthenticationStepResult {
        var binding: SCRAMAttribute.GS2ChannelBinding = .unsupported
        
        // Parse as GS2 header first. This is awful and the parser should be refactored.
        guard var channelAttributes = SCRAMMessageParser.parse(raw: message, isGS2Header: true), channelAttributes.count > 0 else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        // Channel binding flag is required. Binding data may not be specified in initial hello.
        switch channelAttributes.removeFirst() {
            case .gp(.unsupported):
                guard !bindingRequired else { throw SCRAMServerError.serverErrorValueExt("client-negotiated-badly") }
                binding = .unsupported
            case .gp(.unused):
                if bindingRequired { throw SCRAMServerError.serverDoesSupportChannelBinding }
                else { throw SCRAMServerError.serverErrorValueExt("channel-bindings-expected-from-client") }
            case .gp(.bind(let type, .none)):
                guard bindingRequired else { throw SCRAMServerError.channelBindingNotSupported }
                binding = .bind(type, nil)
            default: throw SASLAuthenticationError.genericAuthenticationFailure
        }
        // Optional authorization name may appear in GS2 header
        if case .a(_) = channelAttributes.first {
            channelAttributes.removeFirst()
            // TODO: Allow callers to handle authorization names
        }
        // Extract remaining message content. Again, parser needs refactoring.
        guard case let .gm(clientFirstMessageBare) = channelAttributes.first, channelAttributes.count == 1,
              let incomingAttributes = SCRAMMessageParser.parse(raw: clientFirstMessageBare),
              case let .n(username) = incomingAttributes.dropFirst(0).first,
              case let .r(clientNonce) = incomingAttributes.dropFirst(1).first else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Retrieve credentials
        let (saltedPassword, salt) = try self.passwordGrabber(username)
        
        // Generate a `server-first-message`
        guard let serverFirstMessage = SCRAMMessageParser.serialize([.r(clientNonce + extraNonce), .s(salt), .i(iterationCount)]) else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Save state and send
        self.state = .serverSentFirstMessage(clientBareFirstMessage: clientFirstMessageBare, nonce: clientNonce + extraNonce, binding: binding, saltedPassword: saltedPassword, serverFirstMessage: serverFirstMessage)
        return .continue(response: serverFirstMessage)
    }
    
    private func handleServerSentFirst(
        _ message: [UInt8],
        clientBareFirstMessage: [UInt8], nonce: String, previousBinding: SCRAMAttribute.GS2ChannelBinding,
        saltedPassword: [UInt8], serverFirstMessage: [UInt8]
    ) throws -> SASLAuthenticationStepResult {
        guard let incomingAttributes = SCRAMMessageParser.parse(raw: message) else { throw SASLAuthenticationError.genericAuthenticationFailure }
        guard case let .c(binding, _) = incomingAttributes.dropFirst(0).first,
              case let .r(repeatNonce) = incomingAttributes.dropFirst(1).first,
              case let .p(proof) = incomingAttributes.last else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        switch (binding, previousBinding) {
            case (.unsupported, .unsupported): break // all good
            case (.bind(let type, _), .bind(let prevType, .none)) where type == prevType:
                // TODO: Actually handle the binding
                break
            default: throw SCRAMServerError.channelBindingsDontMatch
        }
        guard nonce == repeatNonce else { throw SASLAuthenticationError.genericAuthenticationFailure }
        
        // Compute client signature
        let clientKey = HMAC<SHA256>.authenticationCode(for: Data("Client Key".utf8), using: .init(data: saltedPassword))
        let storedKey = SHA256.hash(data: Data(clientKey))
        var authMessage = clientBareFirstMessage; authMessage.append(.comma); authMessage.append(contentsOf: serverFirstMessage); authMessage.append(.comma); authMessage.append(contentsOf: message.dropLast(proof.count + 3))
        let clientSignature = HMAC<SHA256>.authenticationCode(for: authMessage, using: .init(data: storedKey))
        
        // Recompute client key from signature and proof, verify match
        var clientProofKey = Array(clientSignature)

        clientProofKey.withUnsafeMutableBytes { proofBuf in
            proof.withUnsafeBytes { signatureBuf in
                for i in 0..<proofBuf.count {
                    proofBuf[i] ^= signatureBuf[i]
                }
            }
        }
        let restoredKey = SHA256.hash(data: Data(clientProofKey))
        guard storedKey == restoredKey else { throw SCRAMServerError.invalidProof }
        
        // Compute server signature
        let serverKey = HMAC<SHA256>.authenticationCode(for: Data("Server Key".utf8), using: .init(data: saltedPassword))
        let serverSignature = HMAC<SHA256>.authenticationCode(for: authMessage, using: .init(data: serverKey))
        
        // Generate a `server-final-message`
        guard let serverFinalMessage = SCRAMMessageParser.serialize([.v(Array(serverSignature))]) else {
            throw SASLAuthenticationError.genericAuthenticationFailure
        }
        
        // Save state and signal success with the reply
        self.state = .serverDone
        return .succeed(response: serverFinalMessage)
    }
    
}

/**
  ````
  o  Hi(str, salt, i):

  U1   := HMAC(str, salt + INT(1))
  U2   := HMAC(str, U1)
  ...
  Ui-1 := HMAC(str, Ui-2)
  Ui   := HMAC(str, Ui-1)

  Hi := U1 XOR U2 XOR ... XOR Ui

  where "i" is the iteration count, "+" is the string concatenation
  operator, and INT(g) is a 4-octet encoding of the integer g, most
  significant octet first.

  Hi() is, essentially, PBKDF2 [RFC2898] with HMAC() as the
  pseudorandom function (PRF) and with dkLen == output length of
  HMAC() == output length of H().
  ````
*/
private func Hi(string: [UInt8], salt: [UInt8], iterations: UInt32) -> [UInt8] {
    let key = SymmetricKey(data: string)
    var Ui = HMAC<SHA256>.authenticationCode(for: salt + [0x00, 0x00, 0x00, 0x01], using: key) // salt + 0x00000001 as big-endian
    var Hi = Array(Ui)
    var uiData = [UInt8]()
    uiData.reserveCapacity(32)

    Hi.withUnsafeMutableBytes { Hibuf -> Void in
        for _ in 2...iterations {
            uiData.removeAll(keepingCapacity: true)
            uiData.append(contentsOf: Ui)

            Ui = HMAC<SHA256>.authenticationCode(for: uiData, using: key)

            Ui.withUnsafeBytes { Uibuf -> Void in
                for i in 0..<Uibuf.count { Hibuf[i] ^= Uibuf[i] }
            }
        }
    }
    return Hi
}
