import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct AuthenticationStateMachineTests {

    @Test func testAuthenticatePlaintext() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")

        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)

        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
        #expect(state.authenticationMessageReceived(.plaintext) == .sendPasswordMessage(.cleartext, authContext))
        #expect(state.authenticationMessageReceived(.ok) == .wait)
    }
    
    @Test func testAuthenticateMD5() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
        let salt: UInt32 = 0x00_01_02_03

        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
        #expect(state.authenticationMessageReceived(.md5(salt: salt)) == .sendPasswordMessage(.md5(salt: salt), authContext))
        #expect(state.authenticationMessageReceived(.ok) == .wait)
    }
    
    @Test func testAuthenticateMD5WithoutPassword() {
        let authContext = AuthContext(username: "test", password: nil, database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
        let salt: UInt32 = 0x00_01_02_03

        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
        #expect(state.authenticationMessageReceived(.md5(salt: salt)) ==
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .authMechanismRequiresPassword, closePromise: nil)))
    }
    
    @Test func testAuthenticateOkAfterStartUpWithoutAuthChallenge() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
        #expect(state.authenticationMessageReceived(.ok) == .wait)
    }
    
    @Test func testAuthenticateSCRAMSHA256WithAtypicalEncoding() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))

        let saslResponse = state.authenticationMessageReceived(.sasl(names: ["SCRAM-SHA-256"]))
        guard case .sendSaslInitialResponse(name: let name, initialResponse: let responseData) = saslResponse else {
            Issue.record("\(saslResponse) is not .sendSaslInitialResponse")
            return
        }
        let responseString = String(decoding: responseData, as: UTF8.self)
        #expect(name == "SCRAM-SHA-256")
        #expect(responseString.starts(with: "n,,n=test,r="))

        let saslContinueResponse = state.authenticationMessageReceived(.saslContinue(data: .init(bytes:
            "r=\(responseString.dropFirst(12))RUJSZHhkeUVFNzRLNERKMkxmU05ITU1NZWcxaQ==,s=ijgUVaWgCDLRJyF963BKNA==,i=4096".utf8
        )))
        guard case .sendSaslResponse(let responseData2) = saslContinueResponse else {
            Issue.record("\(saslContinueResponse) is not .sendSaslResponse")
            return
        }
        let response2String = String(decoding: responseData2, as: UTF8.self)
        #expect(response2String.prefix(76) == "c=biws,r=\(responseString.dropFirst(12))RUJSZHhkeUVFNzRLNERKMkxmU05ITU1NZWcxaQ==,p=")
    }
    
    @Test func testAuthenticationFailure() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
        let salt: UInt32 = 0x00_01_02_03

        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
        #expect(state.authenticationMessageReceived(.md5(salt: salt)) == .sendPasswordMessage(.md5(salt: salt), authContext))
        let fields: [PostgresBackendMessage.Field: String] = [
            .message: "password authentication failed for user \"postgres\"",
            .severity: "FATAL",
            .sqlState: "28P01",
            .localizedSeverity: "FATAL",
            .routine: "auth_failed",
            .line: "334",
            .file: "auth.c"
        ]
        #expect(state.errorReceived(.init(fields: fields)) ==
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .server(.init(fields: fields)), closePromise: nil)))
    }

    // MARK: Test unsupported messages
    
    @Test func testUnsupportedAuthMechanism() {
        let unsupported: [(PostgresBackendMessage.Authentication, PSQLError.UnsupportedAuthScheme)] = [
            (.kerberosV5, .kerberosV5),
            (.scmCredential, .scmCredential),
            (.gss, .gss),
            (.sspi, .sspi),
            (.sasl(names: ["haha"]), .sasl(mechanisms: ["haha"])),
        ]
        
        for (message, mechanism) in unsupported {
            let authContext = AuthContext(username: "test", password: "abc123", database: "test")
            var state = ConnectionStateMachine(requireBackendKeyData: true)
            #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
            #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
            #expect(state.authenticationMessageReceived(message) ==
                           .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .unsupportedAuthMechanism(mechanism), closePromise: nil)))
        }
    }
    
    @Test func testUnexpectedMessagesAfterStartUp() {
        var buffer = ByteBuffer()
        buffer.writeBytes([0, 1, 2, 3, 4, 5, 6, 7, 8])
        let unexpected: [PostgresBackendMessage.Authentication] = [
            .gssContinue(data: buffer),
            .saslContinue(data: buffer),
            .saslFinal(data: buffer)
        ]
        
        for message in unexpected {
            let authContext = AuthContext(username: "test", password: "abc123", database: "test")
            var state = ConnectionStateMachine(requireBackendKeyData: true)
            #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
            #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
            #expect(state.authenticationMessageReceived(message) ==
                           .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .unexpectedBackendMessage(.authentication(message)), closePromise: nil)))
        }
    }
    
    @Test func testUnexpectedMessagesAfterPasswordSent() {
        let salt: UInt32 = 0x00_01_02_03
        var buffer = ByteBuffer()
        buffer.writeBytes([0, 1, 2, 3, 4, 5, 6, 7, 8])
        let unexpected: [PostgresBackendMessage.Authentication] = [
            .kerberosV5,
            .md5(salt: salt),
            .plaintext,
            .scmCredential,
            .gss,
            .sspi,
            .gssContinue(data: buffer),
            .sasl(names: ["haha"]),
            .saslContinue(data: buffer),
            .saslFinal(data: buffer),
        ]
        
        for message in unexpected {
            let authContext = AuthContext(username: "test", password: "abc123", database: "test")
            var state = ConnectionStateMachine(requireBackendKeyData: true)
            #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
            #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
            #expect(state.authenticationMessageReceived(.md5(salt: salt)) == .sendPasswordMessage(.md5(salt: salt), authContext))
            #expect(state.authenticationMessageReceived(message) ==
                           .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .unexpectedBackendMessage(.authentication(message)), closePromise: nil)))
        }
    }
}
