import XCTest
@testable import PostgresNIO

class AuthenticationStateMachineTests: XCTestCase {
    
    func testAuthenticatePlaintext() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(.waitingToStartAuthentication)
        
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.plaintext), .sendPasswordMessage(.cleartext, authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.ok), .wait)
    }
    
    func testAuthenticateMD5() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(.waitingToStartAuthentication)
        let salt: (UInt8, UInt8, UInt8, UInt8) = (0, 1, 2, 3)
        
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.md5(salt: salt)), .sendPasswordMessage(.md5(salt: salt), authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.ok), .wait)
    }
    
    func testAuthenticateOkAfterStartUpWithoutAuthChallenge() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(.waitingToStartAuthentication)
        
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.ok), .wait)
    }
    
    func testAuthenticationFailure() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(.waitingToStartAuthentication)
        let salt: (UInt8, UInt8, UInt8, UInt8) = (0, 1, 2, 3)
        
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.md5(salt: salt)), .sendPasswordMessage(.md5(salt: salt), authContext))
        let fields: [PSQLBackendMessage.Field: String] = [
            .message: "password authentication failed for user \"postgres\"",
            .severity: "FATAL",
            .sqlState: "28P01",
            .localizedSeverity: "FATAL",
            .routine: "auth_failed",
            .line: "334",
            .file: "auth.c"
        ]
        XCTAssertEqual(state.errorReceived(.init(fields: fields)),
                       .fireErrorAndCloseConnetion(.server(.init(fields: fields))))
    }

    // MARK: Test unsupported messages
    
    func testUnsupportedAuthMechanism() {
        let unsupported: [(PSQLBackendMessage.Authentication, PSQLAuthScheme)] = [
            (.kerberosV5, .kerberosV5),
            (.scmCredential, .scmCredential),
            (.gss, .gss),
            (.sspi, .sspi),
            (.sasl(names: ["haha"]), .sasl(mechanisms: ["haha"])),
        ]
        
        for (message, mechanism) in unsupported {
            let authContext = AuthContext(username: "test", password: "abc123", database: "test")
            var state = ConnectionStateMachine(.waitingToStartAuthentication)
            XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
            XCTAssertEqual(state.authenticationMessageReceived(message),
                           .fireErrorAndCloseConnetion(.unsupportedAuthMechanism(mechanism)))
        }
    }
    
    func testUnexpectedMessagesAfterStartUp() {
        var buffer = ByteBuffer()
        buffer.writeBytes([0, 1, 2, 3, 4, 5, 6, 7, 8])
        let unexpected: [PSQLBackendMessage.Authentication] = [
            .gssContinue(data: buffer),
            .saslContinue(data: buffer),
            .saslFinal(data: buffer)
        ]
        
        for message in unexpected {
            let authContext = AuthContext(username: "test", password: "abc123", database: "test")
            var state = ConnectionStateMachine(.waitingToStartAuthentication)
            XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
            XCTAssertEqual(state.authenticationMessageReceived(message),
                           .fireErrorAndCloseConnetion(.unexpectedBackendMessage(.authentication(message))))
        }
    }
    
    func testUnexpectedMessagesAfterPasswordSent() {
        let salt: (UInt8, UInt8, UInt8, UInt8) = (0, 1, 2, 3)
        var buffer = ByteBuffer()
        buffer.writeBytes([0, 1, 2, 3, 4, 5, 6, 7, 8])
        let unexpected: [PSQLBackendMessage.Authentication] = [
            .kerberosV5,
            .md5(salt: (0, 1, 2, 3)),
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
            var state = ConnectionStateMachine(.waitingToStartAuthentication)
            XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
            XCTAssertEqual(state.authenticationMessageReceived(.md5(salt: salt)), .sendPasswordMessage(.md5(salt: salt), authContext))
            XCTAssertEqual(state.authenticationMessageReceived(message),
                           .fireErrorAndCloseConnetion(.unexpectedBackendMessage(.authentication(message))))
        }
    }
}
