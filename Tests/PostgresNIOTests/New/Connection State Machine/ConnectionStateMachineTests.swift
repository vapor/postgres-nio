import Testing
@testable import PostgresNIO
@testable import NIOCore
import NIOPosix
import NIOSSL

@Suite struct ConnectionStateMachineTests {

    @Test func testStartup() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
        #expect(state.authenticationMessageReceived(.plaintext) == .sendPasswordMessage(.cleartext, authContext))
        #expect(state.authenticationMessageReceived(.ok) == .wait)
    }
    
    @Test func testSSLStartupSuccess() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .require) == .sendSSLRequest)
        #expect(state.sslSupportedReceived(unprocessedBytes: 0) == .establishSSLConnection)
        #expect(state.sslHandlerAdded() == .wait)
        #expect(state.sslEstablished() == .provideAuthenticationContext)
        #expect(state.provideAuthenticationContext(authContext) == .sendStartupMessage(authContext))
        let salt: UInt32 = 0x00_01_02_03
        #expect(state.authenticationMessageReceived(.md5(salt: salt)) == .sendPasswordMessage(.md5(salt: salt), authContext))
    }

    @Test func testSSLStartupFailureTooManyBytesRemaining() {
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .require) == .sendSSLRequest)
        let failError = PSQLError.receivedUnencryptedDataAfterSSLRequest
        #expect(state.sslSupportedReceived(unprocessedBytes: 1) == .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: failError, closePromise: nil)))
    }

    @Test func testSSLStartupFailHandler() {
        struct SSLHandlerAddError: Error, Equatable {}
        
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        #expect(state.connected(tls: .require) == .sendSSLRequest)
        #expect(state.sslSupportedReceived(unprocessedBytes: 0) == .establishSSLConnection)
        let failError = PSQLError.failedToAddSSLHandler(underlying: SSLHandlerAddError())
        #expect(state.errorHappened(failError) == .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: failError, closePromise: nil)))
    }
    
    @Test func testTLSRequiredStartupSSLUnsupported() {
        var state = ConnectionStateMachine(requireBackendKeyData: true)
        
        #expect(state.connected(tls: .require) == .sendSSLRequest)
        #expect(state.sslUnsupportedReceived() ==
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: PSQLError.sslUnsupported, closePromise: nil)))
    }

    @Test func testTLSPreferredStartupSSLUnsupported() {
        var state = ConnectionStateMachine(requireBackendKeyData: true)

        #expect(state.connected(tls: .prefer) == .sendSSLRequest)
        #expect(state.sslUnsupportedReceived() == .provideAuthenticationContext)
    }
        
    @Test func testParameterStatusReceivedAndBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]))
        
        #expect(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "application_name", value: "")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")) == .wait)

        #expect(state.backendKeyDataReceived(.init(processID: 2730, secretKey: 882037977)) == .wait)
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }
    
    @Test func testBackendKeyAndParameterStatusReceivedAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]))
        
        #expect(state.backendKeyDataReceived(.init(processID: 2730, secretKey: 882037977)) == .wait)

        #expect(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "application_name", value: "")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")) == .wait)

        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }
    
    @Test func testReadyForQueryReceivedWithoutBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]), requireBackendKeyData: true)
        
        #expect(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "application_name", value: "")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")) == .wait)

        #expect(state.readyForQueryReceived(.idle) ==
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: PSQLError.unexpectedBackendMessage(.readyForQuery(.idle)), closePromise: nil)))
    }
    
    @Test func testReadyForQueryReceivedWithoutUnneededBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]), requireBackendKeyData: false)
        
        #expect(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "application_name", value: "")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")) == .wait)
        #expect(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")) == .wait)

        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }
    
    @Test func testErrorIsIgnoredWhenClosingConnection() {
        // test ignore unclean shutdown when closing connection
        var stateIgnoreChannelError = ConnectionStateMachine(.closing(nil))

        #expect(stateIgnoreChannelError.errorHappened(.connectionError(underlying: NIOSSLError.uncleanShutdown)) == .wait)
        #expect(stateIgnoreChannelError.closed() == .fireChannelInactive)

        // test ignore any other error when closing connection
        
        var stateIgnoreErrorMessage = ConnectionStateMachine(.closing(nil))
        #expect(stateIgnoreErrorMessage.errorReceived(.init(fields: [:])) == .wait)
        #expect(stateIgnoreErrorMessage.closed() == .fireChannelInactive)
    }
    
    @Test func testFailQueuedQueriesOnAuthenticationFailure() throws {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        let salt: UInt32 = 0x00_01_02_03

        let queryPromise = NIOSingletons.posixEventLoopGroup.next().makePromise(of: PSQLRowStream.self)

        var state = ConnectionStateMachine(requireBackendKeyData: true)
        let extendedQueryContext = ExtendedQueryContext(
            query: "Select version()",
            logger: .psqlTest,
            promise: queryPromise)

        #expect(state.enqueue(task: .extendedQuery(extendedQueryContext)) == .wait)
        #expect(state.connected(tls: .disable) == .provideAuthenticationContext)
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
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [.extendedQuery(extendedQueryContext)], error: .server(.init(fields: fields)), closePromise: nil)))
        
        #expect(queryPromise.futureResult._value == nil)

        // make sure we don't crash
        queryPromise.fail(PSQLError.server(.init(fields: fields)))
    }
}
