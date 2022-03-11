import XCTest
import NIOCore
import NIOTLS
import NIOSSL
import NIOEmbedded
@testable import PostgresNIO

class PostgresChannelHandlerTests: XCTestCase {
    
    // MARK: Startup
    
    func testHandlerAddedWithoutSSL() {
        let config = self.testConnectionConfiguration()
        let handler = PostgresChannelHandler(configuration: config, configureSSLCallback: nil)
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ])
        defer { XCTAssertNoThrow(try embedded.finish()) }
        
        var maybeMessage: PostgresFrontendMessage?
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertNoThrow(maybeMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        guard case .startup(let startup) = maybeMessage else {
            return XCTFail("Unexpected message")
        }
        
        XCTAssertEqual(startup.parameters.user, config.authentication?.username)
        XCTAssertEqual(startup.parameters.database, config.authentication?.database)
        XCTAssertEqual(startup.parameters.options, nil)
        XCTAssertEqual(startup.parameters.replication, .false)
        
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.authentication(.ok)))
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678))))
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.readyForQuery(.idle)))
    }
    
    func testEstablishSSLCallbackIsCalledIfSSLIsSupported() {
        var config = self.testConnectionConfiguration()
        XCTAssertNoThrow(config.tls = .require(try NIOSSLContext(configuration: .makeClientConfiguration())))
        var addSSLCallbackIsHit = false
        let handler = PostgresChannelHandler(configuration: config) { channel in
            addSSLCallbackIsHit = true
        }
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ])
        
        var maybeMessage: PostgresFrontendMessage?
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertNoThrow(maybeMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        guard case .sslRequest(let request) = maybeMessage else {
            return XCTFail("Unexpected message")
        }
        
        XCTAssertEqual(request.code, 80877103)
        
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.sslSupported))
        
        // a NIOSSLHandler has been added, after it SSL had been negotiated
        XCTAssertTrue(addSSLCallbackIsHit)
        
        // signal that the ssl connection has been established
        embedded.pipeline.fireUserInboundEventTriggered(TLSUserEvent.handshakeCompleted(negotiatedProtocol: ""))
        
        // startup message should be issued
        var maybeStartupMessage: PostgresFrontendMessage?
        XCTAssertNoThrow(maybeStartupMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        guard case .startup(let startupMessage) = maybeStartupMessage else {
            return XCTFail("Unexpected message")
        }
        
        XCTAssertEqual(startupMessage.parameters.user, config.authentication?.username)
        XCTAssertEqual(startupMessage.parameters.database, config.authentication?.database)
        XCTAssertEqual(startupMessage.parameters.replication, .false)
    }
    
    func testSSLUnsupportedClosesConnection() {
        var config = self.testConnectionConfiguration()
        XCTAssertNoThrow(config.tls = .require(try NIOSSLContext(configuration: .makeClientConfiguration())))
        
        let handler = PostgresChannelHandler(configuration: config) { channel in
            XCTFail("This callback should never be exectuded")
            throw PSQLError.sslUnsupported
        }
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ])
        let eventHandler = TestEventHandler()
        XCTAssertNoThrow(try embedded.pipeline.addHandler(eventHandler, position: .last).wait())
        
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertTrue(embedded.isActive)
        
        // read the ssl request message
        XCTAssertEqual(try embedded.readOutbound(as: PostgresFrontendMessage.self), .sslRequest(.init()))
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.sslUnsupported))
        
        // the event handler should have seen an error
        XCTAssertEqual(eventHandler.errors.count, 1)
        
        // the connections should be closed
        XCTAssertFalse(embedded.isActive)
    }
    
    // MARK: Run Actions
    
    func testRunAuthenticateMD5Password() {
        let config = self.testConnectionConfiguration()
        let authContext = AuthContext(
            username: config.authentication?.username ?? "something wrong",
            password: config.authentication?.password,
            database: config.authentication?.database
        )
        let state = ConnectionStateMachine(.waitingToStartAuthentication)
        let handler = PostgresChannelHandler(configuration: config, state: state, configureSSLCallback: nil)
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ])
        
        embedded.triggerUserOutboundEvent(PSQLOutgoingEvent.authenticate(authContext), promise: nil)
        XCTAssertEqual(try embedded.readOutbound(as: PostgresFrontendMessage.self), .startup(.versionThree(parameters: authContext.toStartupParameters())))
        
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.authentication(.md5(salt: (0,1,2,3)))))
        
        var message: PostgresFrontendMessage?
        XCTAssertNoThrow(message = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        
        XCTAssertEqual(message, .password(.init(value: "md522d085ed8dc3377968dc1c1a40519a2a")))
    }
    
    func testRunAuthenticateCleartext() {
        let password = "postgres"
        var config = self.testConnectionConfiguration()
        config.authentication?.password = password
        
        let authContext = AuthContext(
            username: config.authentication?.username ?? "something wrong",
            password: config.authentication?.password,
            database: config.authentication?.database
        )
        let state = ConnectionStateMachine(.waitingToStartAuthentication)
        let handler = PostgresChannelHandler(configuration: config, state: state, configureSSLCallback: nil)
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ])
        
        embedded.triggerUserOutboundEvent(PSQLOutgoingEvent.authenticate(authContext), promise: nil)
        XCTAssertEqual(try embedded.readOutbound(as: PostgresFrontendMessage.self), .startup(.versionThree(parameters: authContext.toStartupParameters())))
        
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.authentication(.plaintext)))
        
        var message: PostgresFrontendMessage?
        XCTAssertNoThrow(message = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        
        XCTAssertEqual(message, .password(.init(value: password)))
    }
    
    // MARK: Helpers
    
    func testConnectionConfiguration(
        host: String = "127.0.0.1",
        port: Int = 5432,
        username: String = "test",
        database: String = "postgres",
        password: String = "password",
        tls: PostgresConnection.Configuration.TLS = .disable
    ) -> PostgresConnection.InternalConfiguration {
        let authentication = PostgresConnection.Configuration.Authentication(
            username: username,
            database: database,
            password: password
        )

        return PostgresConnection.InternalConfiguration(
            connection: .unresolved(host: host, port: port),
            authentication: authentication,
            tls: tls
        )
    }
}

class TestEventHandler: ChannelInboundHandler {
    typealias InboundIn = Never
    
    var errors = [PSQLError]()
    var events = [PSQLEvent]()
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        guard let psqlError = error as? PSQLError else {
            return XCTFail("Unexpected error type received: \(error)")
        }
        self.errors.append(psqlError)
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        guard let psqlEvent = event as? PSQLEvent else {
            return XCTFail("Unexpected event type received: \(event)")
        }
        self.events.append(psqlEvent)
    }
}
