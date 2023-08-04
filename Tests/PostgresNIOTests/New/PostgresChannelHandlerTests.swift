import XCTest
import NIOCore
import NIOTLS
import NIOSSL
import NIOEmbedded
@testable import PostgresNIO

class PostgresChannelHandlerTests: XCTestCase {

    var eventLoop: EmbeddedEventLoop!

    override func setUp() {
        self.eventLoop = EmbeddedEventLoop()
        super.setUp()
    }

    // MARK: Startup
    
    func testHandlerAddedWithoutSSL() {
        let config = self.testConnectionConfiguration()
        let handler = PostgresChannelHandler(configuration: config, eventLoop: self.eventLoop, configureSSLCallback: nil)
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)
        defer { XCTAssertNoThrow(try embedded.finish()) }
        
        var maybeMessage: PostgresFrontendMessage?
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertNoThrow(maybeMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        guard case .startup(let startup) = maybeMessage else {
            return XCTFail("Unexpected message")
        }
        
        XCTAssertEqual(startup.parameters.user, config.username)
        XCTAssertEqual(startup.parameters.database, config.database)
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
        let handler = PostgresChannelHandler(configuration: config, eventLoop: self.eventLoop) { channel in
            addSSLCallbackIsHit = true
        }
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)

        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertEqual(.sslRequest, try embedded.readOutbound(as: PostgresFrontendMessage.self))

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
        
        XCTAssertEqual(startupMessage.parameters.user, config.username)
        XCTAssertEqual(startupMessage.parameters.database, config.database)
        XCTAssertEqual(startupMessage.parameters.replication, .false)
    }

    func testEstablishSSLCallbackIsNotCalledIfSSLIsSupportedButAnotherMEssageIsSentAsWell() {
        var config = self.testConnectionConfiguration()
        XCTAssertNoThrow(config.tls = .require(try NIOSSLContext(configuration: .makeClientConfiguration())))
        var addSSLCallbackIsHit = false
        let handler = PostgresChannelHandler(configuration: config, eventLoop: self.eventLoop) { channel in
            addSSLCallbackIsHit = true
        }
        let eventHandler = TestEventHandler()
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            handler,
            eventHandler
        ], loop: self.eventLoop)

        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertEqual(.sslRequest, try embedded.readOutbound(as: PostgresFrontendMessage.self))

        var responseBuffer = ByteBuffer()
        responseBuffer.writeInteger(UInt8(ascii: "S"))
        responseBuffer.writeInteger(UInt8(ascii: "1"))
        XCTAssertNoThrow(try embedded.writeInbound(responseBuffer))

        XCTAssertFalse(addSSLCallbackIsHit)

        // the event handler should have seen an error
        XCTAssertEqual(eventHandler.errors.count, 1)

        // the connections should be closed
        XCTAssertFalse(embedded.isActive)
    }

    func testSSLUnsupportedClosesConnection() throws {
        let config = self.testConnectionConfiguration(tls: .require(try NIOSSLContext(configuration: .makeClientConfiguration())))
        
        let handler = PostgresChannelHandler(configuration: config, eventLoop: self.eventLoop) { channel in
            XCTFail("This callback should never be exectuded")
            throw PSQLError.sslUnsupported
        }
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)
        let eventHandler = TestEventHandler()
        try embedded.pipeline.addHandler(eventHandler, position: .last).wait()
        
        embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil)
        XCTAssertTrue(embedded.isActive)
        
        // read the ssl request message
        XCTAssertEqual(try embedded.readOutbound(as: PostgresFrontendMessage.self), .sslRequest)
        try embedded.writeInbound(PostgresBackendMessage.sslUnsupported)
        
        // the event handler should have seen an error
        XCTAssertEqual(eventHandler.errors.count, 1)
        
        // the connections should be closed
        XCTAssertFalse(embedded.isActive)
    }
    
    // MARK: Run Actions
    
    func testRunAuthenticateMD5Password() {
        let config = self.testConnectionConfiguration()
        let authContext = AuthContext(
            username: config.username ?? "something wrong",
            password: config.password,
            database: config.database
        )
        let state = ConnectionStateMachine(.waitingToStartAuthentication)
        let handler = PostgresChannelHandler(configuration: config, eventLoop: self.eventLoop, state: state, configureSSLCallback: nil)
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)

        embedded.triggerUserOutboundEvent(PSQLOutgoingEvent.authenticate(authContext), promise: nil)
        XCTAssertEqual(try embedded.readOutbound(as: PostgresFrontendMessage.self), .startup(.versionThree(parameters: authContext.toStartupParameters())))
        
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.authentication(.md5(salt: (0,1,2,3)))))
        
        var message: PostgresFrontendMessage?
        XCTAssertNoThrow(message = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        
        XCTAssertEqual(message, .password(.init(value: "md522d085ed8dc3377968dc1c1a40519a2a")))
    }
    
    func testRunAuthenticateCleartext() {
        let password = "postgres"
        let config = self.testConnectionConfiguration(password: password)
        let authContext = AuthContext(
            username: config.username ?? "something wrong",
            password: config.password,
            database: config.database
        )
        let state = ConnectionStateMachine(.waitingToStartAuthentication)
        let handler = PostgresChannelHandler(configuration: config, eventLoop: self.eventLoop, state: state, configureSSLCallback: nil)
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)

        embedded.triggerUserOutboundEvent(PSQLOutgoingEvent.authenticate(authContext), promise: nil)
        XCTAssertEqual(try embedded.readOutbound(as: PostgresFrontendMessage.self), .startup(.versionThree(parameters: authContext.toStartupParameters())))
        
        XCTAssertNoThrow(try embedded.writeInbound(PostgresBackendMessage.authentication(.plaintext)))
        
        var message: PostgresFrontendMessage?
        XCTAssertNoThrow(message = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        
        XCTAssertEqual(message, .password(.init(value: password)))
    }

    func testHandlerThatSendsMultipleWrongMessages() {
        let config = self.testConnectionConfiguration()
        let handler = PostgresChannelHandler(configuration: config, eventLoop: self.eventLoop, configureSSLCallback: nil)
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            handler
        ], loop: self.eventLoop)

        var maybeMessage: PostgresFrontendMessage?
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertNoThrow(maybeMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self))
        guard case .startup(let startup) = maybeMessage else {
            return XCTFail("Unexpected message")
        }

        XCTAssertEqual(startup.parameters.user, config.username)
        XCTAssertEqual(startup.parameters.database, config.database)
        XCTAssertEqual(startup.parameters.options, nil)
        XCTAssertEqual(startup.parameters.replication, .false)

        var buffer = ByteBuffer()
        buffer.writeMultipleIntegers(UInt8(ascii: "R"), UInt32(8), Int32(0))
        buffer.writeMultipleIntegers(UInt8(ascii: "K"), UInt32(12), Int32(1234), Int32(5678))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        XCTAssertNoThrow(try embedded.writeInbound(buffer))
        XCTAssertTrue(embedded.isActive)

        buffer.clear()
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))

        XCTAssertThrowsError(try embedded.writeInbound(buffer))
        XCTAssertFalse(embedded.isActive)
    }

    // MARK: Helpers
    
    func testConnectionConfiguration(
        host: String = "127.0.0.1",
        port: Int = 5432,
        username: String = "test",
        database: String = "postgres",
        password: String = "password",
        tls: PostgresConnection.Configuration.TLS = .disable,
        connectTimeout: TimeAmount = .seconds(10),
        requireBackendKeyData: Bool = true
    ) -> PostgresConnection.InternalConfiguration {
        var options = PostgresConnection.Configuration.Options()
        options.connectTimeout = connectTimeout
        options.requireBackendKeyData = requireBackendKeyData

        return PostgresConnection.InternalConfiguration(
            connection: .unresolvedTCP(host: host, port: port),
            username: username,
            password: password,
            database: database,
            tls: tls,
            options: options
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
