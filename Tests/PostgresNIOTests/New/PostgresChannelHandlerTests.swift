import Testing
import NIOCore
import NIOTLS
import NIOSSL
import NIOEmbedded
@testable import PostgresNIO

@Suite
struct PostgresChannelHandlerTests {
    let eventLoop = EmbeddedEventLoop()

    // MARK: Startup
    
    @Test func handlerAddedWithoutSSL() throws {
        let config = self.testConnectionConfiguration()
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: self.eventLoop,
            logger: .psqlNoOpLogger,
            configureSSLCallback: nil
        )
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)
        defer {
            #expect(throws: Never.self) {
                try embedded.finish()
            }
        }

        embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil)
        let maybeMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self)
        guard case .startup(let startup) = maybeMessage else {
            Issue.record("Unexpected message")
            return
        }
        
        #expect(startup.parameters.user == config.username)
        #expect(startup.parameters.database == config.database)
        #expect(startup.parameters.options.isEmpty)

        try embedded.writeInbound(PostgresBackendMessage.authentication(.ok))
        try embedded.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try embedded.writeInbound(PostgresBackendMessage.readyForQuery(.idle))
    }
    
    @Test func establishSSLCallbackIsCalledIfSSLIsSupported() throws {
        var config = self.testConnectionConfiguration()
        config.tls = .require(try NIOSSLContext(configuration: .makeClientConfiguration()))
        var addSSLCallbackIsHit = false
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: self.eventLoop,
            logger: .psqlNoOpLogger
        ) { channel, _ in
            addSSLCallbackIsHit = true
        }
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)

        embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil)
        try #expect(.sslRequest == embedded.readOutbound(as: PostgresFrontendMessage.self))
        try embedded.writeInbound(PostgresBackendMessage.sslSupported)
        
        // a NIOSSLHandler has been added, after it SSL had been negotiated
        #expect(addSSLCallbackIsHit)
        
        // signal that the ssl connection has been established
        embedded.pipeline.fireUserInboundEventTriggered(TLSUserEvent.handshakeCompleted(negotiatedProtocol: ""))
        
        // startup message should be issued
        let maybeStartupMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self)
        guard case .startup(let startupMessage) = maybeStartupMessage else {
            Issue.record("Unexpected message")
            return
        }
        
        #expect(startupMessage.parameters.user == config.username)
        #expect(startupMessage.parameters.database == config.database)
        #expect(startupMessage.parameters.replication == .false)
    }

    @Test func establishSSLCallbackIsNotCalledIfSSLIsSupportedButAnotherMEssageIsSentAsWell() throws {
        var config = self.testConnectionConfiguration()
        config.tls = .require(try NIOSSLContext(configuration: .makeClientConfiguration()))
        var addSSLCallbackIsHit = false
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: self.eventLoop,
            logger: .psqlNoOpLogger
        ) { channel, _ in
            addSSLCallbackIsHit = true
        }
        let eventHandler = TestEventHandler()
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            handler,
            eventHandler
        ], loop: self.eventLoop)

        embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil)
        try #expect(.sslRequest == embedded.readOutbound(as: PostgresFrontendMessage.self))

        var responseBuffer = ByteBuffer()
        responseBuffer.writeInteger(UInt8(ascii: "S"))
        responseBuffer.writeInteger(UInt8(ascii: "1"))
        try embedded.writeInbound(responseBuffer)

        #expect(!addSSLCallbackIsHit)

        // the event handler should have seen an error
        #expect(eventHandler.errors.count == 1)

        // the connections should be closed
        #expect(!embedded.isActive)
    }

    @Test func sslUnsupportedClosesConnection() throws {
        let config = self.testConnectionConfiguration(tls: .require(try NIOSSLContext(configuration: .makeClientConfiguration())))
        
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: self.eventLoop,
            logger: .psqlNoOpLogger
        ) { channel, _ in
            Issue.record("This callback should never be exectuded")
            throw PSQLError.sslUnsupported
        }
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)
        let eventHandler = TestEventHandler()
        try embedded.pipeline.syncOperations.addHandler(eventHandler, position: .last)
        
        embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil)
        #expect(embedded.isActive)
        
        // read the ssl request message
        #expect(try embedded.readOutbound(as: PostgresFrontendMessage.self) == .sslRequest)
        try embedded.writeInbound(PostgresBackendMessage.sslUnsupported)
        
        // the event handler should have seen an error
        #expect(eventHandler.errors.count == 1)
        
        // the connections should be closed
        #expect(!embedded.isActive)
    }
    
    // MARK: Run Actions
    
    @Test func runAuthenticateMD5Password() throws {
        let config = self.testConnectionConfiguration()
        let authContext = AuthContext(
            username: config.username ?? "something wrong",
            password: config.password,
            database: config.database
        )
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: self.eventLoop,
            logger: .psqlNoOpLogger,
            configureSSLCallback: nil
        )
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            handler
        ], loop: self.eventLoop)
        embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil)

        #expect(try embedded.readOutbound(as: PostgresFrontendMessage.self) == .startup(.versionThree(parameters: authContext.toStartupParameters())))
        let salt: UInt32 = 0x00_01_02_03

        let encoder = PSQLBackendMessageEncoder()
        var byteBuffer = ByteBuffer()
        encoder.encode(data: .authentication(.md5(salt: salt)), out: &byteBuffer)
        try embedded.writeInbound(byteBuffer)
        
        #expect(try embedded.readOutbound(as: PostgresFrontendMessage.self) == .password(.init(value: "md522d085ed8dc3377968dc1c1a40519a2a")))
    }
    
    @Test func runAuthenticateCleartext() throws {
        let password = "postgres"
        let config = self.testConnectionConfiguration(password: password)
        let authContext = AuthContext(
            username: config.username ?? "something wrong",
            password: config.password,
            database: config.database
        )
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: self.eventLoop,
            logger: .psqlNoOpLogger,
            configureSSLCallback: nil
        )
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler
        ], loop: self.eventLoop)

        try embedded.connect(to: .init(ipAddress: "1.2.3.4", port: 5678)).wait()
        #expect(try embedded.readOutbound(as: PostgresFrontendMessage.self) == .startup(.versionThree(parameters: authContext.toStartupParameters())))

        try embedded.writeInbound(PostgresBackendMessage.authentication(.plaintext))
        #expect(try embedded.readOutbound(as: PostgresFrontendMessage.self) == .password(.init(value: password)))
    }

    @Test func handlerThatSendsMultipleWrongMessages() throws {
        let config = self.testConnectionConfiguration()
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: self.eventLoop,
            logger: .psqlNoOpLogger,
            configureSSLCallback: nil
        )
        let embedded = EmbeddedChannel(handlers: [
            ReverseByteToMessageHandler(PSQLFrontendMessageDecoder()),
            handler
        ], loop: self.eventLoop)

        embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil)
        let maybeMessage = try embedded.readOutbound(as: PostgresFrontendMessage.self)
        guard case .startup(let startup) = maybeMessage else {
            Issue.record("Unexpected message")
            return
        }

        #expect(startup.parameters.user == config.username)
        #expect(startup.parameters.database == config.database)
        #expect(startup.parameters.options.isEmpty)
        #expect(startup.parameters.replication == .false)

        var buffer = ByteBuffer()
        buffer.writeMultipleIntegers(UInt8(ascii: "R"), UInt32(8), Int32(0))
        buffer.writeMultipleIntegers(UInt8(ascii: "K"), UInt32(12), Int32(1234), Int32(5678))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        try embedded.writeInbound(buffer)
        #expect(embedded.isActive)

        buffer.clear()
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))
        buffer.writeMultipleIntegers(UInt8(ascii: "Z"), UInt32(5), UInt8(ascii: "I"))

        #expect(throws: (any Error).self) { try embedded.writeInbound(buffer) }
        #expect(!embedded.isActive)
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
    
    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        guard let psqlError = error as? PSQLError else {
            Issue.record("Unexpected error type received: \(error)")
            return
        }
        self.errors.append(psqlError)
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        guard let psqlEvent = event as? PSQLEvent else {
            Issue.record("Unexpected event type received: \(event)")
            return
        }
        self.events.append(psqlEvent)
    }
}

extension AuthContext {
    func toStartupParameters() -> PostgresFrontendMessage.Startup.Parameters {
        PostgresFrontendMessage.Startup.Parameters(
            user: self.username,
            database: self.database,
            options: self.additionalParameters,
            replication: .false
        )
    }
}
