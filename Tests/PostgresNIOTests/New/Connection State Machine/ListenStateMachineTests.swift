import NIOCore
import NIOEmbedded
import Testing
@testable import PostgresNIO

@Suite struct ListenStateMachineTests {

    // Reproducer: cancelListening in .initialized state must not crash.
    // This happens when a cancel races with startListeningFailed.
    @Test func cancelInInitializedStateDoesNotCrash() {
        var state = ListenStateMachine.ChannelState()
        switch state.cancelListening(id: 1) {
        case .none:
            break
        default:
            Issue.record("Expected .none")
        }
    }

    /// Drives the exact race through the PostgresChannelHandler (which owns both
    /// the ConnectionStateMachine and the ListenStateMachine):
    ///   1. Write HandlerTask.startListening → LISTEN query is sent to the backend
    ///   2. Backend responds with an error → startListeningFailed resets to .initialized
    ///   3. cancelNotificationListener (late cancel) → must not crash
    @Test func cancelAfterStartListeningFailedThroughChannelHandler() throws {
        let eventLoop = EmbeddedEventLoop()
        defer { try! eventLoop.syncShutdownGracefully() }

        var options = PostgresConnection.Configuration.Options()
        options.requireBackendKeyData = true
        let config = PostgresConnection.InternalConfiguration(
            connection: .unresolvedTCP(host: "127.0.0.1", port: 5432),
            username: "test",
            password: "password",
            database: "postgres",
            tls: .disable,
            options: options
        )
        let handler = PostgresChannelHandler(
            configuration: config,
            eventLoop: eventLoop,
            logger: .psqlNoOpLogger,
            configureSSLCallback: nil
        )
        // Only add the backend message encoder for inbound so we can write
        // PostgresBackendMessage values directly.
        let embedded = EmbeddedChannel(handlers: [
            ReverseMessageToByteHandler(PSQLBackendMessageEncoder()),
            handler,
        ], loop: eventLoop)

        // --- Get the connection to readyForQuery ---
        let connectPromise: EventLoopPromise<Void>? = nil
        embedded.connect(to: try SocketAddress(ipAddress: "127.0.0.1", port: 5432), promise: connectPromise)

        try embedded.writeInbound(PostgresBackendMessage.authentication(.ok))
        try embedded.writeInbound(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 5678)))
        try embedded.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        // Drain any outbound bytes (startup message, etc.)
        while let _ = try embedded.readOutbound(as: ByteBuffer.self) {}

        // --- 1. Start listening: write a startListening task ---
        let listenChannel = "test_channel"
        let listenerID = 1
        let listenContext = PostgresListenContext(promise: eventLoop.makePromise(of: Void.self))
        let listener = NotificationListener(
            channel: listenChannel,
            id: listenerID,
            eventLoop: eventLoop,
            context: listenContext,
            closure: { _, _ in }
        )

        try embedded.writeOutbound(HandlerTask.startListening(listener))

        // Drain the LISTEN query bytes from the outbound buffer.
        while let _ = try embedded.readOutbound(as: ByteBuffer.self) {}

        // --- 2. Backend fails the LISTEN query ---
        // Use a SQLSTATE that does not close the connection.
        try embedded.writeInbound(PostgresBackendMessage.error(.init(fields: [
            .sqlState: "42501",  // insufficient_privilege
        ])))
        try embedded.writeInbound(PostgresBackendMessage.readyForQuery(.idle))

        // The listener has been failed at this point. The listen channel state
        // is back to .initialized.

        // --- 3. Late cancel arrives, as it would from the stream's onTermination
        //    handler. The old code would fatalError here. ---
        handler.cancelNotificationListener(channel: listenChannel, id: listenerID)

        // If we get here without crashing, the fix works.
        #expect(embedded.isActive)
    }
}
