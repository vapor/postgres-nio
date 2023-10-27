import _ConnectionPoolModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct MockPingPongBehavior: ConnectionKeepAliveBehavior {
    let keepAliveFrequency: Duration?

    init(keepAliveFrequency: Duration?) {
        self.keepAliveFrequency = keepAliveFrequency
    }

    func runKeepAlive(for connection: MockConnection) async throws {
        preconditionFailure()
    }
}
