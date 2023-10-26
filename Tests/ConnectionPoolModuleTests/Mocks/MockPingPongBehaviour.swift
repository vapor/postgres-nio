import _ConnectionPoolModule

struct MockPingPongBehavior: ConnectionKeepAliveBehavior {
    let keepAliveFrequency: Duration?

    init(keepAliveFrequency: Duration?) {
        self.keepAliveFrequency = keepAliveFrequency
    }

    func runKeepAlive(for connection: MockConnection) async throws {
        preconditionFailure()
    }
}
