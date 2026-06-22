import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing


@Suite struct NoKeepAliveBehaviorTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testNoKeepAlive() {
        let keepAliveBehavior = NoOpKeepAliveBehavior(connectionType: MockConnection.self)
        #expect(keepAliveBehavior.keepAliveFrequency == nil)
    }
}
