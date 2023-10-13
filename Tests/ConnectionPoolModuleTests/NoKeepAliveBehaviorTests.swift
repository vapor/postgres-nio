import _ConnectionPoolModule
import XCTest

final class NoKeepAliveBehaviorTests: XCTestCase {
    func testNoKeepAlive() {
        let keepAliveBehavior = NoOpKeepAliveBehavior(connectionType: MockConnection.self)
        XCTAssertNil(keepAliveBehavior.keepAliveFrequency)
    }
}
