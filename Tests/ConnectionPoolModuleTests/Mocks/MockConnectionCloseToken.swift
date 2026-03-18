@testable import _ConnectionPoolModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct MockConnectionCloseToken: Hashable, Sendable {
    var connectionID: Int

    init(_ connectionID: Int) {
        self.connectionID = connectionID
    }
}
