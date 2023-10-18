import NIOCore
import NIOEmbedded
import XCTest
@testable import _ConnectionPoolModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
typealias TestPoolStateMachine = PoolStateMachine<
    MockConnection,
    ConnectionIDGenerator,
    MockConnection.ID,
    MockRequest,
    MockRequest.ID,
    MockTimerCancellationToken
>
