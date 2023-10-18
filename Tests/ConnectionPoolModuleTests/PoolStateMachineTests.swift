import NIOCore
import NIOEmbedded
import XCTest
@testable import _ConnectionPoolModule

typealias TestPoolStateMachine = PoolStateMachine<
    MockConnection,
    ConnectionIDGenerator,
    MockConnection.ID,
    MockRequest,
    MockRequest.ID,
    MockTimerCancellationToken
>
