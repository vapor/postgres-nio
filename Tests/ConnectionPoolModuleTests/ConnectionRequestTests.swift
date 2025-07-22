@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import XCTest

final class ConnectionRequestTests: XCTestCase {

    let executor = NothingConnectionPoolExecutor()

    func testHappyPath() async throws {
        let mockConnection = MockConnection(id: 1, executor: self.executor)
        let lease = try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<ConnectionLease<MockConnection<NothingConnectionPoolExecutor>>, any Error>) in
            let request = ConnectionRequest(id: 42, continuation: continuation)
            XCTAssertEqual(request.id, 42)
            let lease = ConnectionLease(connection: mockConnection) { _ in }
            continuation.resume(with: .success(lease))
        }

        XCTAssert(lease.connection === mockConnection)
    }

    func testSadPath() async throws {
        do {
            _ = try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<MockConnection<NothingConnectionPoolExecutor>, any Error>) in
                continuation.resume(with: .failure(ConnectionPoolError.requestCancelled))
            }
            XCTFail("This point should not be reached")
        } catch {
            XCTAssertEqual(error as? ConnectionPoolError, .requestCancelled)
        }
    }
}
