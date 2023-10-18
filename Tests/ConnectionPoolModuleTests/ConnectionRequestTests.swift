@testable import _ConnectionPoolModule
import XCTest

final class ConnectionRequestTests: XCTestCase {

    func testHappyPath() async throws {
        let mockConnection = MockConnection(id: 1)
        let connection = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<MockConnection, any Error>) in
            let request = ConnectionRequest(id: 42, continuation: continuation)
            XCTAssertEqual(request.id, 42)
            continuation.resume(with: .success(mockConnection))
        }

        XCTAssert(connection === mockConnection)
    }

    func testSadPath() async throws {
        do {
            _ = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<MockConnection, any Error>) in
                continuation.resume(with: .failure(ConnectionPoolError.requestCancelled))
            }
            XCTFail("This point should not be reached")
        } catch {
            XCTAssertEqual(error as? ConnectionPoolError, .requestCancelled)
        }
    }
}
