@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

@Suite struct ConnectionRequestTests {

    @Test func testHappyPath() async throws {
        let mockConnection = MockConnection(id: 1)
        let lease = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<ConnectionLease<MockConnection>, any Error>) in
            let request = ConnectionRequest(id: 42, continuation: continuation)
            #expect(request.id == 42)
            let lease = ConnectionLease(connection: mockConnection) { _ in }
            continuation.resume(with: .success(lease))
        }

        #expect(lease.connection === mockConnection)
    }

    @Test func testSadPath() async throws {
        do {
            _ = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<MockConnection, any Error>) in
                continuation.resume(with: .failure(ConnectionPoolError.requestCancelled))
            }
            Issue.record("This point should not be reached")
        } catch {
            #expect(error as? ConnectionPoolError == .requestCancelled)
        }
    }
}
