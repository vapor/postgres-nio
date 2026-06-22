@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

@Suite struct ConnectionRequestTests {

    @Test func testHappyPath() async throws {
        let mockConnection = MockConnection(id: 1)
        let lease: ConnectionLease<MockConnection> = try await withCheckedThrowingContinuation { (continuation) in
            let request = ConnectionRequest(id: 42, continuation: continuation)
            #expect(request.id == 42)
            let lease = ConnectionLease(connection: mockConnection) { _ in }
            continuation.resume(with: .success(lease))
        }

        #expect(lease.connection === mockConnection)
    }

    @Test func testSadPath() async throws {
        do {
            let _: ConnectionLease<MockConnection> = try await withCheckedThrowingContinuation { (continuation) in
                let request = ConnectionRequest(id: 42, continuation: continuation)
                #expect(request.id == 42)
                request.complete(with: .failure(ConnectionPoolError.requestCancelled))
            }
            Issue.record("This point should not be reached")
        } catch {
            #expect(error as? ConnectionPoolError == .requestCancelled)
        }
    }
}
