@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import XCTest

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PoolStateMachine_RequestQueueTests: XCTestCase {

    typealias TestQueue = TestPoolStateMachine.RequestQueue

    func testHappyPath() {
        var queue = TestQueue()
        XCTAssert(queue.isEmpty)

        let request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        XCTAssertEqual(queue.count, 1)
        XCTAssertFalse(queue.isEmpty)
        let popResult = queue.pop(max: 3)
        XCTAssert(popResult.elementsEqual([request1]))
        XCTAssert(queue.isEmpty)
        XCTAssertEqual(queue.count, 0)
    }

    func testEnqueueAndPopMultipleRequests() {
        var queue = TestQueue()
        XCTAssert(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            XCTAssertEqual(queue.count, 3)
            XCTAssertFalse(queue.isEmpty)
            let popResult = queue.pop(max: 3)
            XCTAssert(popResult.elementsEqual([request1, request2, request3]))
            XCTAssert(queue.isEmpty)
            XCTAssertEqual(queue.count, 0)
        }
        XCTAssert(isKnownUniquelyReferenced(&request1))
        XCTAssert(isKnownUniquelyReferenced(&request2))
        XCTAssert(isKnownUniquelyReferenced(&request3))
    }

    func testEnqueueAndPopOnlyOne() {
        var queue = TestQueue()
        XCTAssert(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            XCTAssertEqual(queue.count, 3)
            XCTAssertFalse(queue.isEmpty)
            let popResult = queue.pop(max: 1)
            XCTAssert(popResult.elementsEqual([request1]))
            XCTAssertFalse(queue.isEmpty)
            XCTAssertEqual(queue.count, 2)

            let removeAllResult = queue.removeAll()
            XCTAssert(Set(removeAllResult) == [request2, request3])
        }
        XCTAssert(isKnownUniquelyReferenced(&request1))
        XCTAssert(isKnownUniquelyReferenced(&request2))
        XCTAssert(isKnownUniquelyReferenced(&request3))
    }

    func testCancellation() {
        var queue = TestQueue()
        XCTAssert(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            XCTAssertEqual(queue.count, 3)
            let returnedRequest2 = queue.remove(request2.id)
            XCTAssert(returnedRequest2 === request2)
            XCTAssertEqual(queue.count, 2)
            XCTAssertFalse(queue.isEmpty)
        }

        // still retained by the deque inside the queue
        XCTAssertEqual(queue.requests.count, 2)
        XCTAssertEqual(queue.queue.count, 3)

        do {
            XCTAssertEqual(queue.count, 2)
            XCTAssertFalse(queue.isEmpty)
            let popResult = queue.pop(max: 3)
            XCTAssert(popResult.elementsEqual([request1, request3]))
            XCTAssert(queue.isEmpty)
            XCTAssertEqual(queue.count, 0)
        }

        XCTAssert(isKnownUniquelyReferenced(&request1))
        XCTAssert(isKnownUniquelyReferenced(&request2))
        XCTAssert(isKnownUniquelyReferenced(&request3))
    }

    func testRemoveAllAfterCancellation() {
        var queue = TestQueue()
        XCTAssert(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            XCTAssertEqual(queue.count, 3)
            let returnedRequest2 = queue.remove(request2.id)
            XCTAssert(returnedRequest2 === request2)
            XCTAssertEqual(queue.count, 2)
            XCTAssertFalse(queue.isEmpty)
        }

        // still retained by the deque inside the queue
        XCTAssertEqual(queue.requests.count, 2)
        XCTAssertEqual(queue.queue.count, 3)

        do {
            XCTAssertEqual(queue.count, 2)
            XCTAssertFalse(queue.isEmpty)
            let removeAllResult = queue.removeAll()
            XCTAssert(Set(removeAllResult) == [request1, request3])
            XCTAssert(queue.isEmpty)
            XCTAssertEqual(queue.count, 0)
        }

        XCTAssert(isKnownUniquelyReferenced(&request1))
        XCTAssert(isKnownUniquelyReferenced(&request2))
        XCTAssert(isKnownUniquelyReferenced(&request3))
    }
}
