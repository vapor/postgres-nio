@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

@Suite struct PoolStateMachine_RequestQueueTests {

    typealias TestQueue = TestPoolStateMachine.RequestQueue

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testHappyPath() {
        var queue = TestQueue()
        #expect(queue.isEmpty)

        let request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        #expect(queue.count == 1)
        #expect(!queue.isEmpty)
        let popResult = queue.pop(max: 3)
        #expect(popResult.elementsEqual([request1]))
        #expect(queue.isEmpty)
        #expect(queue.count == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testEnqueueAndPopMultipleRequests() {
        var queue = TestQueue()
        #expect(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            #expect(queue.count == 3)
            #expect(!queue.isEmpty)
            let popResult = queue.pop(max: 3)
            #expect(popResult.elementsEqual([request1, request2, request3]))
            #expect(queue.isEmpty)
            #expect(queue.count == 0)
        }
        #expect(isKnownUniquelyReferenced(&request1))
        #expect(isKnownUniquelyReferenced(&request2))
        #expect(isKnownUniquelyReferenced(&request3))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testEnqueueAndPopOnlyOne() {
        var queue = TestQueue()
        #expect(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            #expect(queue.count == 3)
            #expect(!queue.isEmpty)
            let popResult = queue.pop(max: 1)
            #expect(popResult.elementsEqual([request1]))
            #expect(!queue.isEmpty)
            #expect(queue.count == 2)

            let removeAllResult = queue.removeAll()
            #expect(Set(removeAllResult) == [request2, request3])
        }
        #expect(isKnownUniquelyReferenced(&request1))
        #expect(isKnownUniquelyReferenced(&request2))
        #expect(isKnownUniquelyReferenced(&request3))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testCancellation() {
        var queue = TestQueue()
        #expect(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            #expect(queue.count == 3)
            let returnedRequest2 = queue.remove(request2.id)
            #expect(returnedRequest2 === request2)
            #expect(queue.count == 2)
            #expect(!queue.isEmpty)
        }

        // still retained by the deque inside the queue
        #expect(queue.requests.count == 2)
        #expect(queue.queue.count == 3)

        do {
            #expect(queue.count == 2)
            #expect(!queue.isEmpty)
            let popResult = queue.pop(max: 3)
            #expect(popResult.elementsEqual([request1, request3]))
            #expect(queue.isEmpty)
            #expect(queue.count == 0)
        }

        #expect(isKnownUniquelyReferenced(&request1))
        #expect(isKnownUniquelyReferenced(&request2))
        #expect(isKnownUniquelyReferenced(&request3))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRemoveAllAfterCancellation() {
        var queue = TestQueue()
        #expect(queue.isEmpty)

        var request1 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request1)
        var request2 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request2)
        var request3 = MockRequest(connectionType: MockConnection.self)
        queue.queue(request3)

        do {
            #expect(queue.count == 3)
            let returnedRequest2 = queue.remove(request2.id)
            #expect(returnedRequest2 === request2)
            #expect(queue.count == 2)
            #expect(!queue.isEmpty)
        }

        // still retained by the deque inside the queue
        #expect(queue.requests.count == 2)
        #expect(queue.queue.count == 3)

        do {
            #expect(queue.count == 2)
            #expect(!queue.isEmpty)
            let removeAllResult = queue.removeAll()
            #expect(Set(removeAllResult) == [request1, request3])
            #expect(queue.isEmpty)
            #expect(queue.count == 0)
        }

        #expect(isKnownUniquelyReferenced(&request1))
        #expect(isKnownUniquelyReferenced(&request2))
        #expect(isKnownUniquelyReferenced(&request3))
    }
}
