import DequeModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine {

    /// A request queue, which can enqueue requests in O(1), dequeue requests in O(1) and even cancel requests in O(1).
    ///
    /// While enqueueing and dequeueing on O(1) is trivial, cancellation is hard, as it normally requires a removal within the
    /// underlying Deque. However thanks to having an additional `requests` dictionary, we can remove the cancelled
    /// request from the dictionary and keep it inside the queue. Whenever we pop a request from the deque, we validate
    /// that it hasn't been cancelled in the meantime by checking if the popped request is still in the `requests` dictionary.
    @usableFromInline
    struct RequestQueue {
        @usableFromInline
        private(set) var queue: Deque<RequestID>

        @usableFromInline
        private(set) var requests: [RequestID: Request]

        @inlinable
        var count: Int {
            self.requests.count
        }

        @inlinable
        var isEmpty: Bool {
            self.count == 0
        }

        @usableFromInline
        init() {
            self.queue = .init(minimumCapacity: 256)
            self.requests = .init(minimumCapacity: 256)
        }

        @inlinable
        mutating func queue(_ request: Request) {
            self.requests[request.id] = request
            self.queue.append(request.id)
        }

        @inlinable
        mutating func pop(max: UInt16) -> OneElementFastSequence<Request> {
            var result = OneElementFastSequence<Request>()
            result.reserveCapacity(Int(max))
            var popped = 0
            while let requestID = self.queue.popFirst(), popped < max {
                if let requestIndex = self.requests.index(forKey: requestID) {
                    popped += 1
                    result.append(self.requests.remove(at: requestIndex).value)
                }
            }

            assert(result.count <= max)
            return result
        }

        @inlinable
        mutating func remove(_ requestID: RequestID) -> Request? {
            self.requests.removeValue(forKey: requestID)
        }

        @inlinable
        mutating func removeAll() -> OneElementFastSequence<Request> {
            let result = OneElementFastSequence(self.requests.values)
            self.requests.removeAll()
            self.queue.removeAll()
            return result
        }
    }
}
