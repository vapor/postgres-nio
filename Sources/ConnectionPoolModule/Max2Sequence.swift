// A `Sequence` that can contain at most two elements. However it does not heap allocate.
@usableFromInline
struct Max2Sequence<Element>: Sequence {
    @usableFromInline
    private(set) var first: Element?
    @usableFromInline
    private(set) var second: Element?

    @inlinable
    var count: Int {
        if self.first == nil { return 0 }
        if self.second == nil { return 1 }
        return 2
    }

    @inlinable
    var isEmpty: Bool {
        self.first == nil
    }

    @inlinable
    init(_ first: Element?, _ second: Element? = nil) {
        if let first = first {
            self.first = first
            self.second = second
        } else {
            self.first = second
            self.second = nil
        }
    }

    @inlinable
    init() {
        self.first = nil
        self.second = nil
    }

    @inlinable
    func makeIterator() -> Iterator {
        Iterator(first: self.first, second: self.second)
    }

    @usableFromInline
    struct Iterator: IteratorProtocol {
        @usableFromInline
        let first: Element?
        @usableFromInline
        let second: Element?

        @usableFromInline
        private(set) var index: UInt8 = 0

        @inlinable
        init(first: Element?, second: Element?) {
            self.first = first
            self.second = second
            self.index = 0
        }

        @inlinable
        mutating func next() -> Element? {
            switch self.index {
            case 0:
                self.index += 1
                return self.first
            case 1:
                self.index += 1
                return self.second
            default:
                return nil
            }
        }
    }

    @inlinable
    mutating func append(_ element: Element) {
        precondition(self.second == nil)
        if self.first == nil {
            self.first = element
        } else if self.second == nil {
            self.second = element
        } else {
            fatalError("Max2Sequence can only hold two Elements.")
        }
    }

    @inlinable
    func map<NewElement>(_ transform: (Element) throws -> (NewElement)) rethrows -> Max2Sequence<NewElement> {
        try Max2Sequence<NewElement>(self.first.flatMap(transform), self.second.flatMap(transform))
    }
}

extension Max2Sequence: ExpressibleByArrayLiteral {
    @inlinable
    init(arrayLiteral elements: Element...) {
        precondition(elements.count <= 2)
        var iterator = elements.makeIterator()
        self.first = iterator.next()
        self.second = iterator.next()
    }
}

extension Max2Sequence: Equatable where Element: Equatable {}
extension Max2Sequence: Hashable where Element: Hashable {}
extension Max2Sequence: Sendable where Element: Sendable {}
