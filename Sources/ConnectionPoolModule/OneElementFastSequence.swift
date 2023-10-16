/// A `Sequence` that does not heap allocate, if it only carries a single element
@usableFromInline
struct OneElementFastSequence<Element>: Sequence {
    @usableFromInline
    enum Base {
        case none(reserveCapacity: Int)
        case one(Element, reserveCapacity: Int)
        case n([Element])
    }

    @usableFromInline
    private(set) var base: Base

    @inlinable
    init() {
        self.base = .none(reserveCapacity: 0)
    }

    @inlinable
    init(_ element: Element) {
        self.base = .one(element, reserveCapacity: 1)
    }

    @inlinable
    init(_ collection: some Collection<Element>) {
        switch collection.count {
        case 0:
            self.base = .none(reserveCapacity: 0)
        case 1:
            self.base = .one(collection.first!, reserveCapacity: 0)
        default:
            if let collection = collection as? Array<Element> {
                self.base = .n(collection)
            } else {
                self.base = .n(Array(collection))
            }
        }
    }

    @usableFromInline
    var count: Int {
        switch self.base {
        case .none:
            return 0
        case .one:
            return 1
        case .n(let array):
            return array.count
        }
    }

    @inlinable
    var first: Element? {
        switch self.base {
        case .none:
            return nil
        case .one(let element, _):
            return element
        case .n(let array):
            return array.first
        }
    }

    @usableFromInline
    var isEmpty: Bool {
        switch self.base {
        case .none:
            return true
        case .one, .n:
            return false
        }
    }

    @inlinable
    mutating func reserveCapacity(_ minimumCapacity: Int) {
        switch self.base {
        case .none(let reservedCapacity):
            self.base = .none(reserveCapacity: Swift.max(reservedCapacity, minimumCapacity))
        case .one(let element, let reservedCapacity):
            self.base = .one(element, reserveCapacity: Swift.max(reservedCapacity, minimumCapacity))
        case .n(var array):
            self.base = .none(reserveCapacity: 0) // prevent CoW
            array.reserveCapacity(minimumCapacity)
            self.base = .n(array)
        }
    }

    @inlinable
    mutating func append(_ element: Element) {
        switch self.base {
        case .none(let reserveCapacity):
            self.base = .one(element, reserveCapacity: reserveCapacity)
        case .one(let existing, let reserveCapacity):
            var new = [Element]()
            new.reserveCapacity(reserveCapacity)
            new.append(existing)
            new.append(element)
            self.base = .n(new)
        case .n(var existing):
            self.base = .none(reserveCapacity: 0) // prevent CoW
            existing.append(element)
            self.base = .n(existing)
        }
    }

    @inlinable
    func makeIterator() -> Iterator {
        Iterator(self)
    }

    @usableFromInline
    struct Iterator: IteratorProtocol {
        @usableFromInline private(set) var index: Int = 0
        @usableFromInline private(set) var backing: OneElementFastSequence<Element>

        @inlinable
        init(_ backing: OneElementFastSequence<Element>) {
            self.backing = backing
        }

        @inlinable
        mutating func next() -> Element? {
            switch self.backing.base {
            case .none:
                return nil
            case .one(let element, _):
                if self.index == 0 {
                    self.index += 1
                    return element
                }
                return nil

            case .n(let array):
                if self.index < array.endIndex {
                    defer { self.index += 1}
                    return array[self.index]
                }
                return nil
            }
        }
    }
}

extension OneElementFastSequence: Equatable where Element: Equatable {}
extension OneElementFastSequence.Base: Equatable where Element: Equatable {}

extension OneElementFastSequence: Hashable where Element: Hashable {}
extension OneElementFastSequence.Base: Hashable where Element: Hashable {}

extension OneElementFastSequence: Sendable where Element: Sendable {}
extension OneElementFastSequence.Base: Sendable where Element: Sendable {}
