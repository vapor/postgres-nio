/// A `Sequence` that does not heap allocate, if it only carries a single element
@usableFromInline
struct TinyFastSequence<Element>: Sequence {
    @usableFromInline
    enum Base {
        case none(reserveCapacity: Int)
        case one(Element, reserveCapacity: Int)
        case two(Element, Element, reserveCapacity: Int)
        case n([Element])
    }

    @usableFromInline
    private(set) var base: Base

    @inlinable
    init() {
        self.base = .none(reserveCapacity: 0)
    }

    @inlinable
    init(element: Element) {
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

    @inlinable
    init(_ max2Sequence: Max2Sequence<Element>) {
        switch max2Sequence.count {
        case 0:
            self.base = .none(reserveCapacity: 0)
        case 1:
            self.base = .one(max2Sequence.first!, reserveCapacity: 0)
        case 2:
            self.base = .n(Array(max2Sequence))
        default:
            fatalError()
        }
    }

    @usableFromInline
    var count: Int {
        switch self.base {
        case .none:
            return 0
        case .one:
            return 1
        case .two:
            return 2
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
        case .two(let first, _, _):
            return first
        case .n(let array):
            return array.first
        }
    }

    @usableFromInline
    var isEmpty: Bool {
        switch self.base {
        case .none:
            return true
        case .one, .two, .n:
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
        case .two(let first, let second, let reservedCapacity):
            self.base = .two(first, second, reserveCapacity: Swift.max(reservedCapacity, minimumCapacity))
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
        case .one(let first, let reserveCapacity):
            self.base = .two(first, element, reserveCapacity: reserveCapacity)

        case .two(let first, let second, let reserveCapacity):
            var new = [Element]()
            new.reserveCapacity(Swift.max(4, reserveCapacity))
            new.append(first)
            new.append(second)
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
        @usableFromInline private(set) var backing: TinyFastSequence<Element>

        @inlinable
        init(_ backing: TinyFastSequence<Element>) {
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

            case .two(let first, let second, _):
                defer { self.index += 1 }
                switch self.index {
                case 0:
                    return first
                case 1:
                    return second
                default:
                    return nil
                }

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

extension TinyFastSequence: Equatable where Element: Equatable {}
extension TinyFastSequence.Base: Equatable where Element: Equatable {}

extension TinyFastSequence: Hashable where Element: Hashable {}
extension TinyFastSequence.Base: Hashable where Element: Hashable {}

extension TinyFastSequence: Sendable where Element: Sendable {}
extension TinyFastSequence.Base: Sendable where Element: Sendable {}

extension TinyFastSequence: ExpressibleByArrayLiteral {
    @inlinable
    init(arrayLiteral elements: Element...) {
        var iterator = elements.makeIterator()
        switch elements.count {
        case 0:
            self.base = .none(reserveCapacity: 0)
        case 1:
            self.base = .one(iterator.next()!, reserveCapacity: 0)
        case 2:
            self.base = .two(iterator.next()!, iterator.next()!, reserveCapacity: 0)
        default:
            self.base = .n(elements)
        }
    }
}
