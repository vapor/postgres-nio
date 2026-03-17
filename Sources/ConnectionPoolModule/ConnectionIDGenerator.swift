import Atomics

@usableFromInline
package typealias ConnectionID = Int

@usableFromInline
package struct ConnectionIDGenerator: Sendable {
    @usableFromInline
    package static let globalGenerator = ConnectionIDGenerator()

    @usableFromInline
    /* private */ let atomic: ManagedAtomic<Int>

    @usableFromInline
    init() {
        self.atomic = .init(0)
    }

    @inlinable
    package func next() -> ConnectionID {
        return self.atomic.loadThenWrappingIncrement(ordering: .relaxed)
    }
}
