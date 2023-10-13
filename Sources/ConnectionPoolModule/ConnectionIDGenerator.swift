import Atomics

public struct ConnectionIDGenerator: ConnectionIDGeneratorProtocol {
    static let globalGenerator = ConnectionIDGenerator()

    private let atomic: ManagedAtomic<Int>

    public init() {
        self.atomic = .init(0)
    }

    public func next() -> Int {
        return self.atomic.loadThenWrappingIncrement(ordering: .relaxed)
    }
}
