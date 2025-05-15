import Atomics

public struct ConnectionIDGenerator: ConnectionIDGeneratorProtocol {
    public static let globalGenerator = ConnectionIDGenerator()

    private let atomic: ManagedAtomic<Int>

    public init() {
        self.atomic = .init(0)
    }

    public func next() -> Int {
        return self.atomic.loadThenWrappingIncrement(ordering: .relaxed)
    }
}
