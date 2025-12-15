public struct MockError: Error, Hashable, Sendable {

    public var id: Int

    public init(id: Int = 0) {
        self.id = id
    }
}
