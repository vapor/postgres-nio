import _ConnectionPoolModule

public final class MockRequest: ConnectionRequestProtocol, Hashable, Sendable {
    public typealias Connection = MockConnection

    public struct ID: Hashable, Sendable {
        var objectID: ObjectIdentifier

        init(_ request: MockRequest) {
            self.objectID = ObjectIdentifier(request)
        }
    }

    public init() {}

    public var id: ID { ID(self) }

    public static func ==(lhs: MockRequest, rhs: MockRequest) -> Bool {
        lhs.id == rhs.id
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.id)
    }

    public func complete(with: Result<Connection, ConnectionPoolError>) {

    }
}
