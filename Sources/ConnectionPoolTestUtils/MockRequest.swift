import _ConnectionPoolModule

public final class MockRequest<Connection: PooledConnection>: ConnectionRequestProtocol, Hashable, Sendable {
    public struct ID: Hashable, Sendable {
        var objectID: ObjectIdentifier

        init(_ request: MockRequest) {
            self.objectID = ObjectIdentifier(request)
        }
    }

    public init(connectionType: Connection.Type = Connection.self) {}

    public var id: ID { ID(self) }

    public static func ==(lhs: MockRequest, rhs: MockRequest) -> Bool {
        lhs.id == rhs.id
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.id)
    }

    public func complete(with: Result<ConnectionLease<Connection>, ConnectionPoolError>) {

    }
}
