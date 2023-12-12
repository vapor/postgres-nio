import _ConnectionPoolModule

final class MockRequest: ConnectionRequestProtocol, Hashable, Sendable {
    typealias Connection = MockConnection

    struct ID: Hashable {
        var objectID: ObjectIdentifier

        init(_ request: MockRequest) {
            self.objectID = ObjectIdentifier(request)
        }
    }

    var id: ID { ID(self) }


    static func ==(lhs: MockRequest, rhs: MockRequest) -> Bool {
        lhs.id == rhs.id
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(self.id)
    }

    func complete(with: Result<Connection, ConnectionPoolError>) {

    }
}
