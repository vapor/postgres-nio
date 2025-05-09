public protocol ConnectionPoolExecutor: AnyObject, Sendable {
    associatedtype ID: Hashable, Sendable

    var id: ID { get }

    static func getExecutorID() -> Self.ID?
}

public final class NothingConnectionPoolExecutor: ConnectionPoolExecutor {
    public typealias ID = ObjectIdentifier

    public var id: ObjectIdentifier { ObjectIdentifier(self) }

    public static func getExecutorID() -> ObjectIdentifier? { nil }
}
