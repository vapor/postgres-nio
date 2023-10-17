/// A connection that can be pooled in a ``ConnectionPool``
public protocol PooledConnection: AnyObject, Sendable {
    /// The connections identifier type.
    associatedtype ID: Hashable & Sendable

    /// The connections identifier. The identifier is passed to
    /// the connection factory method and must stay attached to
    /// the connection at all times. It must not change during
    /// the connections lifetime.
    var id: ID { get }

    /// A method to register closures that are invoked when the
    /// connection is closed. If the connection closed unexpectedly
    /// the closure shall be called with the underlying error.
    /// In most NIO clients this can be easily implemented by
    /// attaching to the `channel.closeFuture`:
    /// ```
    ///   func onClose(
    ///     _ closure: @escaping @Sendable ((any Error)?) -> ()
    ///   ) {
    ///     channel.closeFuture.whenComplete { _ in
    ///       closure(previousError)
    ///     }
    ///   }
    /// ```
    func onClose(_ closure: @escaping @Sendable ((any Error)?) -> ())

    /// Close the running connection. Once the close has completed
    /// closures that were registered in `onClose` must be
    /// invoked.
    func close()
}

/// A connection id generator. Its returned connection IDs will
/// be used when creating new ``PooledConnection``s
public protocol ConnectionIDGeneratorProtocol: Sendable {
    /// The connections identifier type.
    associatedtype ID: Hashable & Sendable

    /// The next connection ID that shall be used.
    func next() -> ID
}

/// A keep alive behavior for connections maintained by the pool
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public protocol ConnectionKeepAliveBehavior: Sendable {
    /// the connection type
    associatedtype Connection: PooledConnection

    /// The time after which a keep-alive shall
    /// be triggered.
    /// If nil is returned, keep-alive is deactivated
    var keepAliveFrequency: Duration? { get }

    /// This method is invoked when the keep-alive shall be
    /// run.
    func runKeepAlive(for connection: Connection) async throws
}

/// A request to get a connection from the `ConnectionPool`
public protocol ConnectionRequestProtocol: Sendable {
    /// A connection lease request ID type.
    associatedtype ID: Hashable & Sendable
    /// The leased connection type
    associatedtype Connection: PooledConnection

    /// A connection lease request ID. This ID must be generated
    /// by users of the `ConnectionPool` outside the
    /// `ConnectionPool`. It is not generated inside the pool like
    /// the `ConnectionID`s. The lease request ID must be unique
    /// and must not change, if your implementing type is a
    /// reference type.
    var id: ID { get }

    /// A function that is called with a connection or a
    /// `PoolError`.
    func complete(with: Result<Connection, ConnectionPoolError>)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct ConnectionPoolConfiguration {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the remote servers closes
    /// idle connections,
    /// the `ConnectionPool` will initiate new outbound
    /// connections proactively to avoid the number of available
    /// connections dropping below this number.
    public var minimumConnectionCount: Int

    /// Between the `minimumConnectionCount` and
    /// `maximumConnectionSoftLimit` the connection pool creates
    /// _preserved_ connections. Preserved connections are closed
    /// if they have been idle for ``idleTimeout``.
    public var maximumConnectionSoftLimit: Int

    /// The maximum number of connections for this pool, that can
    /// exist at any point in time. The pool can create _overflow_
    /// connections, if all connections are leased, and the
    /// `maximumConnectionHardLimit` > `maximumConnectionSoftLimit `
    /// Overflow connections are closed immediately as soon as they
    /// become idle.
    public var maximumConnectionHardLimit: Int

    /// The time that a _preserved_ idle connection stays in the
    /// pool before it is closed.
    public var idleTimeout: Duration

    /// initializer
    public init() {
        self.minimumConnectionCount = 0
        self.maximumConnectionSoftLimit = 16
        self.maximumConnectionHardLimit = 16
        self.idleTimeout = .seconds(60)
    }
}
