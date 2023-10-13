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
