/// A wrapper around the errors that can occur during a transaction.
public struct PostgresTransactionError: Error {

    /// The file in which the transaction was started
    public var file: String
    /// The line in which the transaction was started
    public var line: Int

    /// The error thrown when running the `BEGIN` query
    public var beginError: (any Error)?
    /// The error thrown in the transaction closure
    public var closureError: (any Error)?

    /// The error thrown while rolling the transaction back. If the ``closureError`` is set,
    /// but the ``rollbackError`` is empty, the rollback was successful. If the ``rollbackError``
    /// is set, the rollback failed.
    public var rollbackError: (any Error)?

    /// The error thrown while commiting the transaction.
    public var commitError: (any Error)?
}
