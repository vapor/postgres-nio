/// A prepared statement.
///
/// Structs conforming to this protocol will need to provide the SQL statement to
/// send to the server and a way of creating bindings are decoding the result.
///
/// As an example, consider this struct:
/// ```swift
/// struct Example: PostgresPreparedStatement {
///     static var sql = "SELECT pid, datname FROM pg_stat_activity WHERE state = $1"
///     typealias Row = (Int, String)
///
///     var state: String
///
///     func makeBindings() -> PostgresBindings {
///         var bindings = PostgresBindings()
///         bindings.append(self.state)
///         return bindings
///     }
///
///     func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
///         try row.decode(Row.self)
///     }
/// }
/// ```
///
/// Structs conforming to this protocol can then be used with `PostgresConnection.execute(_ preparedStatement:, logger:)`,
/// which will take care of preparing the statement on the server side and executing it.
public protocol PostgresPreparedStatement: Sendable {
    /// The type rows returned by the statement will be decoded into
    associatedtype Row

    /// The SQL statement to prepare on the database server.
    static var sql: String { get }

    /// Make the bindings to provided concrete values to use when executing the prepared SQL statement
    func makeBindings() throws -> PostgresBindings
    
    /// Decode a row returned by the database into an instance of `Row`
    func decodeRow(_ row: PostgresRow) throws -> Row
}
