struct PSQLPreparedStatement {
    
    /// The name with which the statement was prepared at the backend
    let name: String
    
    /// The query that is executed when using this `PSQLPreparedStatement`
    let query: String
    
    /// The postgres connection the statement was prepared on
    let connection: PSQLConnection
    
    /// The `RowDescription` to apply to all `DataRow`s when executing this `PSQLPreparedStatement`
    let rowDescription: PSQLBackendMessage.RowDescription?
}
