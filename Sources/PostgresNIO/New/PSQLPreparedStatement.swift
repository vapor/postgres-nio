struct PSQLPreparedStatement {
    
    ///
    let name: String
    
    ///
    let query: String
    
    /// The postgres connection the statement was prepared on
    let connection: PSQLConnection
    
    /// 
    let rowDescription: PSQLBackendMessage.RowDescription?
}
