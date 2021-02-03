//
//  File.swift
//  
//
//  Created by Fabian Fett on 25.01.21.
//

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
