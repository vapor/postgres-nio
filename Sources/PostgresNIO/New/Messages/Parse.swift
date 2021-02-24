extension PSQLFrontendMessage {
    
    struct Parse: PayloadEncodable, Equatable {
        /// The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
        let preparedStatementName: String
        
        /// The query string to be parsed.
        let query: String
        
        /// The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.
        let parameters: [PSQLDataType]
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(self.preparedStatementName)
            buffer.writeNullTerminatedString(self.query)
            buffer.writeInteger(Int16(self.parameters.count))
            
            self.parameters.forEach { dataType in
                buffer.writeInteger(dataType.rawValue)
            }
        }
    }
    
}
