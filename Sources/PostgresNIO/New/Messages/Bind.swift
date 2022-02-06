import NIOCore

extension PSQLFrontendMessage {
    
    struct Bind: PSQLMessagePayloadEncodable, Equatable {
        /// The name of the destination portal (an empty string selects the unnamed portal).
        var portalName: String
        
        /// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        var preparedStatementName: String

        /// The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.
        var bind: PSQLBindings
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(self.portalName)
            buffer.writeNullTerminatedString(self.preparedStatementName)
            
            // The number of parameter format codes that follow (denoted C below). This can be
            // zero to indicate that there are no parameters or that the parameters all use the
            // default format (text); or one, in which case the specified format code is applied
            // to all parameters; or it can equal the actual number of parameters.
            buffer.writeInteger(Int16(self.bind.count))
            
            // The parameter format codes. Each must presently be zero (text) or one (binary).
            self.bind.metadata.forEach {
                buffer.writeInteger($0.format.rawValue)
            }
            
            buffer.writeInteger(Int16(self.bind.count))

            var parametersCopy = self.bind.bytes
            buffer.writeBuffer(&parametersCopy)

            // The number of result-column format codes that follow (denoted R below). This can be
            // zero to indicate that there are no result columns or that the result columns should
            // all use the default format (text); or one, in which case the specified format code
            // is applied to all result columns (if any); or it can equal the actual number of
            // result columns of the query.
            buffer.writeInteger(1, as: Int16.self)
            // The result-column format codes. Each must presently be zero (text) or one (binary).
            buffer.writeInteger(PSQLFormat.binary.rawValue, as: Int16.self)
        }
    }
}
