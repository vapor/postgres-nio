import NIOCore

extension PostgresMessage {
    /// Identifies the message as a Bind command.
    @available(*, deprecated, message: "Will be removed from public API")
    public struct Bind: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .bind
        }
        
        public var description: String {
            return "Bind(\(self.parameters.count))"
        }
        
        public struct Parameter {
            /// The value of the parameter, in the format indicated by the associated format code. n is the above length.
            var value: ByteBuffer?
        }

        /// The name of the destination portal (an empty string selects the unnamed portal).
        public var portalName: String
        
        /// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        public var statementName: String
        
        /// The number of parameter format codes that follow (denoted C below).
        /// This can be zero to indicate that there are no parameters or that the parameters all use the default format (text);
        /// or one, in which case the specified format code is applied to all parameters; or it can equal the actual number of parameters.
        /// The parameter format codes. Each must presently be zero (text) or one (binary).
        public var parameterFormatCodes: [PostgresFormat]
        
        /// The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.
        public var parameters: [Parameter]
        
        /// The number of result-column format codes that follow (denoted R below).
        /// This can be zero to indicate that there are no result columns or that the result columns should all use the default format (text);
        /// or one, in which case the specified format code is applied to all result columns (if any);
        /// or it can equal the actual number of result columns of the query.
        public var resultFormatCodes: [PostgresFormat]
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(self.portalName)
            buffer.writeNullTerminatedString(self.statementName)
            
            buffer.write(array: self.parameterFormatCodes)
            buffer.write(array: self.parameters) {
                if var data = $1.value {
                    // The length of the parameter value, in bytes (this count does not include itself). Can be zero.
                    $0.writeInteger(numericCast(data.readableBytes), as: Int32.self)
                    // The value of the parameter, in the format indicated by the associated format code. n is the above length.
                    $0.writeBuffer(&data)
                } else {
                    // As a special case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.
                    $0.writeInteger(-1, as: Int32.self)
                }
            }
            buffer.write(array: self.resultFormatCodes)
        }
    }
}
