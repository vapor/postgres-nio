import NIO

extension PostgresMessage {
    /// Identifies the message as a data row.
    struct DataRow: CustomStringConvertible {
        struct Column: CustomStringConvertible {
            /// The length of the column value, in bytes (this count does not include itself).
            /// Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
            
            /// The value of the column, in the format indicated by the associated format code. n is the above length.
            var value: [UInt8]?
            
            /// See `CustomStringConvertible`.
            var description: String {
                if let value = value {
                    return "0x" + value.hexdigest()
                } else {
                    return "<null>"
                }
            }
        }
        
        /// Parses an instance of this message type from a byte buffer.
        static func parse(from buffer: inout ByteBuffer) throws -> DataRow {
            guard let columns = buffer.read(array: Column.self, { buffer in
                return .init(value: buffer.readNullableBytes())
            }) else {
                throw PostgresError(.protocol("Could not parse data row columns"))
            }
            return .init(columns: columns)
        }
        
        /// The data row's columns
        var columns: [Column]
        
        /// See `CustomStringConvertible`.
        var description: String {
            return "Columns(" + columns.map { $0.description }.joined(separator: ", ") + ")"
        }
    }
}
