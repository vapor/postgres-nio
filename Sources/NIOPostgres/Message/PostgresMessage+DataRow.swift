import NIO

extension PostgresMessage {
    /// Identifies the message as a data row.
    public struct DataRow: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .dataRow
        }
        
        public struct Column: CustomStringConvertible {
            /// The length of the column value, in bytes (this count does not include itself).
            /// Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
            
            /// The value of the column, in the format indicated by the associated format code. n is the above length.
            public var value: ByteBuffer?
            
            /// See `CustomStringConvertible`.
            public var description: String {
                if let value = value {
                    return "0x" + value.readableBytesView.hexdigest()
                } else {
                    return "<null>"
                }
            }
        }
        
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> DataRow {
            #warning("look into lazy parsing")
            guard let columns = buffer.read(array: Column.self, { buffer in
                return .init(value: buffer.readNullableBytes())
            }) else {
                throw PostgresError(.protocol("Could not parse data row columns"))
            }
            return .init(columns: columns)
        }
        
        /// The data row's columns
        public var columns: [Column]
        
        /// See `CustomStringConvertible`.
        public var description: String {
            return "Columns(" + columns.map { $0.description }.joined(separator: ", ") + ")"
        }
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            fatalError()
        }
    }
}
