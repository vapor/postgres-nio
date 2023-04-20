import NIOCore

extension PostgresMessage {
    /// Identifies the message as a data row.
    public struct DataRow {
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

        /// The data row's columns
        public var columns: [Column]
        
        /// See `CustomStringConvertible`.
        public var description: String {
            return "Columns(" + columns.map { $0.description }.joined(separator: ", ") + ")"
        }
    }
}

@available(*, deprecated, message: "Deprecating conformance to `PostgresMessageType` since it is deprecated.")
extension PostgresMessage.DataRow: PostgresMessageType {
    public static var identifier: PostgresMessage.Identifier {
        return .dataRow
    }

    /// Parses an instance of this message type from a byte buffer.
    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        guard let columns = buffer.read(array: Column.self, { buffer in
            if var slice = buffer.readNullableBytes() {
                var copy = ByteBufferAllocator().buffer(capacity: slice.readableBytes)
                copy.writeBuffer(&slice)
                return .init(value: copy)
            } else {
                return .init(value: nil)
            }
        }) else {
            throw PostgresError.protocol("Could not parse data row columns")
        }
        return .init(columns: columns)
    }
}
