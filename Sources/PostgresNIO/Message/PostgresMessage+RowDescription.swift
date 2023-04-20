import NIOCore

extension PostgresMessage {
    /// Identifies the message as a row description.
    public struct RowDescription {
        /// Describes a single field returns in a `RowDescription` message.
        public struct Field: CustomStringConvertible {
            static func parse(from buffer: inout ByteBuffer) throws -> Field {
                guard let name = buffer.readNullTerminatedString() else {
                    throw PostgresError.protocol("Could not read row description field name")
                }
                guard let tableOID = buffer.readInteger(as: UInt32.self) else {
                    throw PostgresError.protocol("Could not read row description field table OID")
                }
                guard let columnAttributeNumber = buffer.readInteger(as: Int16.self) else {
                    throw PostgresError.protocol("Could not read row description field column attribute number")
                }
                guard let dataType = buffer.readInteger(as: PostgresDataType.self) else {
                    throw PostgresError.protocol("Could not read row description field data type")
                }
                guard let dataTypeSize = buffer.readInteger(as: Int16.self) else {
                    throw PostgresError.protocol("Could not read row description field data type size")
                }
                guard let dataTypeModifier = buffer.readInteger(as: Int32.self) else {
                    throw PostgresError.protocol("Could not read row description field data type modifier")
                }
                guard let formatCode = buffer.readInteger(as: PostgresFormat.self) else {
                    throw PostgresError.protocol("Could not read row description field format code")
                }
                return .init(
                    name: name,
                    tableOID: tableOID,
                    columnAttributeNumber: columnAttributeNumber,
                    dataType: dataType,
                    dataTypeSize: dataTypeSize,
                    dataTypeModifier: dataTypeModifier,
                    formatCode: formatCode
                )
            }
            
            /// The field name.
            public var name: String
            
            /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
            public var tableOID: UInt32
            
            /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
            public var columnAttributeNumber: Int16
            
            /// The object ID of the field's data type.
            public var dataType: PostgresDataType
            
            /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
            public var dataTypeSize: Int16
            
            /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
            public var dataTypeModifier: Int32
            
            /// The format code being used for the field.
            /// Currently will be zero (text) or one (binary).
            /// In a RowDescription returned from the statement variant of Describe,
            /// the format code is not yet known and will always be zero.
            public var formatCode: PostgresFormat
            
            /// See `CustomStringConvertible`.
            public var description: String {
                return self.name.description + "(\(tableOID))"
            }
        }
        

        
        /// The fields supplied in the row description.
        public var fields: [Field]
        
        /// See `CustomStringConverible`.
        public var description: String {
            return "Row(\(self.fields)"
        }
    }
}

@available(*, deprecated, message: "Deprecating conformance to `PostgresMessageType` since it is deprecated.")
extension PostgresMessage.RowDescription: PostgresMessageType {
    /// See `PostgresMessageType`.
    public static var identifier: PostgresMessage.Identifier {
        return .rowDescription
    }

    /// Parses an instance of this message type from a byte buffer.
    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        guard let fields = try buffer.read(array: Field.self, { buffer in
            return try.parse(from: &buffer)
        }) else {
            throw PostgresError.protocol("Could not read row description fields")
        }
        return .init(fields: fields)
    }
}
