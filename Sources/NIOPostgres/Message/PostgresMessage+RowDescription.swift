import NIO

extension PostgresMessage {
    /// Identifies the message as a row description.
    struct RowDescription {
        /// Describes a single field returns in a `RowDescription` message.
        struct Field {
            /// The field name.
            var name: String
            
            /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
            var tableOID: UInt32
            
            /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
            var columnAttributeNumber: Int16
            
            /// The object ID of the field's data type.
            var dataType: PostgresDataType
            
            /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
            var dataTypeSize: Int16
            
            /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
            var dataTypeModifier: Int32
            
            /// The format code being used for the field.
            /// Currently will be zero (text) or one (binary).
            /// In a RowDescription returned from the statement variant of Describe,
            /// the format code is not yet known and will always be zero.
            var formatCode: PostgresFormatCode
        }
        
        /// Parses an instance of this message type from a byte buffer.
        static func parse(from buffer: inout ByteBuffer) throws -> RowDescription {
            guard let fields = try buffer.readArray(Field.self, { buffer in
                guard let name = buffer.readNullTerminatedString() else {
                    throw PostgresError(.protocol("Could not read row description field name"))
                }
                guard let tableOID = buffer.readInteger(as: UInt32.self) else {
                    throw PostgresError(.protocol("Could not read row description field table OID"))
                }
                guard let columnAttributeNumber = buffer.readInteger(as: Int16.self) else {
                    throw PostgresError(.protocol("Could not read row description field column attribute number"))
                }
                guard let dataType = buffer.readInteger(as: Int32.self).flatMap(PostgresDataType.init(_:)) else {
                    throw PostgresError(.protocol("Could not read row description field data type"))
                }
                guard let dataTypeSize = buffer.readInteger(as: Int16.self) else {
                    throw PostgresError(.protocol("Could not read row description field data type size"))
                }
                guard let dataTypeModifier = buffer.readInteger(as: Int32.self) else {
                    throw PostgresError(.protocol("Could not read row description field data type modifier"))
                }
                guard let formatCode = buffer.readInteger(rawRepresentable: PostgresFormatCode.self) else {
                    throw PostgresError(.protocol("Could not read row description field format code"))
                }
                return .init(name: name, tableOID: tableOID, columnAttributeNumber: columnAttributeNumber, dataType: dataType, dataTypeSize: dataTypeSize, dataTypeModifier: dataTypeModifier, formatCode: formatCode)
            }) else {
                throw PostgresError(.protocol("Could not read row description fields"))
            }
            return .init(fields: fields)
        }
        
        /// The fields supplied in the row description.
        var fields: [Field]
    }
}
