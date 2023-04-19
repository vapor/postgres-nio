import NIOCore

/// A backend row description message.
///
/// - NOTE: This struct is not part of the ``PSQLBackendMessage`` namespace even
///         though this is where it actually belongs. The reason for this is, that we want
///         this type to be @usableFromInline. If a type is made @usableFromInline in an
///         enclosing type, the enclosing type must be @usableFromInline as well.
///         Not putting `DataRow` in ``PSQLBackendMessage`` is our way to trick
///         the Swift compiler.
@usableFromInline
struct RowDescription: PostgresBackendMessage.PayloadDecodable, Sendable, Equatable {
    /// Specifies the object ID of the parameter data type.
    @usableFromInline
    var columns: [Column]

    @usableFromInline
    struct Column: Equatable, Sendable {
        /// The field name.
        @usableFromInline
        var name: String
        
        /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
        @usableFromInline
        var tableOID: Int32
        
        /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
        @usableFromInline
        var columnAttributeNumber: Int16
        
        /// The object ID of the field's data type.
        @usableFromInline
        var dataType: PostgresDataType
        
        /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
        @usableFromInline
        var dataTypeSize: Int16
        
        /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
        @usableFromInline
        var dataTypeModifier: Int32
        
        /// The format being used for the field. Currently will be text or binary. In a RowDescription returned
        /// from the statement variant of Describe, the format code is not yet known and will always be text.
        @usableFromInline
        var format: PostgresFormat
    }
    
    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        let columnCount = try buffer.throwingReadInteger(as: Int16.self)
        
        guard columnCount >= 0 else {
            throw PSQLPartialDecodingError.integerMustBePositiveOrNull(columnCount)
        }
        
        var result = [Column]()
        result.reserveCapacity(Int(columnCount))
        
        for _ in 0..<columnCount {
            guard let name = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            
            let hextuple = buffer.readMultipleIntegers(endianness: .big, as: (Int32, Int16, UInt32, Int16, Int32, Int16).self)
            
            guard let (tableOID, columnAttributeNumber, dataType, dataTypeSize, dataTypeModifier, formatCodeInt16) = hextuple else {
                throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(18, actual: buffer.readableBytes)
            }
            
            guard let format = PostgresFormat(rawValue: formatCodeInt16) else {
                throw PSQLPartialDecodingError.valueNotRawRepresentable(value: formatCodeInt16, asType: PostgresFormat.self)
            }
            
            let field = Column(
                name: name,
                tableOID: tableOID,
                columnAttributeNumber: columnAttributeNumber,
                dataType: PostgresDataType(dataType),
                dataTypeSize: dataTypeSize,
                dataTypeModifier: dataTypeModifier,
                format: format)
            
            result.append(field)
        }
        
        return RowDescription(columns: result)
    }
}
