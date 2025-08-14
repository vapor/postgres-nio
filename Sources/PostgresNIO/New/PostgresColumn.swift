/// Information of a column.
//
// This type has the same definition as `RowDescription.column`, we need to keep 
// that type private so we defines this type.
public struct PostgresColumn: Hashable, Sendable {
    /// The column name.
    public let name: String
    
    /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
    public let tableOID: Int32
    
    /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
    public let columnAttributeNumber: Int16
    
    /// The object ID of the field's data type.
    public let dataType: PostgresDataType
    
    /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    public let dataTypeSize: Int16
    
    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    public let dataTypeModifier: Int32
    
    /// The format being used for the field. Currently will be text or binary. In a RowDescription returned
    /// from the statement variant of Describe, the format code is not yet known and will always be text.
    public let format: PostgresFormat


    internal init(
        name: String,
        tableOID: Int32,
        columnAttributeNumber: Int16,
        dataType: PostgresDataType,
        dataTypeSize: Int16,
        dataTypeModifier: Int32,
        format: PostgresFormat
    ) {
        self.name = name
        self.tableOID = tableOID
        self.columnAttributeNumber = columnAttributeNumber
        self.dataType = dataType
        self.dataTypeSize = dataTypeSize
        self.dataTypeModifier = dataTypeModifier
        self.format = format
    }
}