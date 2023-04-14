import NIOCore

/// A representation of a cell value within a ``PostgresRow`` and ``PostgresRandomAccessRow``.
public struct PostgresCell: Sendable, Equatable {
    /// The cell's value as raw bytes.
    public var bytes: ByteBuffer?
    /// The cell's data type. This is important metadata when decoding the cell.
    public var dataType: PostgresDataType
    /// The format in which the cell's bytes are encoded.
    public var format: PostgresFormat

    /// The cell's column name within the row.
    public var columnName: String
    /// The cell's column index within the row.
    public var columnIndex: Int

    public init(
        bytes: ByteBuffer?,
        dataType: PostgresDataType,
        format: PostgresFormat,
        columnName: String,
        columnIndex: Int
    ) {
        self.bytes = bytes
        self.dataType = dataType
        self.format = format

        self.columnName = columnName
        self.columnIndex = columnIndex
    }
}

extension PostgresCell {
    /// Decode the cell into a Swift type, that conforms to ``PostgresDecodable``
    ///
    /// - Parameters:
    ///   - _: The Swift type, which conforms to ``PostgresDecodable``, to decode from the cell's ``PostgresCell/bytes`` values.
    ///   - context: A ``PostgresDecodingContext`` to supply a custom ``PostgresJSONDecoder`` for decoding JSON fields.
    ///   - file: The source file in which this method was called. Used in the error case in ``PostgresDecodingError``.
    ///   - line: The source file line in which this method was called. Used in the error case in ``PostgresDecodingError``.
    /// - Returns: A decoded Swift type.
    @inlinable
    public func decode<T: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(
        _: T.Type,
        context: PostgresDecodingContext<JSONDecoder>,
        file: String = #file,
        line: Int = #line
    ) throws -> T {
        var copy = self.bytes
        do {
            return try T._decodeRaw(
                from: &copy,
                type: self.dataType,
                format: self.format,
                context: context
            )
        } catch let code as PostgresDecodingError.Code {
            throw PostgresDecodingError(
                code: code,
                columnName: self.columnName,
                columnIndex: self.columnIndex,
                targetType: T.self,
                postgresType: self.dataType,
                postgresFormat: self.format,
                postgresData: copy,
                file: file,
                line: line
            )
        }
    }


    /// Decode the cell into a Swift type, that conforms to ``PostgresDecodable``
    ///
    /// - Parameters:
    ///   - _: The Swift type, which conforms to ``PostgresDecodable``, to decode from the cell's ``PostgresCell/bytes`` values.
    ///   - file: The source file in which this method was called. Used in the error case in ``PostgresDecodingError``.
    ///   - line: The source file line in which this method was called. Used in the error case in ``PostgresDecodingError``.
    /// - Returns: A decoded Swift type.
    @inlinable
    public func decode<T: PostgresDecodable>(
        _: T.Type,
        file: String = #file,
        line: Int = #line
    ) throws -> T {
        try self.decode(T.self, context: .default, file: file, line: line)
    }
}
