import NIOCore

struct PSQLCell: Equatable {
    
    var bytes: ByteBuffer?
    var columnIndex: Int
    var columnDescription: RowDescription.Column
    
    /// use this only for testing
    init(bytes: ByteBuffer?, columnIndex: Int, columnDescription: RowDescription.Column) {
        self.bytes = bytes
        self.columnIndex = columnIndex
        self.columnDescription = columnDescription
    }
}

extension PSQLCell {
    func decode<T: PSQLDecodable, JSONDecoder: PostgresJSONDecoder>(_: T.Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: UInt = #line) throws -> T {
        var cellData = self.bytes

        do {
            return try T.decodeRaw(
                from: &cellData,
                type: self.columnDescription.dataType,
                format: self.columnDescription.format,
                context: context
            )
        } catch let code as PostgresCastingError.Code {
            throw PostgresCastingError(
                code: code,
                columnName: self.columnDescription.name,
                columnIndex: self.columnIndex,
                targetType: T.self,
                postgresType: self.columnDescription.dataType,
                postgresData: cellData,
                file: file,
                line: line
            )
        }
    }
}
