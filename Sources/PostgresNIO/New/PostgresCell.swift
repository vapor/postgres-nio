#if swift(>=5.6)
@preconcurrency import NIOCore
#else
import NIOCore
#endif

public struct PostgresCell: Equatable {
    public var bytes: ByteBuffer?
    public var dataType: PostgresDataType
    public var format: PostgresFormat

    public var columnName: String
    public var columnIndex: Int

    init(bytes: ByteBuffer?, dataType: PostgresDataType, format: PostgresFormat, columnName: String, columnIndex: Int) {
        self.bytes = bytes
        self.dataType = dataType
        self.format = format

        self.columnName = columnName
        self.columnIndex = columnIndex
    }
}

extension PostgresCell {

    func decode<T: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(
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
        } catch let code as PostgresCastingError.Code {
            throw PostgresCastingError(
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
}

#if swift(>=5.6)
extension PostgresCell: Sendable {}
#endif
