import NIOCore

/// `PSQLRow` represents a single row that was received from the Postgres Server.
public struct PSQLRow {
    @usableFromInline
    internal let lookupTable: [String: Int]
    @usableFromInline
    internal let data: DataRow

    @usableFromInline
    internal let columns: [RowDescription.Column]
    
    internal init(data: DataRow, lookupTable: [String: Int], columns: [RowDescription.Column]) {
        self.data = data
        self.lookupTable = lookupTable
        self.columns = columns
    }
}

extension PSQLRow: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        lhs.data == rhs.data && lhs.columns == rhs.columns
    }
}

#if false
extension PSQLRow {
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column name to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PSQLDecodable>(column: String, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        guard let index = self.lookupTable[column] else {
            preconditionFailure("A column '\(column)' does not exist.")
        }
        
        return try self.decode(column: index, as: type, file: file, line: line)
    }
    
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column index to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PSQLDecodable>(column index: Int, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        precondition(index < self.data.columnCount)
        
        let column = self.columns[index]
        let context = PSQLDecodingContext(jsonDecoder: self.jsonDecoder)
        
        guard var cellSlice = self.data[column: index] else {
            throw PSQLCastingError.Code.missingData
        }

        return try T.decode(from: &cellSlice, type: column.dataType, format: column.format, context: context)
    }
}
#endif
