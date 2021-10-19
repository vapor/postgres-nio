import NIOCore

/// `PSQLRow` represents a single row that was received from the Postgres Server.
public struct PSQLRow {
    @usableFromInline
    internal let lookupTable: [String: Int]
    @usableFromInline
    internal let data: DataRow
    
    @usableFromInline
    internal let columns: [RowDescription.Column]
    @usableFromInline
    internal let jsonDecoder: PSQLJSONDecoder
    
    internal init(data: DataRow, lookupTable: [String: Int], columns: [RowDescription.Column], jsonDecoder: PSQLJSONDecoder) {
        self.data = data
        self.lookupTable = lookupTable
        self.columns = columns
        self.jsonDecoder = jsonDecoder
    }
}

extension PSQLRow {
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column name to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    @inlinable
    public func decode<T: PSQLDecodable>(column: String, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
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
    @inlinable
    public func decode<T: PSQLDecodable>(column index: Int, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        precondition(index < self.data.columnCount)
        
        let column = self.columns[index]
        let context = PSQLDecodingContext(
            jsonDecoder: self.jsonDecoder,
            columnName: column.name,
            columnIndex: index,
            file: file,
            line: line)
        
        guard var cellSlice = self.data[column: index] else {
            throw PSQLCastingError.missingData(targetType: T.self, type: column.dataType, context: context)
        }

        return try T.decode(from: &cellSlice, type: column.dataType, format: column.format, context: context)
    }
    
    @inlinable
    public func decode<T: PSQLDecodable>(column index: Int, as type: Optional<T>.Type, file: String = #file, line: Int = #line) throws -> Optional<T> {
        precondition(index < self.data.columnCount)
        
        guard var cellSlice = self.data[column: index] else {
            return nil
        }
        
        let column = self.columns[index]
        let context = PSQLDecodingContext(
            jsonDecoder: self.jsonDecoder,
            columnName: column.name,
            columnIndex: index,
            file: file,
            line: line)

        return try T.decode(from: &cellSlice, type: column.dataType, format: column.format, context: context)
    }
}

extension PSQLRow {

    @inlinable
    public func decode<T0>(_ t0: T0.Type, file: String = #file, line: Int = #line) throws -> T0
        where T0: PSQLDecodable
    {
        var buffer = self.data.bytes
        
        return try (
            self.decodeNextColumn(t0, from: &buffer, index: 0, file: file, line: line)
        )
    }
    
    @inlinable
    public func decode<T0, T1>(_ t0: T0.Type, _ t1: T1.Type, file: String = #file, line: Int = #line) throws -> (T0, T1)
        where T0: PSQLDecodable, T1: PSQLDecodable
    {
        assert(self.columns.count >= 2)
        var buffer = self.data.bytes
        
        return try (
            self.decodeNextColumn(t0, from: &buffer, index: 0, file: file, line: line),
            self.decodeNextColumn(t1, from: &buffer, index: 1, file: file, line: line)
        )
    }
    
    @inlinable
    public func decode<T0, T1, T2>(_ t0: T0.Type, _ t1: T1.Type, _ t2: T2.Type, file: String = #file, line: Int = #line) throws -> (T0, T1, T2)
        where T0: PSQLDecodable, T1: PSQLDecodable, T2: PSQLDecodable
    {
        assert(self.columns.count >= 3)
        var buffer = self.data.bytes
        
        return try (
            self.decodeNextColumn(t0, from: &buffer, index: 0, file: file, line: line),
            self.decodeNextColumn(t1, from: &buffer, index: 1, file: file, line: line),
            self.decodeNextColumn(t2, from: &buffer, index: 2, file: file, line: line)
        )
    }
    
    @inlinable
    public func decode<T0, T1, T2, T3>(_ t0: T0.Type, _ t1: T1.Type, _ t2: T2.Type, _ t3: T3.Type, file: String = #file, line: Int = #line) throws -> (T0, T1, T2, T3)
        where T0: PSQLDecodable, T1: PSQLDecodable, T2: PSQLDecodable, T3: PSQLDecodable
    {
        assert(self.columns.count >= 4)
        var buffer = self.data.bytes
        
        return try (
            self.decodeNextColumn(t0, from: &buffer, index: 0, file: file, line: line),
            self.decodeNextColumn(t1, from: &buffer, index: 1, file: file, line: line),
            self.decodeNextColumn(t2, from: &buffer, index: 2, file: file, line: line),
            self.decodeNextColumn(t3, from: &buffer, index: 3, file: file, line: line)
        )
    }
    
    @inlinable
    public func decode<T0, T1, T2, T3, T4>(_ t0: T0.Type, _ t1: T1.Type, _ t2: T2.Type, _ t3: T3.Type, _ t4: T4.Type, file: String = #file, line: Int = #line) throws -> (T0, T1, T2, T3, T4)
        where T0: PSQLDecodable, T1: PSQLDecodable, T2: PSQLDecodable, T3: PSQLDecodable, T4: PSQLDecodable
    {
        assert(self.columns.count >= 5)
        var buffer = self.data.bytes
        
        return try (
            self.decodeNextColumn(t0, from: &buffer, index: 0, file: file, line: line),
            self.decodeNextColumn(t1, from: &buffer, index: 1, file: file, line: line),
            self.decodeNextColumn(t2, from: &buffer, index: 2, file: file, line: line),
            self.decodeNextColumn(t3, from: &buffer, index: 3, file: file, line: line),
            self.decodeNextColumn(t4, from: &buffer, index: 4, file: file, line: line)
        )
    }
    
    @inlinable
    func decodeNextColumn<T: PSQLDecodable>(_ t: T.Type, from buffer: inout ByteBuffer, index: Int, file: String, line: Int) throws -> T {
        var slice = buffer.readLengthPrefixedSlice(as: Int32.self)

        let dc0 = PSQLDecodingContext(
            jsonDecoder: jsonDecoder,
            columnName: self.columns[index].name,
            columnIndex: index,
            file: file,
            line: line
        )
        let r = try T.decodeRaw(from: &slice, type: self.columns[index].dataType, format: self.columns[index].format, context: dc0)
        return r
    }
}
