import NIOCore

/// A backend data row message.
///
/// - NOTE: This struct is not part of the ``PSQLBackendMessage`` namespace even
///         though this is where it actually belongs. The reason for this is, that we want
///         this type to be @usableFromInline. If a type is made @usableFromInline in an
///         enclosing type, the enclosing type must be @usableFromInline as well.
///         Not putting `DataRow` in ``PSQLBackendMessage`` is our way to trick
///         the Swift compiler
@usableFromInline
struct DataRow: PSQLBackendMessage.PayloadDecodable, Equatable {

    @usableFromInline
    var columnCount: Int16

    @usableFromInline
    var bytes: ByteBuffer
    
    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        let columnCount = try buffer.throwingReadInteger(as: Int16.self)
        let firstColumnIndex = buffer.readerIndex
        
        for _ in 0..<columnCount {
            let bufferLength = try buffer.throwingReadInteger(as: Int32.self)
            guard bufferLength >= 0 else {
                // if buffer length is negative, this means that the value is null
                continue
            }

            try buffer.throwingMoveReaderIndex(forwardBy: Int(bufferLength))
        }
        
        buffer.moveReaderIndex(to: firstColumnIndex)
        let rowSlice = buffer.readSlice(length: buffer.readableBytes)!
        return DataRow(columnCount: columnCount, bytes: rowSlice)
    }
}

extension DataRow: Sequence {
    @usableFromInline
    typealias Element = ByteBuffer?

    @inlinable
    func makeIterator() -> Iterator {
        return Iterator(self)
    }
    
    // There is no contiguous storage available... Sadly
    @usableFromInline
    func withContiguousStorageIfAvailable<R>(_ body: (UnsafeBufferPointer<ByteBuffer?>) throws -> R) rethrows -> R? {
        nil
    }
}

extension DataRow: Collection {

    @usableFromInline
    struct ColumnIndex: Comparable {
        @usableFromInline
        var offset: Int

        @inlinable
        init(_ index: Int) {
            self.offset = index
        }
        
        // Only needed implementation for comparable. The compiler synthesizes the rest from this.
        @inlinable
        static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.offset < rhs.offset
        }
    }

    @usableFromInline
    typealias Index = DataRow.ColumnIndex

    @inlinable
    var startIndex: ColumnIndex {
        ColumnIndex(self.bytes.readerIndex)
    }

    @inlinable
    var endIndex: ColumnIndex {
        ColumnIndex(self.bytes.readerIndex + self.bytes.readableBytes)
    }

    @inlinable
    var count: Int {
        Int(self.columnCount)
    }

    @inlinable
    func index(after index: ColumnIndex) -> ColumnIndex {
        guard index < self.endIndex else {
            preconditionFailure("index out of bounds")
        }
        var elementLength = Int(self.bytes.getInteger(at: index.offset, as: Int32.self)!)
        if elementLength < 0 {
            elementLength = 0
        }
        return ColumnIndex(index.offset + MemoryLayout<Int32>.size + elementLength)
    }

    @inlinable
    subscript(index: ColumnIndex) -> Element {
        guard index < self.endIndex else {
            preconditionFailure("index out of bounds")
        }
        let elementLength = Int(self.bytes.getInteger(at: index.offset, as: Int32.self)!)
        if elementLength < 0 {
            return nil
        }
        return self.bytes.getSlice(at: index.offset + MemoryLayout<Int32>.size, length: elementLength)!
    }
}

extension DataRow {
    subscript(column index: Int) -> Element {
        guard index < self.columnCount else {
            preconditionFailure("index out of bounds")
        }
        
        var byteIndex = self.startIndex
        for _ in 0..<index {
            byteIndex = self.index(after: byteIndex)
        }
        
        return self[byteIndex]
    }
}

extension DataRow {
    @usableFromInline
    struct Iterator: Swift.IteratorProtocol {
        @usableFromInline
        typealias Element = ByteBuffer?

        @usableFromInline
        let dataRow: DataRow

        @usableFromInline
        var columnIndex: Int16 = 0

        @usableFromInline
        var bufferOffset: Int

        @inlinable
        init(_ dataRow: DataRow) {
            self.dataRow = dataRow
            self.bufferOffset = dataRow.bytes.readerIndex
        }

        @inlinable
        mutating func next() -> Optional<Optional<ByteBuffer>> {
            guard self.columnIndex < self.dataRow.columnCount else {
                return .none
            }

            self.columnIndex &+= 1

            let elementLength = Int(self.dataRow.bytes.getInteger(at: self.bufferOffset, as: Int32.self)!)
            self.bufferOffset &+= MemoryLayout<Int32>.size
            if elementLength < 0 {
                return .some(.none)
            }
            defer {
                self.bufferOffset &+= elementLength
            }
            return .some(self.dataRow.bytes.getSlice(at: self.bufferOffset, length: elementLength)!)
        }
    }
}
