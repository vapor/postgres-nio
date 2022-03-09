#if swift(>=5.6)
@preconcurrency import NIOCore
#else
import NIOCore
#endif

/// A backend data row message.
///
/// - NOTE: This struct is not part of the ``PSQLBackendMessage`` namespace even
///         though this is where it actually belongs. The reason for this is, that we want
///         this type to be @usableFromInline. If a type is made @usableFromInline in an
///         enclosing type, the enclosing type must be @usableFromInline as well.
///         Not putting `DataRow` in ``PSQLBackendMessage`` is our way to trick
///         the Swift compiler
@usableFromInline
struct DataRow: PostgresBackendMessage.PayloadDecodable, Equatable {
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
        let columnSlice = buffer.readSlice(length: buffer.readableBytes)!
        return DataRow(columnCount: columnCount, bytes: columnSlice)
    }
}

extension DataRow: Sequence {
    @usableFromInline
    typealias Element = ByteBuffer?
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

#if swift(>=5.6)
extension DataRow: Sendable {}
#endif
