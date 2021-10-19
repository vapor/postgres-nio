import NIOCore

/// A backend data row message.
///
/// - NOTE: This struct is not part of the ``PSQLBackendMessage`` namespace even
///         though this is where it actually belongs. The reason for this is, that we want
///         this type to be @usableFromInline. If a type is made @usableFromInline in an
///         enclosing type, the enclosing type must be @usableFromInline as well.
///         Not putting `DataRow` in ``PSQLBackendMessage`` is our way to trick
///         the Swift compiler.
@usableFromInline
struct DataRow: PSQLBackendMessage.PayloadDecodable, Equatable {
    
    @usableFromInline
    var columnCount: Int16
    
    @usableFromInline
    var bytes: ByteBuffer
    
    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        try buffer.ensureAtLeastNBytesRemaining(2)
        let columnCount = buffer.readInteger(as: Int16.self)!
        let firstColumnIndex = buffer.readerIndex
        
        for _ in 0..<columnCount {
            try buffer.ensureAtLeastNBytesRemaining(2)
            let bufferLength = Int(buffer.readInteger(as: Int32.self)!)
            
            guard bufferLength >= 0 else {
                // if buffer length is negative, this means that the value is null
                continue
            }
            
            try buffer.ensureAtLeastNBytesRemaining(bufferLength)
            buffer.moveReaderIndex(forwardBy: bufferLength)
        }
        
        try buffer.ensureExactNBytesRemaining(0)
        
        buffer.moveReaderIndex(to: firstColumnIndex)
        let columnSlice = buffer.readSlice(length: buffer.readableBytes)!
        return DataRow(columnCount: columnCount, bytes: columnSlice)
    }
}

extension DataRow: Sequence {
    @usableFromInline
    typealias Element = ByteBuffer?
    
    // There is no contiguous storage available... Sadly
    @inlinable
    func withContiguousStorageIfAvailable<R>(_ body: (UnsafeBufferPointer<ByteBuffer?>) throws -> R) rethrows -> R? {
        nil
    }
}

extension DataRow: Collection {
    
    @usableFromInline
    struct ByteIndex: Comparable {
        var offset: Int
        
        init(_ index: Int) {
            self.offset = index
        }
        
        @usableFromInline
        static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.offset == rhs.offset
        }
        
        @usableFromInline
        static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.offset < rhs.offset
        }

        @usableFromInline
        static func <= (lhs: Self, rhs: Self) -> Bool {
            lhs.offset <= rhs.offset
        }

        @usableFromInline
        static func >= (lhs: Self, rhs: Self) -> Bool {
            lhs.offset >= rhs.offset
        }

        @usableFromInline
        static func > (lhs: Self, rhs: Self) -> Bool {
            lhs.offset > rhs.offset
        }
    }
    
    @usableFromInline
    typealias Index = DataRow.ByteIndex
    
    @usableFromInline
    var startIndex: ByteIndex {
        ByteIndex(self.bytes.readerIndex)
    }
    
    @usableFromInline
    var endIndex: ByteIndex {
        ByteIndex(self.bytes.readerIndex + self.bytes.readableBytes)
    }
    
    @usableFromInline
    var count: Int {
        Int(self.columnCount)
    }
    
    @usableFromInline
    func index(after index: ByteIndex) -> ByteIndex {
        guard index < self.endIndex else {
            preconditionFailure("index out of bounds")
        }
        var elementLength = Int(self.bytes.getInteger(at: index.offset, as: Int32.self)!)
        if elementLength < 0 {
            elementLength = 0
        }
        return ByteIndex(index.offset + MemoryLayout<Int32>.size + elementLength)
    }
    
    @usableFromInline
    subscript(index: ByteIndex) -> Element {
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
    @usableFromInline
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
