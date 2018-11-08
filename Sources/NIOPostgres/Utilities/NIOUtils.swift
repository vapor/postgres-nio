import Foundation
import NIO

internal extension ByteBuffer {
    mutating func readNullTerminatedString() -> String? {
        if let nullIndex = readableBytesView.firstIndex(of: 0) {
            defer { moveReaderIndex(forwardBy: 1) }
            return readString(length: nullIndex - readerIndex)
        } else {
            return nil
        }
    }
    
    mutating func write(nullTerminated string: String) {
        write(string: string)
        write(integer: 0, as: UInt8.self)
    }
    
    mutating func readInteger<E>(endianness: Endianness = .big, rawRepresentable: E.Type) -> E? where E: RawRepresentable, E.RawValue: FixedWidthInteger {
        guard let rawValue = readInteger(endianness: endianness, as: E.RawValue.self) else {
            return nil
        }
        return E.init(rawValue: rawValue)
    }
    
    mutating func readNullableBytes() -> ByteBuffer? {
        guard let count: Int = readInteger(as: Int32.self).flatMap(numericCast) else {
            return nil
        }
        switch count {
        case -1:
            // As a special case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.
            return nil
        default: return readSlice(length: count)
        }
    }
    
    mutating func write<T>(array: [T], closure: (inout ByteBuffer, T) -> ()) {
        write(integer: numericCast(array.count), as: Int16.self)
        for el in array {
            closure(&self, el)
        }
    }
    
    mutating func write<T>(array: [T]) where T: FixedWidthInteger {
        write(array: array) { buffer, el in
            buffer.write(integer: el)
        }
    }
    
    mutating func write<T>(array: [T]) where T: RawRepresentable, T.RawValue: FixedWidthInteger {
        write(array: array) { buffer, el in
            buffer.write(integer: el.rawValue)
        }
    }
    
    mutating func read<T>(array type: T.Type, _ closure: (inout ByteBuffer) throws -> (T)) rethrows -> [T]? {
        guard let count: Int = readInteger(as: Int16.self).flatMap(numericCast) else {
            return nil
        }
        var array: [T] = []
        array.reserveCapacity(count)
        for _ in 0..<count {
            try array.append(closure(&self))
        }
        return array
    }
    
    mutating func readFloat<T: BinaryFloatingPoint>(as: T.Type = T.self) -> T? {
        guard self.readableBytes >= MemoryLayout<T>.size else {
            return nil
        }
        
        let value: T = self.getFloat(at: self.readerIndex)! /* must work as we have enough bytes */
        // should be _moveReaderIndex
        self.moveReaderIndex(forwardBy: MemoryLayout<T>.size)
        return value
    }

    func getFloat<T: BinaryFloatingPoint>(at index: Int, as: T.Type = T.self) -> T? {
        precondition(index >= 0, "index must not be negative")
        return self.withVeryUnsafeBytes { ptr in
            guard index <= ptr.count - MemoryLayout<T>.size else {
                return nil
            }
            var value: T = 0
            withUnsafeMutableBytes(of: &value) { valuePtr in
                #warning("improve performance")
                valuePtr.copyBytes(
                    from: UnsafeRawBufferPointer(
                        start: ptr.baseAddress!.advanced(by: index),
                        count: MemoryLayout<T>.size
                    ).reversed()
                )
            }
            return value
        }
    }
    
    mutating func readUUID() -> UUID? {
        guard self.readableBytes >= MemoryLayout<UUID>.size else {
            return nil
        }
        
        let value: UUID = self.getUUID(at: self.readerIndex)! /* must work as we have enough bytes */
        // should be _moveReaderIndex
        self.moveReaderIndex(forwardBy: MemoryLayout<UUID>.size)
        return value
    }
    
    func getUUID(at index: Int) -> UUID? {
        precondition(index >= 0, "index must not be negative")
        return self.withVeryUnsafeBytes { ptr in
            guard index <= ptr.count - MemoryLayout<uuid_t>.size else {
                return nil
            }
            var value: uuid_t = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            withUnsafeMutableBytes(of: &value) { valuePtr in
                valuePtr.copyMemory(
                    from: UnsafeRawBufferPointer(
                        start: ptr.baseAddress!.advanced(by: index),
                        count: MemoryLayout<UUID>.size
                    )
                )
            }
            return UUID(uuid: value)
        }
    }
}

internal extension Sequence where Element == UInt8 {
    func hexdigest() -> String {
        return reduce("") { $0 + String(format: "%02x", $1) }
    }
}
