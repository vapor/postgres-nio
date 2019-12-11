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
        self.writeString(string)
        self.writeInteger(0, as: UInt8.self)
    }
    
    mutating func readInteger<E>(endianness: Endianness = .big, as rawRepresentable: E.Type) -> E? where E: RawRepresentable, E.RawValue: FixedWidthInteger {
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
        self.writeInteger(numericCast(array.count), as: Int16.self)
        for el in array {
            closure(&self, el)
        }
    }
    
    mutating func write<T>(array: [T]) where T: FixedWidthInteger {
        self.write(array: array) { buffer, el in
            buffer.writeInteger(el)
        }
    }
    
    mutating func write<T>(array: [T]) where T: RawRepresentable, T.RawValue: FixedWidthInteger {
        self.write(array: array) { buffer, el in
            buffer.writeInteger(el.rawValue)
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

    mutating func readFloat() -> Float? {
        return self.readInteger(as: UInt32.self).map { Float(bitPattern: $0) }
    }

    mutating func readDouble() -> Double? {
        return self.readInteger(as: UInt64.self).map { Double(bitPattern: $0) }
    }

    mutating func writeFloat(_ float: Float) {
        self.writeInteger(float.bitPattern)
    }

    mutating func writeDouble(_ double: Double) {
        self.writeInteger(double.bitPattern)
    }

    mutating func readUUID() -> UUID? {
        guard self.readableBytes >= MemoryLayout<UUID>.size else {
            return nil
        }
        
        let value: UUID = self.getUUID(at: self.readerIndex)! /* must work as we have enough bytes */
        // should be MoveReaderIndex
        self.moveReaderIndex(forwardBy: MemoryLayout<UUID>.size)
        return value
    }
    
    func getUUID(at index: Int) -> UUID? {
        var uuid: uuid_t = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        return self.viewBytes(at: index, length: MemoryLayout.size(ofValue: uuid)).map { bufferBytes in
            withUnsafeMutableBytes(of: &uuid) { target in
                precondition(target.count <= bufferBytes.count)
                target.copyBytes(from: bufferBytes)
            }
            return UUID(uuid: uuid)
        }
    }
}

internal extension Sequence where Element == UInt8 {
    func hexdigest() -> String {
        return reduce("") { $0 + String(format: "%02x", $1) }
    }
}
