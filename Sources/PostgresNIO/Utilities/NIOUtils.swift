import Foundation
import NIOCore

internal extension ByteBuffer {
    mutating func readRawRepresentableInteger<E>(endianness: Endianness = .big, as rawRepresentable: E.Type) -> E? where E: RawRepresentable, E.RawValue: FixedWidthInteger {
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
}

internal extension Sequence where Element == UInt8 {
    func hexdigest() -> String {
        return reduce("") { $0 + String(format: "%02x", $1) }
    }
}
