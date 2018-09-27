import NIO

internal extension ByteBuffer {
    mutating func readNullTerminatedString() -> String? {
        // scan for null terminator
        var currentIndex = readerIndex
        while true {
            guard let peek = getInteger(at: currentIndex, as: UInt8.self) else {
                return nil
            }
            if peek == 0x00 {
                break
            }
            currentIndex += 1
        }
        // read the string and defer consuming the null terminator
        defer { moveReaderIndex(forwardBy: 1) }
        return readString(length: currentIndex - readerIndex)
    }
    
    mutating func readInteger<E>(endianness: Endianness = .big, rawRepresentable: E.Type) -> E? where E: RawRepresentable, E.RawValue: FixedWidthInteger {
        guard let rawValue = readInteger(endianness: endianness, as: E.RawValue.self) else {
            return nil
        }
        return E.init(rawValue: rawValue)
    }
    
    mutating func readNullableBytes() -> [UInt8]? {
        guard let count: Int = readInteger(as: Int32.self).flatMap(numericCast) else {
            return nil
        }
        switch count {
        case -1:
            // As a special case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.
            return nil
        default: return readBytes(length: count)
        }
    }
    
    mutating func readArray<T>(_ type: T.Type, _ closure: (inout ByteBuffer) throws -> (T)) rethrows -> [T]? {
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

internal extension Array where Element == UInt8 {
    func hexdigest() -> String {
        return reduce("") { $0 + String(format: "%02x", $1) }
    }
}
