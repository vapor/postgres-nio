import NIOCore

internal extension ByteBuffer {
    
    @usableFromInline
    mutating func psqlReadFloat() -> Float? {
        return self.readInteger(as: UInt32.self).map { Float(bitPattern: $0) }
    }

    @usableFromInline
    mutating func psqlReadDouble() -> Double? {
        return self.readInteger(as: UInt64.self).map { Double(bitPattern: $0) }
    }

    @usableFromInline
    mutating func psqlWriteFloat(_ float: Float) {
        self.writeInteger(float.bitPattern)
    }

    @usableFromInline
    mutating func psqlWriteDouble(_ double: Double) {
        self.writeInteger(double.bitPattern)
    }
}
