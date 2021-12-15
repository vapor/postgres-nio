import NIOCore

internal extension ByteBuffer {
    mutating func writeBackendMessageID(_ messageID: PSQLBackendMessage.ID) {
        self.writeInteger(messageID.rawValue)
    }
    
    mutating func writeFrontendMessageID(_ messageID: PSQLFrontendMessage.ID) {
        self.writeInteger(messageID.rawValue)
    }

    @inlinable
    mutating func readFloat() -> Float? {
        guard let uint32 = self.readInteger(as: UInt32.self) else {
            return nil
        }
        return Float(bitPattern: uint32)
    }

    @inlinable
    mutating func readDouble() -> Double? {
        guard let uint64 = self.readInteger(as: UInt64.self) else {
            return nil
        }
        return Double(bitPattern: uint64)
    }

    @inlinable
    mutating func writeFloat(_ float: Float) {
        self.writeInteger(float.bitPattern)
    }

    @inlinable
    mutating func writeDouble(_ double: Double) {
        self.writeInteger(double.bitPattern)
    }
}
