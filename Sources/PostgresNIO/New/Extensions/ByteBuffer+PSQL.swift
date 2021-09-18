import NIOCore

internal extension ByteBuffer {
    mutating func writeNullTerminatedString(_ string: String) {
        self.writeString(string)
        self.writeInteger(0, as: UInt8.self)
    }
    
    mutating func readNullTerminatedString() -> String? {
        guard let nullIndex = readableBytesView.firstIndex(of: 0) else {
            return nil
        }

        defer { moveReaderIndex(forwardBy: 1) }
        return readString(length: nullIndex - readerIndex)
    }
    
    mutating func writeBackendMessageID(_ messageID: PSQLBackendMessage.ID) {
        self.writeInteger(messageID.rawValue)
    }
    
    mutating func writeFrontendMessageID(_ messageID: PSQLFrontendMessage.ID) {
        self.writeInteger(messageID.rawValue)
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
}
