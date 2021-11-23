import NIOCore

internal extension ByteBuffer {
    mutating func psqlWriteNullTerminatedString(_ string: String) {
        self.writeString(string)
        self.writeInteger(0, as: UInt8.self)
    }
    
    mutating func psqlReadNullTerminatedString() -> String? {
        guard let nullIndex = readableBytesView.firstIndex(of: 0) else {
            return nil
        }

        defer { moveReaderIndex(forwardBy: 1) }
        return readString(length: nullIndex - readerIndex)
    }
    
    mutating func psqlWriteBackendMessageID(_ messageID: PSQLBackendMessage.ID) {
        self.writeInteger(messageID.rawValue)
    }
    
    mutating func psqlWriteFrontendMessageID(_ messageID: PSQLFrontendMessage.ID) {
        self.writeInteger(messageID.rawValue)
    }

    mutating func psqlReadFloat() -> Float? {
        return self.readInteger(as: UInt32.self).map { Float(bitPattern: $0) }
    }

    mutating func psqlReadDouble() -> Double? {
        return self.readInteger(as: UInt64.self).map { Double(bitPattern: $0) }
    }

    mutating func psqlWriteFloat(_ float: Float) {
        self.writeInteger(float.bitPattern)
    }

    mutating func psqlWriteDouble(_ double: Double) {
        self.writeInteger(double.bitPattern)
    }
}
