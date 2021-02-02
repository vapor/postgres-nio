//
//  File.swift
//
//
//  Created by Fabian Fett on 15.01.21.
//

import NIO

internal extension ByteBuffer {
    mutating func writeNullTerminatedString(_ string: String) {
        self.writeString(string)
        self.writeInteger(0, as: UInt8.self)
    }
    
    mutating func readNullTerminatedString() -> String? {
        if let nullIndex = readableBytesView.firstIndex(of: 0) {
            defer { moveReaderIndex(forwardBy: 1) }
            return readString(length: nullIndex - readerIndex)
        } else {
            return nil
        }
    }
    
    mutating func writeBackendMessageID(_ messageID: PSQLBackendMessage.ID) {
        self.writeInteger(messageID.rawValue)
    }
    
    mutating func writeFrontendMessageID(_ messageID: PSQLFrontendMessage.ID) {
        self.writeInteger(messageID.byte)
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
