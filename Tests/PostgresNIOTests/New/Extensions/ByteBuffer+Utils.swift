//
//  File.swift
//  
//
//  Created by Fabian Fett on 01.02.21.
//

import NIO
@testable import PostgresNIO

extension ByteBuffer {
    
    static func backendMessage(id: PSQLBackendMessage.ID, _ payload: (inout ByteBuffer) throws -> ()) rethrows -> ByteBuffer {
        var byteBuffer = ByteBuffer()
        try byteBuffer.writeBackendMessage(id: id, payload)
        return byteBuffer
    }
    
    mutating func writeBackendMessage(id: PSQLBackendMessage.ID, _ payload: (inout ByteBuffer) throws -> ()) rethrows {
        self.writeBackendMessageID(id)
        let lengthIndex = self.writerIndex
        self.writeInteger(Int32(0))
        try payload(&self)
        let length = self.writerIndex - lengthIndex
        self.setInteger(Int32(length), at: lengthIndex)
    }
}
