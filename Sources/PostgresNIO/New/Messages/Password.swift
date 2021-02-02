//
//  File.swift
//  
//
//  Created by Fabian Fett on 06.01.21.
//

extension PSQLFrontendMessage {
    
    struct Password: PayloadEncodable, Equatable {
        let value: String
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeString(value)
            buffer.writeInteger(UInt8(0))
        }
    }
    
}
