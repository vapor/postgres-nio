//
//  File.swift
//  
//
//  Created by Fabian Fett on 11.01.21.
//

extension PSQLFrontendMessage {
    
    struct Cancel: PayloadEncodable, Equatable {
        let processID: Int32
        let secretKey: Int32
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeInteger(80877102, as: Int32.self)
            buffer.writeInteger(self.processID)
            buffer.writeInteger(self.secretKey)
        }
    }
    
}
