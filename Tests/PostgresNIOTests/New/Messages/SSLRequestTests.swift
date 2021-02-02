//
//  File.swift
//
//
//  Created by Fabian Fett on 12.01.21.
//

import XCTest
@testable import PostgresNIO

class SSLRequestTests: XCTestCase {
    
    func testSSLRequest() {
        let encoder = PSQLFrontendMessage.Encoder.forTests
        var byteBuffer = ByteBuffer()
        let request = PSQLFrontendMessage.SSLRequest()
        let message = PSQLFrontendMessage.sslRequest(request)
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        let byteBufferLength = Int32(byteBuffer.readableBytes)
        XCTAssertEqual(byteBufferLength, byteBuffer.readInteger())
        XCTAssertEqual(request.code, byteBuffer.readInteger())
        
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
}
