//
//  File.swift
//
//
//  Created by Fabian Fett on 12.01.21.
//

import XCTest
@testable import PostgresNIO

class DescribeTests: XCTestCase {
    
    func testEncodeDescribePortal() {
        let encoder = PSQLFrontendMessage.Encoder.forTests
        var byteBuffer = ByteBuffer()
        let message = PSQLFrontendMessage.describe(.portal("Hello"))
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        XCTAssertEqual(byteBuffer.readableBytes, 12)
        XCTAssertEqual(PSQLFrontendMessage.ID.describe.byte, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(11, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "P"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("Hello", byteBuffer.readNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }
    
    func testEncodeDescribeUnnamedStatement() {
        let encoder = PSQLFrontendMessage.Encoder.forTests
        var byteBuffer = ByteBuffer()
        let message = PSQLFrontendMessage.describe(.preparedStatement(""))
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        XCTAssertEqual(byteBuffer.readableBytes, 7)
        XCTAssertEqual(PSQLFrontendMessage.ID.describe.byte, byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual(6, byteBuffer.readInteger(as: Int32.self))
        XCTAssertEqual(UInt8(ascii: "S"), byteBuffer.readInteger(as: UInt8.self))
        XCTAssertEqual("", byteBuffer.readNullTerminatedString())
        XCTAssertEqual(byteBuffer.readableBytes, 0)
    }

}
