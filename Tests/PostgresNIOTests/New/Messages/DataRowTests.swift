//
//  File.swift
//
//
//  Created by Fabian Fett on 12.01.21.
//

import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class DataRowTests: XCTestCase {
    func testDecode() {
        let buffer = ByteBuffer.backendMessage(id: .dataRow) { buffer in
            buffer.writeInteger(2, as: Int16.self)
            buffer.writeInteger(-1, as: Int32.self)
            buffer.writeInteger(10, as: Int32.self)
            buffer.writeBytes([UInt8](repeating: 5, count: 10))
        }

        let expectedColumns: [ByteBuffer?] = [
            nil,
            ByteBuffer(bytes: [UInt8](repeating: 5, count: 10))
        ]
        
        let expectedInOuts = [
            (buffer, [PSQLBackendMessage.dataRow(.init(columns: expectedColumns))]),
        ]
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: expectedInOuts,
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) }))
    }
}
