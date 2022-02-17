import XCTest
import NIOCore
@testable import PostgresNIO

class ParseTests: XCTestCase {

    func testEncode() {
        let encoder = PSQLFrontendMessageEncoder.forTests
        var byteBuffer = ByteBuffer()
        let parse = PSQLFrontendMessage.Parse(
            preparedStatementName: "test",
            query: "SELECT version()",
            parameters: [.bool, .int8, .bytea, .varchar, .text, .uuid, .json, .jsonbArray])
        let message = PSQLFrontendMessage.parse(parse)
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        let length: Int = 1 + 4 + (parse.preparedStatementName.count + 1) + (parse.query.count + 1) + 2 + parse.parameters.count * 4

        //   1 id
        // + 4 length
        // + 4 preparedStatement (3 + 1 null terminator)
        // + 1 query ()
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PSQLFrontendMessage.ID.parse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), parse.preparedStatementName)
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), parse.query)
        XCTAssertEqual(byteBuffer.readInteger(as: Int16.self), Int16(parse.parameters.count))
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.bool.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.int8.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.bytea.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.varchar.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.text.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.uuid.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.json.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), PostgresDataType.jsonbArray.rawValue)
    }

}
