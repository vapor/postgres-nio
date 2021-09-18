import XCTest
import NIOCore
import NIOEmbedded
import Logging
@testable import PostgresNIO

class ExtendedQueryStateMachineTests: XCTestCase {
    
    func testExtendedQueryWithoutDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "DELETE FROM table WHERE id=$0"
        let queryContext = ExtendedQueryContext(query: query, bind: [1], logger: logger, jsonDecoder: JSONDecoder(), promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext)), .sendParseDescribeBindExecuteSync(query: query, binds: [1]))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        XCTAssertEqual(state.noDataReceived(), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .wait)
        XCTAssertEqual(state.commandCompletedReceived("DELETE 1"), .succeedQueryNoRowsComming(queryContext, commandTag: "DELETE 1"))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testExtendedQueryWithDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let queryPromise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        queryPromise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: queryPromise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext)), .sendParseDescribeBindExecuteSync(query: query, binds: []))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [PSQLBackendMessage.RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [PSQLBackendMessage.RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }
        
        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: input)), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .succeedQuery(queryContext, columns: expected))
        let row1: PSQLBackendMessage.DataRow = [ByteBuffer(string: "test1")]
        XCTAssertEqual(state.dataRowReceived(row1), .wait)
        XCTAssertEqual(state.channelReadComplete(), .forwardRows([row1]))
        XCTAssertEqual(state.readEventCaught(), .wait)
        XCTAssertEqual(state.requestQueryRows(), .read)
        
        let row2: PSQLBackendMessage.DataRow = [ByteBuffer(string: "test2")]
        let row3: PSQLBackendMessage.DataRow = [ByteBuffer(string: "test3")]
        let row4: PSQLBackendMessage.DataRow = [ByteBuffer(string: "test4")]
        XCTAssertEqual(state.dataRowReceived(row2), .wait)
        XCTAssertEqual(state.dataRowReceived(row3), .wait)
        XCTAssertEqual(state.dataRowReceived(row4), .wait)
        XCTAssertEqual(state.channelReadComplete(), .forwardRows([row2, row3, row4]))
        XCTAssertEqual(state.requestQueryRows(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)
        
        XCTAssertEqual(state.channelReadComplete(), .wait)
        XCTAssertEqual(state.readEventCaught(), .read)
        
        let row5: PSQLBackendMessage.DataRow = [ByteBuffer(string: "test5")]
        let row6: PSQLBackendMessage.DataRow = [ByteBuffer(string: "test6")]
        XCTAssertEqual(state.dataRowReceived(row5), .wait)
        XCTAssertEqual(state.dataRowReceived(row6), .wait)
        
        XCTAssertEqual(state.commandCompletedReceived("SELECT 2"), .forwardStreamComplete([row5, row6], commandTag: "SELECT 2"))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testReceiveTotallyUnexpectedMessageInQuery() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "DELETE FROM table WHERE id=$0"
        let queryContext = ExtendedQueryContext(query: query, bind: [1], logger: logger, jsonDecoder: JSONDecoder(), promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext)), .sendParseDescribeBindExecuteSync(query: query, binds: [1]))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        let psqlError = PSQLError.unexpectedBackendMessage(.authentication(.ok))
        XCTAssertEqual(state.authenticationMessageReceived(.ok),
                       .failQuery(queryContext, with: psqlError, cleanupContext: .init(action: .close, tasks: [], error: psqlError, closePromise: nil)))
    }

}
