import Testing
import NIOCore
import NIOEmbedded
import Logging
@testable import PostgresNIO

@Suite struct ExtendedQueryStateMachineTests {
    
    @Test func testExtendedQueryWithoutDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "DELETE FROM table WHERE id=\(1)"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)
        
        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)
        #expect(state.noDataReceived() == .wait)
        #expect(state.bindCompleteReceived() == .wait)
        #expect(state.commandCompletedReceived("DELETE 1") == .succeedQuery(promise, with: .init(value: .noRows(.tag("DELETE 1")), logger: logger)))
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }
    
    @Test func testExtendedQueryWithDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)
        
        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)
        
        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }
        
        #expect(state.rowDescriptionReceived(.init(columns: input)) == .wait)
        #expect(state.bindCompleteReceived() == .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        let row1: DataRow = [ByteBuffer(string: "test1")]
        #expect(state.dataRowReceived(row1) == .wait)
        #expect(state.channelReadComplete() == .forwardRows([row1]))
        #expect(state.readEventCaught() == .wait)
        #expect(state.requestQueryRows() == .read)
        
        let row2: DataRow = [ByteBuffer(string: "test2")]
        let row3: DataRow = [ByteBuffer(string: "test3")]
        let row4: DataRow = [ByteBuffer(string: "test4")]
        #expect(state.dataRowReceived(row2) == .wait)
        #expect(state.dataRowReceived(row3) == .wait)
        #expect(state.dataRowReceived(row4) == .wait)
        #expect(state.channelReadComplete() == .forwardRows([row2, row3, row4]))
        #expect(state.requestQueryRows() == .wait)
        #expect(state.readEventCaught() == .read)
        
        #expect(state.channelReadComplete() == .wait)
        #expect(state.readEventCaught() == .read)
        
        let row5: DataRow = [ByteBuffer(string: "test5")]
        let row6: DataRow = [ByteBuffer(string: "test6")]
        #expect(state.dataRowReceived(row5) == .wait)
        #expect(state.dataRowReceived(row6) == .wait)
        
        #expect(state.commandCompletedReceived("SELECT 2") == .forwardStreamComplete([row5, row6], commandTag: "SELECT 2"))
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }

    @Test func testExtendedQueryWithNoQuery() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "-- some comments"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)
        #expect(state.noDataReceived() == .wait)
        #expect(state.bindCompleteReceived() == .wait)
        #expect(state.emptyQueryResponseReceived() == .succeedQuery(promise, with: .init(value: .noRows(.emptyResponse), logger: logger)))
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }

    @Test func testReceiveTotallyUnexpectedMessageInQuery() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "DELETE FROM table WHERE id=\(1)"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)
        
        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)
        
        let psqlError = PSQLError.unexpectedBackendMessage(.authentication(.ok))
        #expect(state.authenticationMessageReceived(.ok) ==
                       .failQuery(promise, with: psqlError, cleanupContext: .init(action: .close, tasks: [], error: psqlError, closePromise: nil)))
    }

    @Test func testExtendedQueryIsCancelledImmediately() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }

        #expect(state.rowDescriptionReceived(.init(columns: input)) == .wait)
        #expect(state.bindCompleteReceived() == .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        #expect(state.cancel() == .forwardStreamError(.queryCancelled, read: false, cleanupContext: nil))
        #expect(state.dataRowReceived([ByteBuffer(string: "test1")]) == .wait)
        #expect(state.channelReadComplete() == .wait)
        #expect(state.readEventCaught() == .read)

        #expect(state.dataRowReceived([ByteBuffer(string: "test2")]) == .wait)
        #expect(state.dataRowReceived([ByteBuffer(string: "test3")]) == .wait)
        #expect(state.dataRowReceived([ByteBuffer(string: "test4")]) == .wait)
        #expect(state.channelReadComplete() == .wait)
        #expect(state.readEventCaught() == .read)

        #expect(state.channelReadComplete() == .wait)
        #expect(state.readEventCaught() == .read)

        #expect(state.commandCompletedReceived("SELECT 2") == .wait)
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }

    @Test func testExtendedQueryIsCancelledWithReadPending() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }

        #expect(state.rowDescriptionReceived(.init(columns: input)) == .wait)
        #expect(state.bindCompleteReceived() == .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        let row1: DataRow = [ByteBuffer(string: "test1")]
        #expect(state.dataRowReceived(row1) == .wait)
        #expect(state.channelReadComplete() == .forwardRows([row1]))
        #expect(state.readEventCaught() == .wait)
        #expect(state.cancel() == .forwardStreamError(.queryCancelled, read: true, cleanupContext: nil))

        #expect(state.dataRowReceived([ByteBuffer(string: "test2")]) == .wait)
        #expect(state.dataRowReceived([ByteBuffer(string: "test3")]) == .wait)
        #expect(state.dataRowReceived([ByteBuffer(string: "test4")]) == .wait)
        #expect(state.channelReadComplete() == .wait)
        #expect(state.readEventCaught() == .read)

        #expect(state.commandCompletedReceived("SELECT 4") == .wait)
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }

    @Test func testCancelQueryAfterServerError() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)

        // We need to ensure that even though the row description from the wire says that we
        // will receive data in `.text` format, we will actually receive it in binary format,
        // since we requested it in binary with our bind message.
        let input: [RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, format: .text)
        ]
        let expected: [RowDescription.Column] = input.map {
            .init(name: $0.name, tableOID: $0.tableOID, columnAttributeNumber: $0.columnAttributeNumber, dataType: $0.dataType,
                  dataTypeSize: $0.dataTypeSize, dataTypeModifier: $0.dataTypeModifier, format: .binary)
        }

        #expect(state.rowDescriptionReceived(.init(columns: input)) == .wait)
        #expect(state.bindCompleteReceived() == .succeedQuery(promise, with: .init(value: .rowDescription(expected), logger: logger)))
        let dataRows1: [DataRow] = [
            [ByteBuffer(string: "test1")],
            [ByteBuffer(string: "test2")],
            [ByteBuffer(string: "test3")]
        ]
        for row in dataRows1 {
            #expect(state.dataRowReceived(row) == .wait)
        }
        #expect(state.channelReadComplete() == .forwardRows(dataRows1))
        #expect(state.readEventCaught() == .wait)
        #expect(state.requestQueryRows() == .read)
        let dataRows2: [DataRow] = [
            [ByteBuffer(string: "test4")],
            [ByteBuffer(string: "test5")],
            [ByteBuffer(string: "test6")]
        ]
        for row in dataRows2 {
            #expect(state.dataRowReceived(row) == .wait)
        }
        let serverError = PostgresBackendMessage.ErrorResponse(fields: [.severity: "Error", .sqlState: "123"])
        #expect(state.errorReceived(serverError) == .forwardStreamError(.server(serverError), read: false, cleanupContext: .none))

        #expect(state.channelReadComplete() == .wait)
        #expect(state.readEventCaught() == .read)

        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }

    @Test func testQueryErrorDoesNotKillConnection() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)

        let serverError = PostgresBackendMessage.ErrorResponse(fields: [.severity: "Error", .sqlState: "123"])
        #expect(
            state.errorReceived(serverError) == .failQuery(promise, with: .server(serverError), cleanupContext: .none)
        )

        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }

    @Test func testQueryErrorAfterCancelDoesNotKillConnection() {
        var state = ConnectionStateMachine.readyForQuery()

        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRowStream.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query: PostgresQuery = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, logger: logger, promise: promise)

        #expect(state.enqueue(task: .extendedQuery(queryContext)) == .sendParseDescribeBindExecuteSync(query))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)
        #expect(state.cancel() == .failQuery(promise, with: .queryCancelled, cleanupContext: .none))

        let serverError = PostgresBackendMessage.ErrorResponse(fields: [.severity: "Error", .sqlState: "123"])
        #expect(state.errorReceived(serverError) == .wait)

        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }

}
