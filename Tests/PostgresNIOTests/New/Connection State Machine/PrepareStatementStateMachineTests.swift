import Testing
import NIOEmbedded
@testable import PostgresNIO

@Suite struct PrepareStatementStateMachineTests {
    @Test func testCreatePreparedStatementReturningRowDescription() throws {
        var state = try ConnectionStateMachine.makeReadyForQuery()

        let promise = EmbeddedEventLoop().makePromise(of: RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        
        let name = "haha"
        let query = #"SELECT id FROM users WHERE id = $1 "#
        let prepareStatementContext = ExtendedQueryContext(
            name: name, query: query, bindingDataTypes: [], logger: .psqlTest, promise: promise
        )

        #expect(state.enqueue(task: .extendedQuery(prepareStatementContext)) ==
                       .sendParseDescribeSync(name: name, query: query, bindingDataTypes: []))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)

        let columns: [RowDescription.Column] = [
            .init(name: "id", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: -1, format: .binary)
        ]
        
        #expect(state.rowDescriptionReceived(.init(columns: columns)) ==
                       .succeedPreparedStatementCreation(promise, with: .init(columns: columns)))
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }
    
    @Test func testCreatePreparedStatementReturningNoData() throws {
        var state = try ConnectionStateMachine.makeReadyForQuery()

        let promise = EmbeddedEventLoop().makePromise(of: RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        
        let name = "haha"
        let query = #"DELETE FROM users WHERE id = $1 "#
        let prepareStatementContext = ExtendedQueryContext(
            name: name, query: query, bindingDataTypes: [], logger: .psqlTest, promise: promise
        )

        #expect(state.enqueue(task: .extendedQuery(prepareStatementContext)) ==
                       .sendParseDescribeSync(name: name, query: query, bindingDataTypes: []))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)

        #expect(state.noDataReceived() ==
                       .succeedPreparedStatementCreation(promise, with: nil))
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)
    }
    
    @Test func testErrorReceivedAfter() throws {
        var state = try ConnectionStateMachine.makeReadyForQuery()

        let promise = EmbeddedEventLoop().makePromise(of: RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.

        let name = "haha"
        let query = #"DELETE FROM users WHERE id = $1 "#
        let prepareStatementContext = ExtendedQueryContext(
            name: name, query: query, bindingDataTypes: [], logger: .psqlTest, promise: promise
        )

        #expect(state.enqueue(task: .extendedQuery(prepareStatementContext)) ==
                       .sendParseDescribeSync(name: name, query: query, bindingDataTypes: []))
        #expect(state.parseCompleteReceived() == .wait)
        #expect(state.parameterDescriptionReceived(.init(dataTypes: [.int8])) == .wait)

        #expect(state.noDataReceived() ==
                       .succeedPreparedStatementCreation(promise, with: nil))
        #expect(state.readyForQueryReceived(.idle) == .fireEventReadyForQuery)

        #expect(state.authenticationMessageReceived(.ok) ==
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .unexpectedBackendMessage(.authentication(.ok)), closePromise: nil)))
    }
}
