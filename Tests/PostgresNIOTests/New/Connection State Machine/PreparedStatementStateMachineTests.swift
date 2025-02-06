import XCTest
import NIOEmbedded
@testable import PostgresNIO

class PreparedStatementStateMachineTests: XCTestCase {
    func testPrepareAndExecuteStatement() {
        let eventLoop = EmbeddedEventLoop()
        var stateMachine = PreparedStatementStateMachine()

        let firstPreparedStatement = self.makePreparedStatementContext(eventLoop: eventLoop)
        // Initial lookup, the statement hasn't been prepared yet
        let lookupAction = stateMachine.lookup(preparedStatement: firstPreparedStatement)
        guard case .preparing = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        guard case .prepareStatement = lookupAction else {
            XCTFail("State machine returned the wrong action")
            return
        }

        // Once preparation is complete we transition to a prepared state
        let preparationCompleteAction = stateMachine.preparationComplete(name: "test", rowDescription: nil)
        guard case .prepared(nil) = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        XCTAssertEqual(preparationCompleteAction.statements.count, 1)
        XCTAssertNil(preparationCompleteAction.rowDescription)
        firstPreparedStatement.promise.succeed(PSQLRowStream(
            source: .noRows(.success(.tag("tag"))),
            eventLoop: eventLoop,
            logger: .psqlTest
        ))

        // Create a new prepared statement
        let secondPreparedStatement = self.makePreparedStatementContext(eventLoop: eventLoop)
        // The statement is already preparead, lookups tell us to execute it
        let secondLookupAction = stateMachine.lookup(preparedStatement: secondPreparedStatement)
        guard case .prepared(nil) = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        guard case .executeStatement(nil) = secondLookupAction else {
            XCTFail("State machine returned the wrong action")
            return
        }
        secondPreparedStatement.promise.succeed(PSQLRowStream(
            source: .noRows(.success(.tag("tag"))),
            eventLoop: eventLoop,
            logger: .psqlTest
        ))
    }

    func testPrepareAndExecuteStatementWithError() {
        let eventLoop = EmbeddedEventLoop()
        var stateMachine = PreparedStatementStateMachine()

        let firstPreparedStatement = self.makePreparedStatementContext(eventLoop: eventLoop)
        // Initial lookup, the statement hasn't been prepared yet
        let lookupAction = stateMachine.lookup(preparedStatement: firstPreparedStatement)
        guard case .preparing = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        guard case .prepareStatement = lookupAction else {
            XCTFail("State machine returned the wrong action")
            return
        }

        // Simulate an error occurring during preparation
        let error = PostgresError(code: .server)
        let preparationCompleteAction = stateMachine.errorHappened(
            name: "test",
            error: error
        )
        guard case .error = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        XCTAssertEqual(preparationCompleteAction.statements.count, 1)
        firstPreparedStatement.promise.fail(error)

        // Create a new prepared statement
        let secondPreparedStatement = self.makePreparedStatementContext(eventLoop: eventLoop)
        // Ensure that we don't try again to prepare a statement we know will fail
        let secondLookupAction = stateMachine.lookup(preparedStatement: secondPreparedStatement)
        guard case .error = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        guard case .returnError = secondLookupAction else {
            XCTFail("State machine returned the wrong action")
            return
        }
        secondPreparedStatement.promise.fail(error)
    }

    func testBatchStatementPreparation() {
        let eventLoop = EmbeddedEventLoop()
        var stateMachine = PreparedStatementStateMachine()

        let firstPreparedStatement = self.makePreparedStatementContext(eventLoop: eventLoop)
        // Initial lookup, the statement hasn't been prepared yet
        let lookupAction = stateMachine.lookup(preparedStatement: firstPreparedStatement)
        guard case .preparing = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        guard case .prepareStatement = lookupAction else {
            XCTFail("State machine returned the wrong action")
            return
        }

        // A new request comes in before the statement completes
        let secondPreparedStatement = self.makePreparedStatementContext(eventLoop: eventLoop)
        let secondLookupAction = stateMachine.lookup(preparedStatement: secondPreparedStatement)
        guard case .preparing = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        guard case .waitForAlreadyInFlightPreparation = secondLookupAction else {
            XCTFail("State machine returned the wrong action")
            return
        }

        // Once preparation is complete we transition to a prepared state.
        // The action tells us to execute both the pending statements.
        let preparationCompleteAction = stateMachine.preparationComplete(name: "test", rowDescription: nil)
        guard case .prepared(nil) = stateMachine.preparedStatements["test"] else {
            XCTFail("State machine in the wrong state")
            return
        }
        XCTAssertEqual(preparationCompleteAction.statements.count, 2)
        XCTAssertNil(preparationCompleteAction.rowDescription)

        firstPreparedStatement.promise.succeed(PSQLRowStream(
            source: .noRows(.success(.tag("tag"))),
            eventLoop: eventLoop,
            logger: .psqlTest
        ))
        secondPreparedStatement.promise.succeed(PSQLRowStream(
            source: .noRows(.success(.tag("tag"))),
            eventLoop: eventLoop,
            logger: .psqlTest
        ))
    }

    private func makePreparedStatementContext(eventLoop: EmbeddedEventLoop) -> PreparedStatementContext {
        let promise = eventLoop.makePromise(of: PSQLRowStream.self)
        return PreparedStatementContext(
            name: "test",
            sql: "INSERT INTO test_table (column1) VALUES (1)",
            bindings: PostgresBindings(),
            bindingDataTypes: [],
            logger: .psqlTest,
            promise: promise
        )
    }
}
