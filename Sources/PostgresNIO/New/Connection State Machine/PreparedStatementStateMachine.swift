import NIOCore

struct PreparedStatementStateMachine {
    enum State {
        case preparing([PreparedStatementContext])
        case prepared(RowDescription?)
        case error(PSQLError)
    }

    var preparedStatements: [String: State] = [:]
    
    enum LookupAction {
        case prepareStatement
        case waitForAlreadyInFlightPreparation
        case executeStatement(RowDescription?)
        case returnError(PSQLError)
    }

    mutating func lookup(preparedStatement: PreparedStatementContext) -> LookupAction {
        if let state = self.preparedStatements[preparedStatement.name] {
            switch state {
            case .preparing(var statements):
                statements.append(preparedStatement)
                self.preparedStatements[preparedStatement.name] = .preparing(statements)
                return .waitForAlreadyInFlightPreparation
            case .prepared(let rowDescription):
                return .executeStatement(rowDescription)
            case .error(let error):
                return .returnError(error)
            }
        } else {
            self.preparedStatements[preparedStatement.name] = .preparing([preparedStatement])
            return .prepareStatement
        }
    }

    struct PreparationCompleteAction {
        var statements: [PreparedStatementContext]
        var rowDescription: RowDescription?
    }

    mutating func preparationComplete(
        name: String,
        rowDescription: RowDescription?
    ) -> PreparationCompleteAction {
        guard case .preparing(let statements) = self.preparedStatements[name] else {
            preconditionFailure("Preparation completed for an unexpected statement")
        }
        // When sending the bindings we are going to ask for binary data.
        if var rowDescription {
            for i in 0..<rowDescription.columns.count {
                rowDescription.columns[i].format = .binary
            }
            self.preparedStatements[name] = .prepared(rowDescription)
            return PreparationCompleteAction(
                statements: statements,
                rowDescription: rowDescription
            )
        } else {
            self.preparedStatements[name] = .prepared(nil)
            return PreparationCompleteAction(
                statements: statements,
                rowDescription: nil
            )
        }
    }

    struct ErrorHappenedAction {
        var statements: [PreparedStatementContext]
        var error: PSQLError
    }
    
    mutating func errorHappened(name: String, error: PSQLError) -> ErrorHappenedAction {
        guard case .preparing(let statements) = self.preparedStatements[name] else {
            preconditionFailure("Preparation completed for an unexpected statement")
        }
        self.preparedStatements[name] = .error(error)
        return ErrorHappenedAction(
            statements: statements,
            error: error
        )
    }
}
