import NIOCore

struct PreparedStatementStateMachine {
    enum State {
        case preparing([PreparedStatementContext])
        case prepared(RowDescription?)
        case error(PSQLError)
    }

    var preparedStatements: [String: State]
    
    init() {
        self.preparedStatements = [:]
    }

    enum LookupAction {
        case prepareStatement
        case waitForAlreadyInFlightPreparation
        case executeStatement(RowDescription?)
        case executePendingStatements([PreparedStatementContext], RowDescription?)
        case returnError([PreparedStatementContext], PSQLError)
    }

    mutating func lookup(name: String, context: PreparedStatementContext) -> LookupAction {
        if let state = self.preparedStatements[name] {
            switch state {
            case .preparing(var statements):
                statements.append(context)
                self.preparedStatements[name] = .preparing(statements)
                return .waitForAlreadyInFlightPreparation
            case .prepared(let rowDescription):
                return .executeStatement(rowDescription)
            case .error(let error):
                return .returnError([context], error)
            }
        } else {
            self.preparedStatements[name] = .preparing([context])
            return .prepareStatement
        }
    }

    enum PreparationCompleteAction {
        case executePendingStatements([PreparedStatementContext], RowDescription?)
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
            return .executePendingStatements(statements, rowDescription)
        } else {
            self.preparedStatements[name] = .prepared(nil)
            return .executePendingStatements(statements, nil)
        }
    }

    enum ErrorHappenedAction {
        case returnError([PreparedStatementContext], PSQLError)
    }
    
    mutating func errorHappened(name: String, error: PSQLError) -> ErrorHappenedAction {
        guard case .preparing(let statements) = self.preparedStatements[name] else {
            preconditionFailure("Preparation completed for an unexpected statement")
        }
        self.preparedStatements[name] = .error(error)
        return .returnError(statements, error)
    }
}
