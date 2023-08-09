import NIOCore

struct PreparedStatementStateMachine {
    enum State {
        case preparing([PreparedStatementContext])
        case prepared(RowDescription?)
        case error(PSQLError)
    }
    
    enum Action {
        case prepareStatement
        case waitForAlreadyInFlightPreparation
        case executePendingStatements([PreparedStatementContext], RowDescription?)
        case returnError([PreparedStatementContext], PSQLError)
    }
    
    var preparedStatements: [String: State]
    
    init() {
        self.preparedStatements = [:]
    }
    
    mutating func lookup(name: String, context: PreparedStatementContext) -> Action {
        if let state = self.preparedStatements[name] {
            switch state {
            case .preparing(var statements):
                statements.append(context)
                self.preparedStatements[name] = .preparing(statements)
                return .waitForAlreadyInFlightPreparation
            case .prepared(let rowDescription):
                return .executePendingStatements([context], rowDescription)
            case .error(let error):
                return .returnError([context], error)
            }
        } else {
            self.preparedStatements[name] = .preparing([context])
            return .prepareStatement
        }
    }
    
    mutating func preparationComplete(
        name: String,
        rowDescription: RowDescription?
    ) -> Action {
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

    mutating func errorHappened(name: String, error: PSQLError) -> Action {
        guard case .preparing(let statements) = self.preparedStatements[name] else {
            preconditionFailure("Preparation completed for an unexpected statement")
        }
        self.preparedStatements[name] = .error(error)
        return .returnError(statements, error)
    }
}
