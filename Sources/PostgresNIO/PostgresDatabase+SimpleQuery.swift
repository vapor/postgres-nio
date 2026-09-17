import Logging
import NIOConcurrencyHelpers
import NIOCore

extension PostgresDatabase {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        let rowsBoxed = NIOLockedValueBox([PostgresRow]())
        return self.simpleQuery(string) { row in
            rowsBoxed.withLockedValue {
                $0.append(row)
            }
        }.map { rowsBoxed.withLockedValue { $0 } }
    }

    @preconcurrency
    public func simpleQuery(_ string: String, _ onRow: @Sendable @escaping (PostgresRow) throws -> Void) -> EventLoopFuture<Void> {
        self.query(string, onRow: onRow)
    }
}
