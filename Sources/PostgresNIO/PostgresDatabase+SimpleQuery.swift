import NIOCore
import Logging

extension PostgresDatabase {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        let rowsBoxed = NIOLoopBoundBox([PostgresRow](), eventLoop: self.eventLoop)
        return self.simpleQuery(string) {
            var rows = rowsBoxed.value
            rowsBoxed.value = [] // prevent CoW
            rows.append($0)
            rowsBoxed.value = rows
        }.map { rowsBoxed.value }
    }
    
    @preconcurrency
    public func simpleQuery(_ string: String, _ onRow: @Sendable @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        self.query(string, onRow: onRow)
    }
}
