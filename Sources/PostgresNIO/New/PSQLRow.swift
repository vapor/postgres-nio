import NIOCore
import Foundation

/// `PSQLRow` represents a single row that was received from the Postgres Server.
public struct PSQLRow {
    @usableFromInline
    internal let lookupTable: [String: Int]
    @usableFromInline
    internal let data: DataRow

    @usableFromInline
    internal let columns: [RowDescription.Column]
    
    internal init(data: DataRow, lookupTable: [String: Int], columns: [RowDescription.Column]) {
        self.data = data
        self.lookupTable = lookupTable
        self.columns = columns
    }
}

extension PSQLRow: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        lhs.data == rhs.data && lhs.columns == rhs.columns
    }
}
