import Foundation

public protocol PostgresDataConvertible {
    static var postgresDataType: PostgresDataType { get }
    init?(postgresData: PostgresData)
    var postgresData: PostgresData? { get }
}
