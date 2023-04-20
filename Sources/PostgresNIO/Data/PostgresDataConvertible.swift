import Foundation

@available(*, deprecated, message: "This protocol is going to be replaced with ``PostgresEncodable`` and ``PostgresDecodable``")
public protocol PostgresDataConvertible {
    static var postgresDataType: PostgresDataType { get }
    init?(postgresData: PostgresData)
    var postgresData: PostgresData? { get }
}
