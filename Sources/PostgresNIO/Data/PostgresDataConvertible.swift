import Foundation

public protocol PostgresDataConvertible {
    static var postgresDataType: PostgresDataType { get }
    init?(postgresData: PostgresData)
    var postgresData: PostgresData? { get }
}

public protocol PostgresEncoder {
    func encode(_ value: Encodable) throws -> PostgresData
}

public protocol PostgresDecoder {
    func decode<T>(_ type: T.Type, from data: PostgresData) throws -> T where T: Decodable
}
