import Foundation

public protocol PostgresDataConvertible {
    init?(postgresData: PostgresData)
    var postgresData: PostgresData? { get }
}

public protocol CustomPostgresDecodable {
    static func decode(from decoder: PostgresDataDecoder) throws -> Self
}

extension Decimal: CustomPostgresDecodable {
    public static func decode(from decoder: PostgresDataDecoder) throws -> Decimal {
        #warning("fix ! and use more optimized algorithm")
        let string = try String(from: decoder)
        return Decimal(string: string)!
    }
}

extension UUID: CustomPostgresDecodable {
    public static func decode(from decoder: PostgresDataDecoder) throws -> UUID {
        guard var value = decoder.data.value else {
            fatalError()
        }
        return value.readUUID()!
    }
}
