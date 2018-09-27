public typealias PostgresDataCodable = PostgresDataDecodable & PostgresDataEncodable

public protocol PostgresDataDecodable {
    static func decode(from data: PostgresData) -> Self?
}

public protocol PostgresDataEncodable {
    func encode(to data: inout PostgresData?)
}
