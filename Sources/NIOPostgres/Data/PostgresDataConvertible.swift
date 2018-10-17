public protocol PostgresDataConvertible {
    init(postgresData: PostgresData)
    var postgresData: PostgresData { get }
}
