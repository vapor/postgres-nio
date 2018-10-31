public protocol PostgresDataConvertible {
    #warning("make this nil-able or throwing?")
    init(postgresData: PostgresData)
    var postgresData: PostgresData { get }
}
