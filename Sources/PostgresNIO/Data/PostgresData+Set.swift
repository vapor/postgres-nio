extension Set: PostgresDataConvertible where Element: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        [Element].postgresDataType
    }

    public init?(postgresData: PostgresData) {
        guard let array = [Element](postgresData: postgresData) else {
            return nil
        }
        self = Set(array)
    }

    public var postgresData: PostgresData? {
        [Element](self).postgresData
    }
}
