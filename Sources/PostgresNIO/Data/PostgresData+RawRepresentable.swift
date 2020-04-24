extension RawRepresentable where Self.RawValue: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        RawValue.postgresDataType
    }

    public init?(postgresData: PostgresData) {
        guard let rawValue = RawValue.init(postgresData: postgresData) else {
            return nil
        }
        self.init(rawValue: rawValue)
    }

    public var postgresData: PostgresData? {
        self.rawValue.postgresData
    }
}
