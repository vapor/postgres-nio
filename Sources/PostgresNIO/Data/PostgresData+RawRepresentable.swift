extension RawRepresentable where Self.RawValue: PostgresDataConvertible {
    static var postgresDataType: PostgresDataType {
        RawValue.postgresDataType
    }

    init?(postgresData: PostgresData) {
        guard let rawValue = RawValue.init(postgresData: postgresData) else {
            return nil
        }
        self.init(rawValue: rawValue)
    }

    var postgresData: PostgresData? {
        self.rawValue.postgresData
    }
}
