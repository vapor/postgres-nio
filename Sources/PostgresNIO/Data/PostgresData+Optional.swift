extension Optional: PostgresDataConvertible where Wrapped: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return Wrapped.postgresDataType
    }

    public init?(postgresData: PostgresData) {
        self = Wrapped.init(postgresData: postgresData)
    }

    public var postgresData: PostgresData? {
        switch self {
        case .some(let wrapped):
            return wrapped.postgresData
        case .none:
            return PostgresData(type: Wrapped.postgresDataType)
        }
    }
}
