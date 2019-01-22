import Foundation

public protocol PostgresDataCustomConvertible {
    init?(postgresData: PostgresData)
    var postgresData: PostgresData? { get }
}

extension PostgresData {
    public init?<T>(custom: T)
        where T: PostgresDataCustomConvertible
    {
        guard let data = custom.postgresData else {
            return nil
        }
        self = data
    }
    
    public func `as`<T>(custom type: T.Type) -> T?
        where T: PostgresDataCustomConvertible
    {
        return T(postgresData: self)
    }
}

extension Decimal: PostgresDataCustomConvertible {
    public init?(postgresData: PostgresData) {
        guard let string = postgresData.string else {
            return nil
        }
        guard let decimal = Decimal(string: string) else {
            return nil
        }
        
        self = decimal
    }
    
    public var postgresData: PostgresData? {
        return .init(string: self.description)
    }
}
