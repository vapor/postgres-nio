import Foundation

extension PostgresData {
    public var decimal: Decimal? {
        guard let string = self.string else {
            return nil
        }
        guard let decimal = Decimal(string: string) else {
            return nil
        }
        return decimal
    }

    public init(decimal: Decimal) {
        self.init(string: decimal.description)
    }
}
