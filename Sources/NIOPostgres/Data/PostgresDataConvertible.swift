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

extension Date: CustomPostgresDecodable {
    public static func decode(from decoder: PostgresDataDecoder) throws -> Date {
        guard var value = decoder.data.value else {
            fatalError()
        }
        switch decoder.data.formatCode {
        case .text: fatalError()
//            switch data.type {
//            case .timestamp: return try value.parseDate(format:  "yyyy-MM-dd HH:mm:ss")
//            case .date: return try value.parseDate(format:  "yyyy-MM-dd")
//            case .time: return try value.parseDate(format:  "HH:mm:ss")
//            default: throw PostgreSQLError.decode(Date.self, from: data)
//            }
        case .binary:
            switch decoder.data.type {
            case .timestamp, .timestamptz:
                #warning("fix !")
                let microseconds = value.readInteger(as: Int64.self)!
                let seconds = Double(microseconds) / Double(_microsecondsPerSecond)
                return Date(timeInterval: seconds, since: _psqlDateStart)
            case .time, .timetz: fatalError()
            case .date:
                let days = value.readInteger(as: Int32.self)!
                let seconds = Int64(days) * _secondsInDay
                return Date(timeInterval: Double(seconds), since: _psqlDateStart)
            default: fatalError()
            }
        }
    }
}

// MARK: Private
private let _microsecondsPerSecond: Int64 = 1_000_000
private let _secondsInDay: Int64 = 24 * 60 * 60
private let _psqlDateStart = Date(timeIntervalSince1970: 946_684_800) // values are stored as seconds before or after midnight 2000-01-01

//private extension String {
//    /// Parses a Date from this string with the supplied date format.
//    func parseDate(format: String) throws -> Date {
//        let formatter = DateFormatter()
//        if contains(".") {
//            formatter.dateFormat = format + ".SSSSSS"
//        } else {
//            formatter.dateFormat = format
//        }
//        formatter.timeZone = TimeZone(secondsFromGMT: 0)
//        guard let date = formatter.date(from: self) else {
//            throw PostgreSQLError(identifier: "date", reason: "Malformed date: \(self)")
//        }
//        return date
//    }
//}
