import Foundation

extension PostgresData {
    public init(date: Date) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        let seconds = date.timeIntervalSince(_psqlDateStart) * Double(_microsecondsPerSecond)
        buffer.writeInteger(Int64(seconds))
        self.init(type: .timestamptz, value: buffer)
    }
    
    public var date: Date? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .text:
            return nil
        case .binary:
            switch self.type {
            case .timestamp, .timestamptz:
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

extension Date: PostgresDataCustomConvertible {
    public init?(postgresData: PostgresData) {
        guard let date = postgresData.date else {
            return nil
        }
        self = date
    }
    
    public var postgresData: PostgresData? {
        return .init(date: self)
    }
}

// MARK: Private
private let _microsecondsPerSecond: Int64 = 1_000_000
private let _secondsInDay: Int64 = 24 * 60 * 60
private let _psqlDateStart = Date(timeIntervalSince1970: 946_684_800) // values are stored as seconds before or after midnight 2000-01-01
