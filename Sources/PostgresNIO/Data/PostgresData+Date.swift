import Foundation
import NIO

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
            guard let string = value.readString(length: value.readableBytes) else {
                return nil
            }
            guard string.count >= 10 && string.count <= 32 else {
                // Shortest has format: yyyy-mm-dd
                // Longest has format: yyyy-mm-dd hh:mm:ss.123456+12:45
                return nil
            }
            return PostgresData.convertPostgresStringToDate(string)
        case .binary:
            switch self.type {
            case .timestamp, .timestamptz:
                let microseconds = value.readInteger(as: Int64.self)!
                let seconds = Double(microseconds) / Double(_microsecondsPerSecond)
                return Date(timeInterval: seconds, since: _psqlDateStart)
            case .time, .timetz:
                return nil
            case .date:
                let days = value.readInteger(as: Int32.self)!
                let seconds = Int64(days) * _secondsInDay
                return Date(timeInterval: Double(seconds), since: _psqlDateStart)
            default:
                return nil
            }
        }
    }
    
    class CalendarContainer {
        let calendar = Calendar(identifier: .iso8601)
    }
    
    private static var calendar: ThreadSpecificVariable<CalendarContainer> = .init()
    
    static var threadSpecificCalendar:Calendar {
        let container:CalendarContainer
        if let existing = PostgresData.calendar.currentValue {
            container = existing
        } else {
            container = CalendarContainer()
            self.calendar.currentValue = container
        }
        return container.calendar
    }
    
    /*
     This regular expression is comprised of three sub-phrases
     
         Date: (?<year>[0-9]{4})-(?<month>[0-9]{2})-(?<day>[0-9]{2})
         Time: (?<hour>[0-9]{2}):(?<min>[0-9]{2}):(?<sec>[0-9]{2})(?<micro>\\.[0-9]{1,6})?
         TZ: (?<tzhr>[-+][0-9]{1,2})(?:[:](?<tzmin>[0-9]{2}))?

     that enforce the format for each component. These are combined in the expression

       ^Date(?: Time(?:TZ)?)?$

     Here the incoming string must start with a date and can be followed by an optional time.
     Only if the time is present can an optional timezone appear.
     */
    private static let regex = try! NSRegularExpression(pattern:
         "^(?<year>[0-9]{4})-(?<month>[0-9]{2})-(?<day>[0-9]{2})(?: (?<hour>[0-9]{2}):(?<min>[0-9]{2}):(?<sec>[0-9]{2})(?<micro>\\.[0-9]{1,6})?(?:(?<tzhr>[-+][0-9]{1,2})(?:[:](?<tzmin>[0-9]{2}))?)?)?$")

    private static func convertPostgresStringToDate(_ string:String) -> Date? {
        var year:Int?
        var month:Int?
        var day:Int?
        var hour:Int?
        var minute:Int?
        var second:Int?
        var nano:Int?
        var minutesFromGMT:Int = 0

        if let match = PostgresData.regex.firstMatch(in: string, range: NSRange(location: 0, length: string.count)) {
            if let yearRange = Range(match.range(withName: "year"), in: string) {
                year = Int(string[yearRange])
            }
            if let monthRange = Range(match.range(withName: "month"), in: string) {
                month = Int(string[monthRange])
            }
            if let dayRange = Range(match.range(withName: "day"), in: string) {
                day = Int(string[dayRange])
            }
            
            if let hourRange = Range(match.range(withName: "hour"), in: string) {
                hour = Int(string[hourRange])
                
                if let minuteRange = Range(match.range(withName: "min"), in: string) {
                    minute = Int(string[minuteRange])
                }
                if let secondRange = Range(match.range(withName: "sec"), in: string) {
                    second = Int(string[secondRange])
                }
                if let microRange = Range(match.range(withName: "micro"), in: string) {
                    if let micro = Float(string[microRange]) {
                        nano = Int(micro * 1e9)
                    }
                }
            }
                    
            if let tzhrRange = Range(match.range(withName: "tzhr"), in: string) {
                if let tzhr = Int(string[tzhrRange]) {
                    minutesFromGMT = 60*tzhr
                    
                    if let tzminRange = Range(match.range(withName: "tzmin"), in: string) {
                        if let tzmin = Int(string[tzminRange]) {
                            if minutesFromGMT > 0 { minutesFromGMT += tzmin }
                            else { minutesFromGMT -= tzmin }
                        }
                    }
                }
            }
        
            let date = DateComponents(calendar: threadSpecificCalendar, timeZone: TimeZone(secondsFromGMT: 0), year: year, month: month, day: day, hour: hour, minute: minute, second: second, nanosecond: nano).date
            return date?.advanced(by: TimeInterval(minutesFromGMT * -60))
        }
        
        return nil
    }
}

extension Date: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .timestamptz
    }

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
