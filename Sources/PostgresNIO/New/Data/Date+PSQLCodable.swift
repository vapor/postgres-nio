import NIOCore
import struct Foundation.Date

extension Date: PSQLCodable {
    public var psqlType: PostgresDataType {
        .timestamptz
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        switch type {
        case .timestamp, .timestamptz:
            guard buffer.readableBytes == 8, let microseconds = buffer.readInteger(as: Int64.self) else {
                throw PostgresCastingError.Code.failure
            }
            let seconds = Double(microseconds) / Double(_microsecondsPerSecond)
            return Date(timeInterval: seconds, since: _psqlDateStart)
        case .date:
            guard buffer.readableBytes == 4, let days = buffer.readInteger(as: Int32.self) else {
                throw PostgresCastingError.Code.failure
            }
            let seconds = Int64(days) * _secondsInDay
            return Date(timeInterval: Double(seconds), since: _psqlDateStart)
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) {
        let seconds = self.timeIntervalSince(Self._psqlDateStart) * Double(Self._microsecondsPerSecond)
        buffer.writeInteger(Int64(seconds))
    }
    
    // MARK: Private Constants

    @usableFromInline
    static let _microsecondsPerSecond: Int64 = 1_000_000
    @usableFromInline
    static let _secondsInDay: Int64 = 24 * 60 * 60
    
    /// values are stored as seconds before or after midnight 2000-01-01
    @usableFromInline
    static let _psqlDateStart = Date(timeIntervalSince1970: 946_684_800)
}

