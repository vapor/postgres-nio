import NIOCore
import struct Foundation.Date

extension Date: PostgresEncodable {
    static var psqlType: PostgresDataType {
        .timestamptz
    }
    
    static var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        let seconds = self.timeIntervalSince(Self._psqlDateStart) * Double(Self._microsecondsPerSecond)
        byteBuffer.writeInteger(Int64(seconds))
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

extension Date: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch type {
        case .timestamp, .timestamptz:
            guard buffer.readableBytes == 8, let microseconds = buffer.readInteger(as: Int64.self) else {
                throw PostgresCastingError.Code.failure
            }
            let seconds = Double(microseconds) / Double(Self._microsecondsPerSecond)
            self = Date(timeInterval: seconds, since: Self._psqlDateStart)
        case .date:
            guard buffer.readableBytes == 4, let days = buffer.readInteger(as: Int32.self) else {
                throw PostgresCastingError.Code.failure
            }
            let seconds = Int64(days) * Self._secondsInDay
            self = Date(timeInterval: Double(seconds), since: Self._psqlDateStart)
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension Date: PostgresCodable {}
