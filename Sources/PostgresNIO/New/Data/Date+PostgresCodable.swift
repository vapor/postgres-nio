import NIOCore
import struct Foundation.Date

extension Date: PostgresNonThrowingEncodable {
    public static var psqlType: PostgresDataType {
        .timestamptz
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self._psqlMicroseconds)
    }

    /// The number of microseconds between ``_psqlDateStart`` and this `Date`, in PostgreSQL's
    /// timestamp representation.
    ///
    /// This clamps to `MIN_TIMESTAMP` - 1 for underflow and to `END_TIMESTAMP` for overflow,
    /// allowing Postgres to reject the value. We cannot reject it ourselves because 
    /// of the `PostgresNonThrowingEncodable` conformance.
    @usableFromInline
    var _psqlMicroseconds: Int64 {
        let microseconds = self.timeIntervalSince(Self._psqlDateStart) * Double(Self._microsecondsPerSecond)
        guard 
            let exact = Int64(exactly: microseconds.rounded(.towardZero)),
            exact >= Self._minTimestamp, exact < Self._endTimestamp
        else {
            return microseconds < 0 ? Self._minTimestamp - 1 : Self._endTimestamp
        }
        return exact
    }

    // MARK: Private Constants

    @usableFromInline
    static let _microsecondsPerSecond: Int64 = 1_000_000
    @usableFromInline
    static let _secondsInDay: Int64 = 24 * 60 * 60
    /// PostgreSQL's `END_TIMESTAMP`.
    @usableFromInline
    static let _endTimestamp: Int64 = 9_223_371_331_200_000_000
    /// PostgreSQL's `MIN_TIMESTAMP`.
    @usableFromInline
    static let _minTimestamp: Int64 = -211_813_488_000_000_000

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
                throw PostgresDecodingError.Code.failure
            }
            let seconds = Double(microseconds) / Double(Self._microsecondsPerSecond)
            self = Date(timeInterval: seconds, since: Self._psqlDateStart)
        case .date:
            guard buffer.readableBytes == 4, let days = buffer.readInteger(as: Int32.self) else {
                throw PostgresDecodingError.Code.failure
            }
            let seconds = Int64(days) * Self._secondsInDay
            self = Date(timeInterval: Double(seconds), since: Self._psqlDateStart)
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}
