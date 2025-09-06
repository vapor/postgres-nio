import Foundation
import NIOFoundationCompat
import NIOCore
import NIOConcurrencyHelpers

/// A protocol that mimicks the Foundation `JSONEncoder.encode(_:)` function.
/// Conform a non-Foundation JSON encoder to this protocol if you want PostgresNIO to be
/// able to use it when encoding JSON & JSONB values (see `PostgresNIO._defaultJSONEncoder`)
@preconcurrency
public protocol PostgresJSONEncoder: Sendable {
    func encode<T>(_ value: T) throws -> Data where T : Encodable

    func encode<T: Encodable>(_ value: T, into buffer: inout ByteBuffer) throws
}

extension PostgresJSONEncoder {
    public func encode<T: Encodable>(_ value: T, into buffer: inout ByteBuffer) throws {
        let data = try self.encode(value)
        buffer.writeData(data)
    }
}

extension JSONEncoder: PostgresJSONEncoder {}

private let jsonEncoderLocked: NIOLockedValueBox<any PostgresJSONEncoder> = NIOLockedValueBox(JSONEncoder())

/// The default JSON encoder used by PostgresNIO when encoding JSON & JSONB values.
/// As `_defaultJSONEncoder` will be reused for encoding all JSON & JSONB values
/// from potentially multiple threads at once, you must ensure your custom JSON encoder is
/// thread safe internally like `Foundation.JSONEncoder`.
public var _defaultJSONEncoder: any PostgresJSONEncoder {
    set {
        jsonEncoderLocked.withLockedValue { $0 = newValue }
    }
    get {
        jsonEncoderLocked.withLockedValue { $0 }
    }
}

