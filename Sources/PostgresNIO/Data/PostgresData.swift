#if swift(>=5.6)
@preconcurrency import NIOCore
#else
import NIOCore
#endif
import Foundation

public struct PostgresData: CustomStringConvertible, CustomDebugStringConvertible {
    public static var null: PostgresData {
        return .init(type: .null)
    }
    
    /// The object ID of the field's data type.
    public var type: PostgresDataType
    
    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    public var typeModifier: Int32?
    
    /// The format code being used for the field.
    /// Currently will be zero (text) or one (binary).
    /// In a RowDescription returned from the statement variant of Describe,
    /// the format code is not yet known and will always be zero.
    public var formatCode: PostgresFormat
    
    public var value: ByteBuffer?
    
    public init(type: PostgresDataType, typeModifier: Int32? = nil, formatCode: PostgresFormat = .binary, value: ByteBuffer? = nil) {
        self.type = type
        self.typeModifier = typeModifier
        self.formatCode = formatCode
        self.value = value
    }
    
    public var description: String {
        guard var value = self.value else {
            return "<null>"
        }
        let description: String?

        switch self.type {
        case .bool:
            description = self.bool?.description
        case .float4:
            description = self.float?.description
        case .float8, .numeric:
            description = self.double?.description
        case .int2:
            description = self.int16?.description
        case .int4, .regproc, .oid:
            description = self.int32?.description
        case .int8:
            description = self.int64?.description
        case .timestamp, .timestamptz, .date, .time, .timetz:
            description = self.date?.description
        case .text:
            description = self.string?.debugDescription
        case .uuid:
            description = self.uuid?.description
        case .json:
            description = String(decoding: value.readableBytesView, as: UTF8.self)
        case .jsonb:
            var value = value
            value.moveReaderIndex(forwardBy: 1)
            description = String(decoding: value.readableBytesView, as: UTF8.self)
        case .uuidArray:
            description = self.array(of: UUID.self)?.description
        case .int8Array:
            description = self.array(of: Int.self)?.description
        case .float8Array:
            description = self.array(of: Double.self)?.description
        case .float4Array:
            description = self.array(of: Float.self)?.description
        case .textArray:
            description = self.array(of: String.self)?.description
        case .jsonbArray:
            description = self.array?.description
        default:
            if self.type.isUserDefined {
                // custom type
                description = value.readString(length: value.readableBytes)
            } else {
                description = nil
            }
        }

        if let description = description {
            return description
        } else {
            let raw: String
            switch self.formatCode {
            case .text:
                raw = (value.readString(length: value.readableBytes) ?? "")
                    .debugDescription
            case .binary:
                raw = "0x" + value.readableBytesView.hexdigest()
            }
            return "\(raw) (\(self.type))"
        }
    }

    public var debugDescription: String {
        return self.description
    }
}

extension PostgresData: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        fatalError("PostgresData cannot be statically represented as a single data type")
    }

    public init?(postgresData: PostgresData) {
        self = postgresData
    }

    public var postgresData: PostgresData? {
        return self
    }
}

#if swift(>=5.6)
extension PostgresData: Sendable {}
#endif
