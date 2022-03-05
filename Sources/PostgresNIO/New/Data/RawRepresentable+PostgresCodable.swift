import NIOCore

extension PostgresEncodable where Self: RawRepresentable, RawValue: PostgresEncodable {
    var psqlType: PostgresDataType {
        self.rawValue.psqlType
    }
    
    var psqlFormat: PostgresFormat {
        self.rawValue.psqlFormat
    }
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try rawValue.encode(into: &byteBuffer, context: context)
    }
}

extension PostgresDecodable where Self: RawRepresentable, RawValue: PostgresDecodable, RawValue._DecodableType == RawValue {
    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard let rawValue = try? RawValue(from: &buffer, type: type, format: format, context: context),
              let selfValue = Self.init(rawValue: rawValue) else {
            throw PostgresCastingError.Code.failure
        }

        self = selfValue
    }
}

extension PostgresCodable where Self: RawRepresentable, RawValue: PostgresCodable, RawValue._DecodableType == RawValue {}
