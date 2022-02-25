import NIOCore

extension PostgresCodable where Self: RawRepresentable, RawValue: PostgresCodable {
    var psqlType: PostgresDataType {
        self.rawValue.psqlType
    }
    
    var psqlFormat: PostgresFormat {
        self.rawValue.psqlFormat
    }
    
    static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        guard let rawValue = try? RawValue.decode(from: &buffer, type: type, format: format, context: context),
              let selfValue = Self.init(rawValue: rawValue) else {
            throw PostgresCastingError.Code.failure
        }
        
        return selfValue
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        try rawValue.encode(into: &byteBuffer, context: context)
    }
}
