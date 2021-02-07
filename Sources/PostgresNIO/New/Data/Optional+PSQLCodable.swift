extension Optional: PSQLDecodable where Wrapped: PSQLDecodable {
    static func decode(from byteBuffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Optional<Wrapped> {
        preconditionFailure("This code path should never be hit.")
        // The code path for decoding an optional should be:
        //  -> PSQLData.decode(as: String?.self)
        //       -> PSQLData.decodeIfPresent(String.self)
        //            -> String.decode(from: type:)
    }
}

extension Optional: PSQLEncodable where Wrapped: PSQLEncodable {
    var psqlType: PSQLDataType {
        switch self {
        case .some(let value):
            return value.psqlType
        case .none:
            return .null
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        switch self {
        case .none:
            return
        case .some(let value):
            try value.encode(into: &byteBuffer, context: context)
        }
    }
}

extension Optional: PSQLCodable where Wrapped: PSQLCodable {
    
}
