import NIOCore

extension PostgresMessage {
    /// SASL ongoing challenge response message sent by the client.
    @available(*, deprecated, message: "Will be removed from public API")
    public struct SASLResponse {
        public static var identifier: PostgresMessage.Identifier {
            return .saslResponse
        }
        
        public let responseData: [UInt8]
        
        public static func parse(from buffer: inout ByteBuffer) throws -> SASLResponse {
            guard let data = buffer.readBytes(length: buffer.readableBytes) else {
                throw PostgresError.protocol("Could not parse SASL response from response message")
            }
            
            return SASLResponse(responseData: data)
        }
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            buffer.writeBytes(responseData)
        }
        
        public var description: String {
            return "SASLResponse(\(responseData))"
        }
    }
}

extension PostgresMessage {
    /// SASL initial challenge response message sent by the client.
    @available(*, deprecated, message: "Will be removed from public API")
    public struct SASLInitialResponse {
        public let mechanism: String
        public let initialData: [UInt8]
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            buffer.writeNullTerminatedString(self.mechanism)
            if initialData.count > 0 {
                buffer.writeInteger(Int32(initialData.count), as: Int32.self) // write(array:) writes Int16, which is incorrect here
                buffer.writeBytes(initialData)
            } else {
                buffer.writeInteger(-1, as: Int32.self)
            }
        }
        
        public var description: String {
            return "SASLInitialResponse(\(mechanism), data: \(initialData))"
        }
    }
}

@available(*, deprecated, message: "Deprecating conformance to `PostgresMessageType` since it is deprecated.")
extension PostgresMessage.SASLInitialResponse {
    public static var identifier: PostgresMessage.Identifier {
        return .saslInitialResponse
    }

    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        guard let mechanism = buffer.readNullTerminatedString() else {
            throw PostgresError.protocol("Could not parse SASL mechanism from initial response message")
        }
        guard let dataLength = buffer.readInteger(as: Int32.self) else {
            throw PostgresError.protocol("Could not parse SASL initial data length from initial response message")
        }

        var actualData: [UInt8] = []

        if dataLength != -1 {
            guard let data = buffer.readBytes(length: Int(dataLength)) else {
                throw PostgresError.protocol("Could not parse SASL initial data from initial response message")
            }
            actualData = data
        }
        return .init(mechanism: mechanism, initialData: actualData)
    }
}
