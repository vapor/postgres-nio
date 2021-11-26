import NIOCore

extension PSQLBackendMessage {
    
    struct BackendKeyData: PayloadDecodable, Equatable {
        let processID: Int32
        let secretKey: Int32
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let (processID, secretKey) = buffer.readMultipleIntegers(endianness: .big, as: (Int32, Int32).self) else {
                throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(8, actual: buffer.readableBytes)
            }

            return .init(processID: processID, secretKey: secretKey)
        }
    }
}

extension PSQLBackendMessage.BackendKeyData: CustomDebugStringConvertible {
    var debugDescription: String {
        "processID: \(processID), secretKey: \(secretKey)"
    }
}
