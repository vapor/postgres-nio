//
//  File.swift
//  
//
//  Created by Fabian Fett on 07.01.21.
//

extension PSQLBackendMessage {
    enum TransactionState: PayloadDecodable, RawRepresentable {
        typealias RawValue = UInt8
        
        case idle
        case inTransaction
        case inFailedTransaction
        
        init?(rawValue: UInt8) {
            switch rawValue {
            case UInt8(ascii: "I"):
                self = .idle
            case UInt8(ascii: "T"):
                self = .inTransaction
            case UInt8(ascii: "E"):
                self = .inFailedTransaction
            default:
                return nil
            }
        }

        var rawValue: Self.RawValue {
            switch self {
            case .idle:
                return UInt8(ascii: "I")
            case .inTransaction:
                return UInt8(ascii: "T")
            case .inFailedTransaction:
                return UInt8(ascii: "E")
            }
        }
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard buffer.readableBytes == 1 else {
                throw PartialDecodingError.expectedExactlyNRemainingBytes(1, actual: buffer.readableBytes)
            }
            
            // Exactly one byte is readable. For this reason, we can force unwrap the UInt8 below
            let value = buffer.readInteger(as: UInt8.self)!
            guard let state = Self.init(rawValue: value) else {
                throw PartialDecodingError.valueNotRawRepresentable(value: value, asType: TransactionState.self)
            }
            
            return state
        }
    }
}

extension PSQLBackendMessage.TransactionState: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .idle:
            return ".idle"
        case .inTransaction:
            return ".inTransaction"
        case .inFailedTransaction:
            return ".inFailedTransaction"
        }
    }
}
