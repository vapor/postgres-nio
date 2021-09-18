import NIOCore
@testable import PostgresNIO

struct PSQLBackendMessageEncoder: MessageToByteEncoder {
    typealias OutboundIn = PSQLBackendMessage

    /// Called once there is data to encode.
    ///
    /// - parameters:
    ///     - data: The data to encode into a `ByteBuffer`.
    ///     - out: The `ByteBuffer` into which we want to encode.
    func encode(data message: PSQLBackendMessage, out buffer: inout ByteBuffer) throws {
        switch message {
        case .authentication(let authentication):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(authentication, into: &buffer)
            
        case .backendKeyData(let keyData):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(keyData, into: &buffer)
            
        case .bindComplete,
             .closeComplete,
             .emptyQueryResponse,
             .noData,
             .parseComplete,
             .portalSuspended:
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(EmptyPayload(), into: &buffer)
            
        case .commandComplete(let string):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(StringPayload(string), into: &buffer)
            
        case .dataRow(let row):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(row, into: &buffer)
            
        case .error(let errorResponse):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(errorResponse, into: &buffer)
            
        case .notice(let noticeResponse):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(noticeResponse, into: &buffer)
            
        case .notification(let notificationResponse):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(notificationResponse, into: &buffer)
            
        case .parameterDescription(let description):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(description, into: &buffer)
            
        case .parameterStatus(let status):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(status, into: &buffer)
            
        case .readyForQuery(let transactionState):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(transactionState, into: &buffer)
            
        case .rowDescription(let description):
            buffer.writeBackendMessageID(message.id)
            self.encodePayload(description, into: &buffer)
            
        case .sslSupported:
            buffer.writeInteger(UInt8(ascii: "S"))
            
        case .sslUnsupported:
            buffer.writeInteger(UInt8(ascii: "N"))
        }
    }
    
    private struct EmptyPayload: PSQLMessagePayloadEncodable {
        func encode(into buffer: inout ByteBuffer) {}
    }
    
    private struct StringPayload: PSQLMessagePayloadEncodable {
        var string: String
        init(_ string: String) { self.string = string }
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(self.string)
        }
    }

    private func encodePayload<Payload: PSQLMessagePayloadEncodable>(_ payload: Payload, into buffer: inout ByteBuffer) {
        let startIndex = buffer.writerIndex
        buffer.writeInteger(Int32(0)) // placeholder for length
        payload.encode(into: &buffer)
        let length = Int32(buffer.writerIndex - startIndex)
        buffer.setInteger(length, at: startIndex)
    }
}

extension PSQLBackendMessage {
    var id: ID {
        switch self {
        case .authentication:
            return .authentication
        case .backendKeyData:
            return .backendKeyData
        case .bindComplete:
            return .bindComplete
        case .closeComplete:
            return .closeComplete
        case .commandComplete:
            return .commandComplete
        case .dataRow:
            return .dataRow
        case .emptyQueryResponse:
            return .emptyQueryResponse
        case .error:
            return .error
        case .noData:
            return .noData
        case .notice:
            return .noticeResponse
        case .notification:
            return .notificationResponse
        case .parameterDescription:
            return .parameterDescription
        case .parameterStatus:
            return .parameterStatus
        case .parseComplete:
            return .parseComplete
        case .portalSuspended:
            return .portalSuspended
        case .readyForQuery:
            return .readyForQuery
        case .rowDescription:
            return .rowDescription
        case .sslSupported,
             .sslUnsupported:
            preconditionFailure("Message has no id.")
        }
    }
}

extension PSQLBackendMessage.Authentication: PSQLMessagePayloadEncodable {
    
    public func encode(into buffer: inout ByteBuffer) {
        switch self {
        case .ok:
            buffer.writeInteger(Int32(0))
            
        case .kerberosV5:
            buffer.writeInteger(Int32(2))
            
        case .plaintext:
            buffer.writeInteger(Int32(3))
            
        case .md5(salt: let salt):
            buffer.writeInteger(Int32(5))
            buffer.writeInteger(salt.0)
            buffer.writeInteger(salt.1)
            buffer.writeInteger(salt.2)
            buffer.writeInteger(salt.3)
            
        case .scmCredential:
            buffer.writeInteger(Int32(6))
            
        case .gss:
            buffer.writeInteger(Int32(7))
            
        case .gssContinue(var data):
            buffer.writeInteger(Int32(8))
            buffer.writeBuffer(&data)
            
        case .sspi:
            buffer.writeInteger(Int32(9))
            
        case .sasl(names: let names):
            buffer.writeInteger(Int32(10))
            for name in names {
                buffer.writeNullTerminatedString(name)
            }
            
        case .saslContinue(data: var data):
            buffer.writeInteger(Int32(11))
            buffer.writeBuffer(&data)
            
        case .saslFinal(data: var data):
            buffer.writeInteger(Int32(12))
            buffer.writeBuffer(&data)
        }
    }
    
}

extension PSQLBackendMessage.BackendKeyData: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        buffer.writeInteger(self.processID)
        buffer.writeInteger(self.secretKey)
    }
}

extension PSQLBackendMessage.DataRow: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        buffer.writeInteger(Int16(self.columns.count))
        
        for column in self.columns {
            switch column {
            case .none:
                buffer.writeInteger(-1, as: Int32.self)
            case .some(var writable):
                buffer.writeInteger(Int32(writable.readableBytes))
                buffer.writeBuffer(&writable)
            }
        }
    }
}

extension PSQLBackendMessage.ErrorResponse: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        for (key, value) in self.fields {
            buffer.writeInteger(key.rawValue, as: UInt8.self)
            buffer.writeNullTerminatedString(value)
        }
        buffer.writeInteger(0, as: UInt8.self) // signal done
    }
}

extension PSQLBackendMessage.NoticeResponse: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        for (key, value) in self.fields {
            buffer.writeInteger(key.rawValue, as: UInt8.self)
            buffer.writeNullTerminatedString(value)
        }
        buffer.writeInteger(0, as: UInt8.self) // signal done
    }
}

extension PSQLBackendMessage.NotificationResponse: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        buffer.writeInteger(self.backendPID)
        buffer.writeNullTerminatedString(self.channel)
        buffer.writeNullTerminatedString(self.payload)
    }
}

extension PSQLBackendMessage.ParameterDescription: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        buffer.writeInteger(Int16(self.dataTypes.count))
        
        for dataType in self.dataTypes {
            buffer.writeInteger(dataType.rawValue)
        }
    }
}

extension PSQLBackendMessage.ParameterStatus: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        buffer.writeNullTerminatedString(self.parameter)
        buffer.writeNullTerminatedString(self.value)
    }
}

extension PSQLBackendMessage.TransactionState: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        buffer.writeInteger(self.rawValue)
    }
}

extension PSQLBackendMessage.RowDescription: PSQLMessagePayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) {
        buffer.writeInteger(Int16(self.columns.count))
        
        for column in self.columns {
            buffer.writeNullTerminatedString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(column.format.rawValue)
        }
    }
}
