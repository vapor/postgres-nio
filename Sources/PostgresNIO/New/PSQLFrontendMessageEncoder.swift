
struct PSQLFrontendMessageEncoder: MessageToByteEncoder {
    typealias OutboundIn = PSQLFrontendMessage
    
    let jsonEncoder: PostgresJSONEncoder
    
    init(jsonEncoder: PostgresJSONEncoder) {
        self.jsonEncoder = jsonEncoder
    }
    
    func encode(data message: PSQLFrontendMessage, out buffer: inout ByteBuffer) throws {
        switch message {
        case .bind(let bind):
            buffer.writeInteger(message.id.rawValue)
            let startIndex = buffer.writerIndex
            buffer.writeInteger(Int32(0)) // placeholder for length
            try bind.encode(into: &buffer, using: self.jsonEncoder)
            let length = Int32(buffer.writerIndex - startIndex)
            buffer.setInteger(length, at: startIndex)
            
        case .cancel(let cancel):
            // cancel requests don't have an identifier
            self.encode(payload: cancel, into: &buffer)
            
        case .close(let close):
            self.encode(messageID: message.id, payload: close, into: &buffer)

        case .describe(let describe):
            self.encode(messageID: message.id, payload: describe, into: &buffer)

        case .execute(let execute):
            self.encode(messageID: message.id, payload: execute, into: &buffer)

        case .flush:
            self.encode(messageID: message.id, payload: EmptyPayload(), into: &buffer)

        case .parse(let parse):
            self.encode(messageID: message.id, payload: parse, into: &buffer)

        case .password(let password):
            self.encode(messageID: message.id, payload: password, into: &buffer)

        case .saslInitialResponse(let saslInitialResponse):
            self.encode(messageID: message.id, payload: saslInitialResponse, into: &buffer)

        case .saslResponse(let saslResponse):
            self.encode(messageID: message.id, payload: saslResponse, into: &buffer)

        case .sslRequest(let request):
            // sslRequests don't have an identifier
            self.encode(payload: request, into: &buffer)
            
        case .startup(let startup):
            // startup requests don't have an identifier
            self.encode(payload: startup, into: &buffer)
            
        case .sync:
            self.encode(messageID: message.id, payload: EmptyPayload(), into: &buffer)

        case .terminate:
            self.encode(messageID: message.id, payload: EmptyPayload(), into: &buffer)
        }
    }
    
    private struct EmptyPayload: PSQLMessagePayloadEncodable {
        func encode(into buffer: inout ByteBuffer) {}
    }
    
    private func encode<Payload: PSQLMessagePayloadEncodable>(
        messageID: PSQLFrontendMessage.ID,
        payload: Payload,
        into buffer: inout ByteBuffer)
    {
        buffer.psqlWriteFrontendMessageID(messageID)
        self.encode(payload: payload, into: &buffer)
    }
    
    private func encode<Payload: PSQLMessagePayloadEncodable>(
        payload: Payload,
        into buffer: inout ByteBuffer)
    {
        let startIndex = buffer.writerIndex
        buffer.writeInteger(Int32(0)) // placeholder for length
        payload.encode(into: &buffer)
        let length = Int32(buffer.writerIndex - startIndex)
        buffer.setInteger(length, at: startIndex)
    }
}
