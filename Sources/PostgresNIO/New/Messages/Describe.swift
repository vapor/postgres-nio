extension PSQLFrontendMessage {
    
    enum Describe: PayloadEncodable, Equatable {
        
        case preparedStatement(String)
        case portal(String)
        
        func encode(into buffer: inout ByteBuffer) {
            switch self {
            case .preparedStatement(let name):
                buffer.writeInteger(UInt8(ascii: "S"))
                buffer.writeNullTerminatedString(name)
            case .portal(let name):
                buffer.writeInteger(UInt8(ascii: "P"))
                buffer.writeNullTerminatedString(name)
            }
        }
    }
}
