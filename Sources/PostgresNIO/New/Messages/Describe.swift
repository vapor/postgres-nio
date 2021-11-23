import NIOCore

extension PSQLFrontendMessage {
    
    enum Describe: PSQLMessagePayloadEncodable, Equatable {
        
        case preparedStatement(String)
        case portal(String)
        
        func encode(into buffer: inout ByteBuffer) {
            switch self {
            case .preparedStatement(let name):
                buffer.writeInteger(UInt8(ascii: "S"))
                buffer.psqlWriteNullTerminatedString(name)
            case .portal(let name):
                buffer.writeInteger(UInt8(ascii: "P"))
                buffer.psqlWriteNullTerminatedString(name)
            }
        }
    }
}
