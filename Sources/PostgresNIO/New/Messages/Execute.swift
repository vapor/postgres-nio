import NIOCore

extension PostgresFrontendMessage {
    
    struct Execute: PSQLMessagePayloadEncodable, Equatable {
        /// The name of the portal to execute (an empty string selects the unnamed portal).
        let portalName: String
        
        /// Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
        let maxNumberOfRows: Int32
        
        init(portalName: String, maxNumberOfRows: Int32 = 0) {
            self.portalName = portalName
            self.maxNumberOfRows = maxNumberOfRows
        }
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(self.portalName)
            buffer.writeInteger(self.maxNumberOfRows)
        }
    }
    
}
