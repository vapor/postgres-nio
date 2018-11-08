import NIO
extension PostgresConnection {
    struct TableNames {
        static func load(on connection: PostgresConnection) -> EventLoopFuture<TableNames> {
            return connection.simpleQuery("SELECT oid, relname FROM pg_class").map { rows in
                // hack to determine the tableOID since we have not yet generated the lookup table
                let tableOID = rows[0].lookupTable.rowDescription.fields[0].tableOID
                
                // generate the lookup table
                var map: [String: UInt32] = [:]
                for row in rows {
                    // decoding should never fail here
                    let oid = try! row.decode(UInt32.self, at: "oid", tableOID: tableOID)!
                    let relname = try! row.decode(String.self, at: "relname", tableOID: tableOID)!
                    map[relname] = oid
                }
                return TableNames(map: map)
            }
        }
        
        private var map: [String: UInt32]
        
        func oid(forName name: String) -> UInt32? {
            return map[name]
        }
    }
    
    public func loadTableNames() -> EventLoopFuture<Void> {
        assert(self.tableNames == nil, "Table names have already been loaded")
        return TableNames.load(on: self).map { tableNames in
            self.tableNames = tableNames
        }
    }
}
