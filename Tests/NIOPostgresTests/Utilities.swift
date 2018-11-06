import NIO
import NIOPostgres

extension PostgresConnection {
    static func test() -> EventLoopFuture<PostgresConnection> {
        let eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
        do {
            let address: SocketAddress
            #if os(Linux)
            address = try .init(ipAddress: hostname, port: 5432)
            #else
            address = try .newAddressResolving(host: "psql", port: 5432)
            #endif
            return try connect(to: address, on: eventLoop).then { conn in
                return conn.authenticate(username: "vapor_username", database: "vapor_database", password: "vapor_password")
                    .map { conn }
            }
        } catch {
            return eventLoop.newFailedFuture(error: error)
        }
    }
}
