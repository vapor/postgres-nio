import NIO
import NIOPostgres

let eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
let conn = try PostgresConnection.connect(to: .init(ipAddress: "127.0.0.1", port: 5432), on: eventLoop).wait()
try conn.authenticate(username: "vapor_username", database: "vapor_database", password: "vapor_password").wait()

print("Starting")

for _ in 0..<10_000 {
    _ = try conn.simpleQuery("SELECT version();").wait()
}

print("Done")
try conn.close().wait()
