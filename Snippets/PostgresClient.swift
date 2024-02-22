import PostgresNIO
import struct Foundation.UUID

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
enum Runner {
    static func main() async throws {

// snippet.configuration
let config = PostgresClient.Configuration(
    host: "localhost",
    port: 5432,
    username: "my_username",
    password: "my_password",
    database: "my_database",
    tls: .disable
)
// snippet.end

// snippet.makeClient
let client = PostgresClient(configuration: config)
// snippet.end

    }

    static func runAndCancel(client: PostgresClient) async {
// snippet.run
await withTaskGroup(of: Void.self) { taskGroup in
    taskGroup.addTask {
        await client.run() // !important
    }

    // You can use the client while the `client.run()` method is not cancelled.

    // To shutdown the client, cancel its run method, by cancelling the taskGroup.
    taskGroup.cancelAll()
}
// snippet.end
    }
}

