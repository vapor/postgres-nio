import PostgresNIO
import Foundation

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
enum Birthday {
    static func main() async throws {
        // 1. Create a configuration to match server's parameters
        let config = PostgresClient.Configuration(
            host: "localhost",
            port: 5432,
            username: "test_username",
            password: "test_password",
            database: "test_database",
            tls: .disable
        )

        // 2. Create a client
        let client = PostgresClient(configuration: config)

        // 3. Run the client
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run() // !important
            }

            // 4. Create a friends table to store data into
            try await client.query("""
                CREATE TABLE IF NOT EXISTS "friends" (
                    id SERIAL PRIMARY KEY,
                    given_name TEXT,
                    last_name TEXT,
                    birthday TIMESTAMP WITH TIME ZONE
                )
                """
            )

            // 5. Create a Swift friend representation
            struct Friend {
                var firstName: String
                var lastName: String
                var birthday: Date
            }

            // 6. Create John Appleseed with special birthday
            let dateFormatter = DateFormatter()
            dateFormatter.dateFormat = "yyyy-MM-dd"
            let johnsBirthday = dateFormatter.date(from: "1960-09-26")!
            let friend = Friend(firstName: "Hans", lastName: "Müller", birthday: johnsBirthday)

            // 7. Store friend into the database
            try await client.query("""
                INSERT INTO "friends" (given_name, last_name, birthday)
                    VALUES
                        (\(friend.firstName), \(friend.lastName), \(friend.birthday));
                """
            )

            // 8. Query database for the friend we just inserted
            let rows = try await client.query("""
                SELECT id, given_name, last_name, birthday FROM "friends" WHERE given_name = \(friend.firstName)
                """
            )

            // 9. Iterate the returned rows, decoding the rows into Swift primitives
            for try await (id, firstName, lastName, birthday) in rows.decode((Int, String, String, Date).self) {
                print("\(id) | \(firstName) \(lastName), \(birthday)")
            }

            // 10. Shutdown the client, by cancelling its run method, through cancelling the taskGroup.
            taskGroup.cancelAll()
        }
    }
}

