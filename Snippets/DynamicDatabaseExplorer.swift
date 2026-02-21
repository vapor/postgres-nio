import PostgresNIO
import Foundation

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
enum DynamicDatabaseExplorer {
    static func main() async throws {
        try await withMockDatabase { client in
            // snippet.explore
            // 1. Discover all user tables via the Postgres metadata tables
            let tables = try await client.query("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """
            )

            for try await tableName in tables.decode(String.self) {
                print("## \(tableName)")

                // 2. Load rows from each table without knowing the schema upfront
                // > Note: table names are identifiers, not values — they cannot be passed as bind parameters, so we must use `unsafeSQL` here.
                let rows = try await client.query(
                    PostgresQuery(unsafeSQL: #"SELECT * FROM "\#(tableName)""#)
                )

                for try await row in rows {
                    // 3. Access column metadata via `rows.columns`
                    for (column, metadata) in zip(row, rows.columns) {
                        let value: Any
                        // 4. Dynamically decode column values based on their metadata data type
                        switch metadata.dataType {
                        case .int2:
                            value = try column.decode(Int16.self)
                        case .int4:
                            value = try column.decode(Int32.self)
                        case .int8:
                            value = try column.decode(Int64.self)
                        case .float4:
                            value = try column.decode(Float.self)
                        case .text:
                            value = try column.decode(String.self)
                        case .timestamp, .timestamptz:
                            value = try column.decode(Date.self)
                        case .bytea:
                            value = "<\(try column.decode(ByteBuffer.self).readableBytes) bytes>"
                        default:
                            // Fallback: most types can be decoded as String
                            value = (try? column.decode(String.self)) ?? "<unknown>"
                        }
                        print("  `\(metadata.name)` (\(metadata.dataType), value: `\(value)`)")
                    }
                }
            }
            // snippet.end
        }
    }
}

/// A helper function that sets up a mock Postgres database with some tables and data,
/// runs a user-provided closure with a ``PostgresClient`` to explore the database,
/// and then cleans up by dropping the tables.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func withMockDatabase(closure: (PostgresClient) async throws -> Void) async throws {
    let config = PostgresClient.Configuration(
        host: "localhost",
        port: 5432,
        username: "test_username",
        password: "test_password",
        database: "test_database",
        tls: .disable
    )
    let client = PostgresClient(configuration: config)

    try await withThrowingTaskGroup(of: Void.self) { taskGroup in
        taskGroup.addTask { await client.run() }

        // Create some tables with different schemas to explore, and populate them with some mock data
        try await client.query("""
            CREATE TABLE IF NOT EXISTS "users" (
                id SERIAL PRIMARY KEY,
                email TEXT,
                created_at TIMESTAMPTZ
            );
            """
        )
        try await client.query("""
            INSERT INTO "users" (email, created_at) VALUES
                ('alice@example.com', NOW()),
                ('bob@example.com', NOW()),
                ('charlie@example.com', NOW());
            """
        )

        try await client.query("""
            CREATE TABLE IF NOT EXISTS "orders" (
                id SERIAL PRIMARY KEY,
                user_id INT,
                product_name TEXT,
                quantity INT,
                price FLOAT4
            );
            """
        )
        try await client.query("""
            INSERT INTO "orders" (user_id, product_name, quantity, price) VALUES
                (1, 'MacGuffin', 3, 19.99),
                (2, 'Gadget', 1, 99.95)
            """
        )

        try await client.query("""
            CREATE TABLE IF NOT EXISTS "files" (
                id SERIAL PRIMARY KEY,
                filename TEXT,
                data BYTEA
            );
            """
        )
        try await client.query("""
            INSERT INTO "files" (filename, data) VALUES
                ('report.pdf', decode('255044462d312e350a25d0d4c5d80a34', 'hex')),
                ('photo.jpg', decode('ffd8ffe000104a46494600010101006000600000', 'hex'));
            """
        )

        // Run the user-provided closure to explore the database
        try await closure(client)

        // Clean up the database by dropping the mock tables we created
        try await client.query(#"DROP TABLE IF EXISTS "users", "orders", "files";"#)

        taskGroup.cancelAll()
    }
}

if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *) {
    try await DynamicDatabaseExplorer.main()
} else {
    print("Requires at least macOS 13.0, iOS 16.0, tvOS 16.0, or watchOS 9.0")
}
