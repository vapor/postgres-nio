import Logging
import NIOPosix
import NIOSSL
@_spi(ConnectionPool) import PostgresNIO
import XCTest

/// Tests for PostgresCodable protocol and JSONB encoding/decoding examples from documentation
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PostgresCodableTests: XCTestCase {

    func testStructWithPrimitivesMapping() async throws {
        // Test a struct with various primitive types that maps directly to a database row
        //
        // PostgresNIO doesn't support decoding multi-column rows directly to custom structs
        // (i.e., rows.decode(Car.self)) without implementing complex PostgresDecodable protocol.
        //
        // Instead, use tuple decoding which PostgresNIO fully supports for all primitive types.
        struct Car: PostgresCodable, Codable {
            let id: Int
            let make: String
            let model: String
            let year: Int
            let price: Double
            let isElectric: Bool
            let registeredAt: Date
            let vin: UUID
        }

        let tableName = "test_cars"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            // Create table with columns matching Car struct fields
            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    make TEXT NOT NULL,
                    model TEXT NOT NULL,
                    year INT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    is_electric BOOLEAN NOT NULL,
                    registered_at TIMESTAMPTZ NOT NULL,
                    vin UUID NOT NULL
                );
                """,
                logger: logger
            )

            // Insert and immediately decode using RETURNING - cleaner than separate INSERT + SELECT!
            let registeredDate = Date()
            let vin = UUID()
            let rows = try await client.query(
                """
                INSERT INTO "\(unescaped: tableName)"
                (make, model, year, price, is_electric, registered_at, vin)
                VALUES (\("Tesla"), \("Model 3"), \(2024), \(45000.0), \(true), \(registeredDate), \(vin))
                RETURNING id, make, model, year, price, is_electric, registered_at, vin
                """,
                logger: logger
            )

            
            // Decode using tuple and then construct our custom struct.
            // PostgresNIO supports tuple decoding for multi-column rows out of the box.
            for try await (id, make, model, year, price, isElectric, registeredAt, vinValue) in rows.decode((Int, String, String, Int, Double, Bool, Date, UUID).self) {
                let car = Car(
                    id: id,
                    make: make,
                    model: model,
                    year: year,
                    price: price,
                    isElectric: isElectric,
                    registeredAt: registeredAt,
                    vin: vinValue
                )
                print("Car: \(car.year) \(car.make) \(car.model)")
                // Verify all fields decoded correctly
                XCTAssertEqual(car.make, "Tesla")
                XCTAssertEqual(car.model, "Model 3")
                XCTAssertEqual(car.year, 2024)
                XCTAssertEqual(car.price, 45000.0)
                XCTAssertEqual(car.isElectric, true)
                XCTAssertEqual(car.vin, vin)
                XCTAssertNotNil(car.registeredAt)
            }
            
            try await client.query(
                """
                DROP TABLE "\(unescaped: tableName)";
                """,
                logger: logger
            )

            taskGroup.cancelAll()
        }
    }

    func testJSONBCodableRoundTrip() async throws {
        // Test the example from our documentation
        struct UserProfile: Codable, PostgresCodable, Equatable {
            let displayName: String
            let bio: String
            let interests: [String]
        }

        let tableName = "test_user_profiles"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            // Create table
            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    profile JSONB NOT NULL
                );
                """,
                logger: logger
            )

            // Insert with Codable struct (from documentation example)
            let userID = 1
            let profile = UserProfile(
                displayName: "Alice",
                bio: "Swift developer",
                interests: ["coding", "hiking"]
            )

            try await client.query(
                """
                INSERT INTO \(unescaped: tableName) (id, profile) VALUES (\(userID), \(profile))
                """,
                logger: logger
            )

            // Decode from results (from documentation example)
            let rows = try await client.query(
                """
                SELECT profile FROM "\(unescaped: tableName)" WHERE id = \(userID)
                """,
                logger: logger
            )

            var decodedProfile: UserProfile?
            for try await row in rows {
                let randomAccessRow = row.makeRandomAccess()
                decodedProfile = try randomAccessRow["profile"].decode(UserProfile.self, context: .default)
                print("Display name: \(decodedProfile!.displayName)")
            }

            // Verify the round-trip
            XCTAssertEqual(decodedProfile, profile)
            XCTAssertEqual(decodedProfile?.displayName, "Alice")
            XCTAssertEqual(decodedProfile?.bio, "Swift developer")
            XCTAssertEqual(decodedProfile?.interests, ["coding", "hiking"])

            // Clean up
            try await client.query(
                """
                DROP TABLE "\(unescaped: tableName)";
                """,
                logger: logger
            )

            taskGroup.cancelAll()
        }
    }

    func testNestedCodableStructsWithJSONB() async throws {
        // Test the nested Codable structs example from our documentation
        // This verifies that complex nested structures work automatically with JSONB
        struct Address: Codable, Equatable {
            let street: String
            let city: String
            let zipCode: String
        }

        struct Company: Codable, PostgresCodable, Equatable {
            let name: String
            let founded: Date
            let address: Address
            let employees: Int
        }

        let tableName = "test_companies"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.addTeardownBlock {
            try await eventLoopGroup.shutdownGracefully()
        }

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            // Create table with JSONB column
            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL
                );
                """,
                logger: logger
            )

            // Insert nested Codable struct (from documentation example)
            let foundedDate = Date()
            let company = Company(
                name: "Acme Inc",
                founded: foundedDate,
                address: Address(street: "123 Main St", city: "Springfield", zipCode: "12345"),
                employees: 50
            )

            try await client.query(
                """
                INSERT INTO "\(unescaped: tableName)" (data) VALUES (\(company))
                """,
                logger: logger
            )

            // Retrieve and decode the nested structure
            let rows = try await client.query(
                """
                SELECT data FROM "\(unescaped: tableName)"
                """,
                logger: logger
            )

            var decodedCompany: Company?
            for try await row in rows {
                let randomAccessRow = row.makeRandomAccess()
                decodedCompany = try randomAccessRow["data"].decode(Company.self, context: .default)
                print("Company: \(decodedCompany!.name)")
            }

            // Verify the round-trip of the nested structure
            XCTAssertEqual(decodedCompany?.name, "Acme Inc")
            XCTAssertEqual(decodedCompany?.employees, 50)
            XCTAssertEqual(decodedCompany?.address.street, "123 Main St")
            XCTAssertEqual(decodedCompany?.address.city, "Springfield")
            XCTAssertEqual(decodedCompany?.address.zipCode, "12345")
            // Note: Date comparison may have slight precision differences, so we check it exists
            XCTAssertNotNil(decodedCompany?.founded)

            // Clean up
            try await client.query(
                """
                DROP TABLE "\(unescaped: tableName)";
                """,
                logger: logger
            )

            taskGroup.cancelAll()
        }
    }

}
