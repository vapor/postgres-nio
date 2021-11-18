import PostgresNIO
import XCTest

class PostgresData_JSONTests: XCTestCase {
    func testJSONBConvertible() {
        struct Object: PostgresJSONBCodable {
            let foo: Int
            let bar: Int
        }

        XCTAssertEqual(Object.postgresDataType, .jsonb)

        let postgresData = Object(foo: 1, bar: 2).postgresData
        XCTAssertEqual(postgresData?.type, .jsonb)

        let object = Object(postgresData: postgresData!)
        XCTAssertEqual(object?.foo, 1)
        XCTAssertEqual(object?.bar, 2)
    }
}
