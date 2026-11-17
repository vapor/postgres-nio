import PostgresNIO
import Testing

@Suite
struct PostgresData_JSONTests {
    @available(*, deprecated, message: "Testing deprecated functionality")
    @Test func jsonbConvertible() {
        struct Object: PostgresJSONBCodable {
            let foo: Int
            let bar: Int
        }

        #expect(Object.postgresDataType == .jsonb)

        let postgresData = Object(foo: 1, bar: 2).postgresData
        #expect(postgresData?.type == .jsonb)

        let object = Object(postgresData: postgresData!)
        #expect(object?.foo == 1)
        #expect(object?.bar == 2)
    }
}
