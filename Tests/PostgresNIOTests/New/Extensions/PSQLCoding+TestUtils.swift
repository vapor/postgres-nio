@testable import PostgresNIO
import Foundation

extension PostgresDecodingContext where JSONDecoder == Foundation.JSONDecoder {
    static func forTests() -> Self {
        Self(jsonDecoder: JSONDecoder())
    }
}

extension PostgresEncodingContext where JSONEncoder == Foundation.JSONEncoder {
    static func forTests(jsonEncoder: JSONEncoder = JSONEncoder()) -> Self {
        Self(jsonEncoder: jsonEncoder)
    }
}
