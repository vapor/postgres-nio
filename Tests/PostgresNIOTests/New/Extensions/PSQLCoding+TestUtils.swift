@testable import PostgresNIO
import Foundation

extension PSQLFrontendMessageEncoder {
    static var forTests: Self {
        Self(jsonEncoder: JSONEncoder())
    }
}

extension PostgresDecodingContext where JSONDecoder == Foundation.JSONDecoder {
    static func forTests() -> Self {
        Self(jsonDecoder: JSONDecoder())
    }
}

extension PSQLEncodingContext {
    static func forTests(jsonEncoder: PostgresJSONEncoder = JSONEncoder()) -> Self {
        Self(jsonEncoder: jsonEncoder)
    }
}
