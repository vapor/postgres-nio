@testable import PostgresNIO
import Foundation

extension PostgresDecodingContext where JSONDecoder == Foundation.JSONDecoder {
    static func forTests() -> Self {
        Self(jsonDecoder: JSONDecoder())
    }
}
