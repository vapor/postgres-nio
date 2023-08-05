import SwiftSyntaxMacros
import SwiftSyntaxMacrosTestSupport
import XCTest
import PostgresNIOMacros

let testMacros: [String: Macro.Type] = [
    "Query": PostgresTypedQueryMacro.self,
]

final class MacrosTests: XCTestCase {
    func testMacro() {
        assertMacroExpansion(
            #"""
            @Query("SELECT \("id", Int.self), \("name", String.self) FROM users")
            struct MyQuery {}
            """#,
            expandedSource: #"""
            struct MyQuery {
                struct Row: PostgresTypedRow {
                    let id: Int
                    let name: String
                }
            }
            """#,
//            expandedSource: #"""
//            struct MyQuery: PostgresTypedQuery {
//                struct Row: PostgresTypedRow {
//                    let id: Int
//                    let name: String
//
//                    init(from row: PostgresRow) throws {
//                        (id, name) = try row.decode((Int, String).self, context: .default)
//                    }
//                }
//
//                var sql: PostgresQuery {
//                    "SELECT id, name FROM users"
//                }
//            }
//            """#,
            macros: testMacros
        )
    }
}

