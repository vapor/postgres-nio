import _ConnectionPoolModule
import XCTest

final class ConnectionIDGeneratorTests: XCTestCase {
    func testGenerateConnectionIDs() async {
        let idGenerator = ConnectionIDGenerator()

        XCTAssertEqual(idGenerator.next(), 0)
        XCTAssertEqual(idGenerator.next(), 1)
        XCTAssertEqual(idGenerator.next(), 2)

        await withTaskGroup(of: Void.self) { taskGroup in
            for _ in 0..<1000 {
                taskGroup.addTask {
                    _ = idGenerator.next()
                }
            }
        }

        XCTAssertEqual(idGenerator.next(), 1003)
    }
}
