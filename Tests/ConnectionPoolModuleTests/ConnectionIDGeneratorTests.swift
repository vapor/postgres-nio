import _ConnectionPoolModule
import Testing

@Suite struct ConnectionIDGeneratorTests {

    @Test func testGenerateConnectionIDs() async {
        let idGenerator = ConnectionIDGenerator()

        #expect(idGenerator.next() == 0)
        #expect(idGenerator.next() == 1)
        #expect(idGenerator.next() == 2)

        await withTaskGroup(of: Void.self) { taskGroup in
            for _ in 0..<1000 {
                taskGroup.addTask {
                    _ = idGenerator.next()
                }
            }
        }

        #expect(idGenerator.next() == 1003)
    }
}
