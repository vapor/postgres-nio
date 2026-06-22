@testable import _ConnectionPoolModule
import Testing

@Suite struct Max2SequenceTests {

    @Test func testCountAndIsEmpty() async {
        var sequence = Max2Sequence<Int>()
        #expect(sequence.count == 0)
        #expect(sequence.isEmpty == true)
        sequence.append(1)
        #expect(sequence.count == 1)
        #expect(sequence.isEmpty == false)
        sequence.append(2)
        #expect(sequence.count == 2)
        #expect(sequence.isEmpty == false)
    }

    @Test func testOptionalInitializer() {
        let emptySequence = Max2Sequence<Int>(nil, nil)
        #expect(emptySequence.count == 0)
        #expect(emptySequence.isEmpty == true)
        var emptySequenceIterator = emptySequence.makeIterator()
        #expect(emptySequenceIterator.next() == nil)
        #expect(emptySequenceIterator.next() == nil)
        #expect(emptySequenceIterator.next() == nil)

        let oneElemSequence1 = Max2Sequence<Int>(1, nil)
        #expect(oneElemSequence1.count == 1)
        #expect(oneElemSequence1.isEmpty == false)
        var oneElemSequence1Iterator = oneElemSequence1.makeIterator()
        #expect(oneElemSequence1Iterator.next() == 1)
        #expect(oneElemSequence1Iterator.next() == nil)
        #expect(oneElemSequence1Iterator.next() == nil)

        let oneElemSequence2 = Max2Sequence<Int>(nil, 2)
        #expect(oneElemSequence2.count == 1)
        #expect(oneElemSequence2.isEmpty == false)
        var oneElemSequence2Iterator = oneElemSequence2.makeIterator()
        #expect(oneElemSequence2Iterator.next() == 2)
        #expect(oneElemSequence2Iterator.next() == nil)
        #expect(oneElemSequence2Iterator.next() == nil)

        let twoElemSequence = Max2Sequence<Int>(1, 2)
        #expect(twoElemSequence.count == 2)
        #expect(twoElemSequence.isEmpty == false)
        var twoElemSequenceIterator = twoElemSequence.makeIterator()
        #expect(twoElemSequenceIterator.next() == 1)
        #expect(twoElemSequenceIterator.next() == 2)
        #expect(twoElemSequenceIterator.next() == nil)
    }

    func testMap() {
        let twoElemSequence = Max2Sequence<Int>(1, 2).map({ "\($0)" })
        #expect(twoElemSequence.count == 2)
        #expect(twoElemSequence.isEmpty == false)
        var twoElemSequenceIterator = twoElemSequence.makeIterator()
        #expect(twoElemSequenceIterator.next() == "1")
        #expect(twoElemSequenceIterator.next() == "2")
        #expect(twoElemSequenceIterator.next() == nil)
    }
}
