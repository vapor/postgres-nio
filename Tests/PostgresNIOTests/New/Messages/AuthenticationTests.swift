import XCTest
import NIOCore
import NIOTestUtils
@testable import PostgresNIO

class AuthenticationTests: XCTestCase {
    
    func testDecodeAuthentication() {
        var expected = [PostgresBackendMessage]()
        var buffer = ByteBuffer()
        let encoder = PSQLBackendMessageEncoder()
        
        // add ok
        XCTAssertNoThrow(try encoder.encode(data: .authentication(.ok), out: &buffer))
        expected.append(.authentication(.ok))
        
        // add kerberos
        XCTAssertNoThrow(try encoder.encode(data: .authentication(.kerberosV5), out: &buffer))
        expected.append(.authentication(.kerberosV5))
        
        // add plaintext
        XCTAssertNoThrow(try encoder.encode(data: .authentication(.plaintext), out: &buffer))
        expected.append(.authentication(.plaintext))
        
        // add md5
        XCTAssertNoThrow(try encoder.encode(data: .authentication(.md5(salt: (1, 2, 3, 4))), out: &buffer))
        expected.append(.authentication(.md5(salt: (1, 2, 3, 4))))
        
        // add scm credential
        XCTAssertNoThrow(try encoder.encode(data: .authentication(.scmCredential), out: &buffer))
        expected.append(.authentication(.scmCredential))
        
        // add gss
        XCTAssertNoThrow(try encoder.encode(data: .authentication(.gss), out: &buffer))
        expected.append(.authentication(.gss))
        
        // add sspi
        XCTAssertNoThrow(try encoder.encode(data: .authentication(.sspi), out: &buffer))
        expected.append(.authentication(.sspi))
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected)],
            decoderFactory: { PSQLBackendMessageDecoder(hasAlreadyReceivedBytes: false) }))
    }
}
