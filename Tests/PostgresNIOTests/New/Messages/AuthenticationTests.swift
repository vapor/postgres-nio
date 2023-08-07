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
        encoder.encode(data: .authentication(.ok), out: &buffer)
        expected.append(.authentication(.ok))
        
        // add kerberos
        encoder.encode(data: .authentication(.kerberosV5), out: &buffer)
        expected.append(.authentication(.kerberosV5))
        
        // add plaintext
        encoder.encode(data: .authentication(.plaintext), out: &buffer)
        expected.append(.authentication(.plaintext))
        
        // add md5
        let salt: UInt32 = 0x01_02_03_04
        encoder.encode(data: .authentication(.md5(salt: salt)), out: &buffer)
        expected.append(.authentication(.md5(salt: salt)))

        // add scm credential
        encoder.encode(data: .authentication(.scmCredential), out: &buffer)
        expected.append(.authentication(.scmCredential))
        
        // add gss
        encoder.encode(data: .authentication(.gss), out: &buffer)
        expected.append(.authentication(.gss))
        
        // add sspi
        encoder.encode(data: .authentication(.sspi), out: &buffer)
        expected.append(.authentication(.sspi))
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected)],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
        ))
    }
}
