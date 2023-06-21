import SwiftDiagnostics

enum PostgresNIODiagnostic: String, DiagnosticMessage {
    case wrongArgument
    case notAStruct

    var message: String {
        switch self {
        case .wrongArgument:
            return "Invalid argument"
        case .notAStruct:
            return "Macro only works with structs"
        }
    }

    var diagnosticID: SwiftDiagnostics.MessageID {
        MessageID(domain: "PostgresNIOMacros", id: rawValue)
    }

    var severity: SwiftDiagnostics.DiagnosticSeverity { .error }
}
