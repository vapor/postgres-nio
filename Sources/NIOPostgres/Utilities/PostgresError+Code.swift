extension PostgresError {
    public struct Code: ExpressibleByStringLiteral, Equatable {
        // Class 00 — Successful Completion
        public static let successfulCompletion: Code = "00000"
        
        // Class 01 — Warning
        public static let warning: Code = "01000"
        public static let dynamicResultSetsReturned: Code = "0100C"
        public static let implicitZeroBitPadding: Code = "01008"
        public static let nullValueEliminatedInSetFunction: Code = "01003"
        public static let privilegeNotGranted: Code = "01007"
        public static let privilegeNotRevoked: Code = "01006"
        public static let stringDataRightTruncation: Code = "01004"
        public static let deprecatedFeature: Code = "01P01"
        
        // Class 02 — No Data (this is also a warning class per the SQL standard)
        public static let noData: Code = "02000"
        public static let noAdditionalDynamicResultSetsReturned: Code = "02001"
        
        // Class 03 — SQL Statement Not Yet Complete
        public static let sqlStatementNotYetComplete: Code = "03000"
        
        // Class 08 — Connection Exception
        public static let connectionException: Code = "08000"
        public static let connectionDoesNotExist: Code = "08003"
        public static let connectionFailure: Code = "08006"
        public static let sqlclientUnableToEstablishSqlconnection: Code = "08001"
        public static let sqlserverRejectedEstablishmentOfSqlconnection: Code = "08004"
        public static let transactionResolutionUnknown: Code = "08007"
        public static let protocolViolation: Code = "08P01"
        
        // Class 09 — Triggered Action Exception
        public static let triggeredActionException: Code = "09000"
        
        // Class 0A — Feature Not Supported
        public static let featureNotSupported: Code = "0A000"
        
        // Class 0B — Invalid Transaction Initiation
        public static let invalidTransactionInitiation: Code = "0B000"
        
        // Class 0F — Locator Exception
        public static let locatorException: Code = "0F000"
        public static let invalidLocatorSpecification: Code = "0F001"
        
        // Class 0L — Invalid Grantor
        public static let invalidGrantor: Code = "0L000"
        public static let invalidGrantOperation: Code = "0LP01"
        
        // Class 0P — Invalid Role Specification
        public static let invalidRoleSpecification: Code = "0P000"
        
        // Class 0Z — Diagnostics Exception
        public static let diagnosticsException: Code = "0Z000"
        public static let stackedDiagnosticsAccessedWithoutActiveHandler: Code = "0Z002"
        
        // Class 20 — Case Not Found
        public static let caseNotFound: Code = "20000"
        
        // Class 21 — Cardinality Violation
        public static let cardinalityViolation: Code = "21000"
        
        // Class 22 — Data Exception
        public static let dataException: Code = "22000"
        public static let arraySubscriptError: Code = "2202E"
        public static let characterNotInRepertoire: Code = "22021"
        public static let datetimeFieldOverflow: Code = "22008"
        public static let divisionByZero: Code = "22012"
        public static let errorInAssignment: Code = "22005"
        public static let escapeCharacterConflict: Code = "2200B"
        public static let indicatorOverflow: Code = "22022"
        public static let intervalFieldOverflow: Code = "22015"
        public static let invalidArgumentForLogarithm: Code = "2201E"
        public static let invalidArgumentForNtileFunction: Code = "22014"
        public static let invalidArgumentForNthValueFunction: Code = "22016"
        public static let invalidArgumentForPowerFunction: Code = "2201F"
        public static let invalidArgumentForWidthBucketFunction: Code = "2201G"
        public static let invalidCharacterValueForCast: Code = "22018"
        public static let invalidDatetimeFormat: Code = "22007"
        public static let invalidEscapeCharacter: Code = "22019"
        public static let invalidEscapeOctet: Code = "2200D"
        public static let invalidEscapeSequence: Code = "22025"
        public static let nonstandardUseOfEscapeCharacter: Code = "22P06"
        public static let invalidIndicatorParameterValue: Code = "22010"
        public static let invalidParameterValue: Code = "22023"
        public static let invalidRegularExpression: Code = "2201B"
        public static let invalidRowCountInLimitClause: Code = "2201W"
        public static let invalidRowCountInResultOffsetClause: Code = "2201X"
        public static let invalidTablesampleArgument: Code = "2202H"
        public static let invalidTablesampleRepeat: Code = "2202G"
        public static let invalidTimeZoneDisplacementValue: Code = "22009"
        public static let invalidUseOfEscapeCharacter: Code = "2200C"
        public static let mostSpecificTypeMismatch: Code = "2200G"
        public static let nullValueNotAllowed: Code = "22004"
        public static let nullValueNoIndicatorParameter: Code = "22002"
        public static let numericValueOutOfRange: Code = "22003"
        public static let stringDataLengthMismatch: Code = "22026"
        public static let stringDataRightTruncationException: Code = "22001"
        public static let substringError: Code = "22011"
        public static let trimError: Code = "22027"
        public static let unterminatedCString: Code = "22024"
        public static let zeroLengthCharacterString: Code = "2200F"
        public static let floatingPointException: Code = "22P01"
        public static let invalidTextRepresentation: Code = "22P02"
        public static let invalidBinaryRepresentation: Code = "22P03"
        public static let badCopyFileFormat: Code = "22P04"
        public static let untranslatableCharacter: Code = "22P05"
        public static let notAnXmlDocument: Code = "2200L"
        public static let invalidXmlDocument: Code = "2200M"
        public static let invalidXmlContent: Code = "2200N"
        public static let invalidXmlComment: Code = "2200S"
        public static let invalidXmlProcessingInstruction: Code = "2200T"
        
        // Class 23 — Integrity Constraint Violation
        public static let integrityConstraintViolation: Code = "23000"
        public static let restrictViolation: Code = "23001"
        public static let notNullViolation: Code = "23502"
        public static let foreignKeyViolation: Code = "23503"
        public static let uniqueViolation: Code = "23505"
        public static let checkViolation: Code = "23514"
        public static let exclusionViolation: Code = "23P01"
        
        // Class 24 — Invalid Cursor State
        public static let invalidCursorState: Code = "24000"
        
        // Class 25 — Invalid Transaction State
        public static let invalidTransactionState: Code = "25000"
        public static let activeSqlTransaction: Code = "25001"
        public static let branchTransactionAlreadyActive: Code = "25002"
        public static let heldCursorRequiresSameIsolationLevel: Code = "25008"
        public static let inappropriateAccessModeForBranchTransaction: Code = "25003"
        public static let inappropriateIsolationLevelForBranchTransaction: Code = "25004"
        public static let noActiveSqlTransactionForBranchTransaction: Code = "25005"
        public static let readOnlySqlTransaction: Code = "25006"
        public static let schemaAndDataStatementMixingNotSupported: Code = "25007"
        public static let noActiveSqlTransaction: Code = "25P01"
        public static let inFailedSqlTransaction: Code = "25P02"
        public static let idleInTransactionSessionTimeout: Code = "25P03"
        
        // Class 26 — Invalid SQL Statement Name
        public static let invalidSqlStatementName: Code = "26000"
        
        // Class 27 — Triggered Data Change Violation
        public static let triggeredDataChangeViolation: Code = "27000"
        
        // Class 28 — Invalid Authorization Specification
        public static let invalidAuthorizationSpecification: Code = "28000"
        public static let invalidPassword: Code = "28P01"
        
        // Class 2B — Dependent Privilege Descriptors Still Exist
        public static let dependentPrivilegeDescriptorsStillExist: Code = "2B000"
        public static let dependentObjectsStillExist: Code = "2BP01"
        
        // Class 2D — Invalid Transaction Termination
        public static let invalidTransactionTermination: Code = "2D000"
        
        // Class 2F — SQL Routine Exception
        public static let sqlRoutineException: Code = "2F000"
        public static let functionExecutedNoReturnStatement: Code = "2F005"
        public static let modifyingSqlDataNotPermitted: Code = "2F002"
        public static let prohibitedSqlStatementAttempted: Code = "2F003"
        public static let readingSqlDataNotPermitted: Code = "2F004"
        
        // Class 34 — Invalid Cursor Name
        public static let invalidCursorName: Code = "34000"
        
        // Class 38 — External Routine Exception
        public static let externalRoutineException: Code = "38000"
        public static let containingSqlNotPermitted: Code = "38001"
        public static let modifyingSqlDataNotPermittedExternal: Code = "38002"
        public static let prohibitedSqlStatementAttemptedExternal: Code = "38003"
        public static let readingSqlDataNotPermittedExternal: Code = "38004"
        
        // Class 39 — External Routine Invocation Exception
        public static let externalRoutineInvocationException: Code = "39000"
        public static let invalidSqlstateReturned: Code = "39001"
        public static let nullValueNotAllowedExternal: Code = "39004"
        public static let triggerProtocolViolated: Code = "39P01"
        public static let srfProtocolViolated: Code = "39P02"
        public static let eventTriggerProtocolViolated: Code = "39P03"
        
        // Class 3B — Savepoint Exception
        public static let savepointException: Code = "3B000"
        public static let invalidSavepointSpecification: Code = "3B001"
        
        // Class 3D — Invalid Catalog Name
        public static let invalidCatalogName: Code = "3D000"
        
        // Class 3F — Invalid Schema Name
        public static let invalidSchemaName: Code = "3F000"
        
        // Class 40 — Transaction Rollback
        public static let transactionRollback: Code = "40000"
        public static let transactionIntegrityConstraintViolation: Code = "40002"
        public static let serializationFailure: Code = "40001"
        public static let statementCompletionUnknown: Code = "40003"
        public static let deadlockDetected: Code = "40P01"
        
        // Class 42 — Syntax Error or Access Rule Violation
        public static let syntaxErrorOrAccessRuleViolation: Code = "42000"
        public static let syntaxError: Code = "42601"
        public static let insufficientPrivilege: Code = "42501"
        public static let cannotCoerce: Code = "42846"
        public static let groupingError: Code = "42803"
        public static let windowingError: Code = "42P20"
        public static let invalidRecursion: Code = "42P19"
        public static let invalidForeignKey: Code = "42830"
        public static let invalidName: Code = "42602"
        public static let nameTooLong: Code = "42622"
        public static let reservedName: Code = "42939"
        public static let datatypeMismatch: Code = "42804"
        public static let indeterminateDatatype: Code = "42P18"
        public static let collationMismatch: Code = "42P21"
        public static let indeterminateCollation: Code = "42P22"
        public static let wrongObjectType: Code = "42809"
        public static let undefinedColumn: Code = "42703"
        public static let undefinedFunction: Code = "42883"
        public static let undefinedTable: Code = "42P01"
        public static let undefinedParameter: Code = "42P02"
        public static let undefinedObject: Code = "42704"
        public static let duplicateColumn: Code = "42701"
        public static let duplicateCursor: Code = "42P03"
        public static let duplicateDatabase: Code = "42P04"
        public static let duplicateFunction: Code = "42723"
        public static let duplicatePreparedStatement: Code = "42P05"
        public static let duplicateSchema: Code = "42P06"
        public static let duplicateTable: Code = "42P07"
        public static let duplicateAlias: Code = "42712"
        public static let duplicateObject: Code = "42710"
        public static let ambiguousColumn: Code = "42702"
        public static let ambiguousFunction: Code = "42725"
        public static let ambiguousParameter: Code = "42P08"
        public static let ambiguousAlias: Code = "42P09"
        public static let invalidColumnReference: Code = "42P10"
        public static let invalidColumnDefinition: Code = "42611"
        public static let invalidCursorDefinition: Code = "42P11"
        public static let invalidDatabaseDefinition: Code = "42P12"
        public static let invalidFunctionDefinition: Code = "42P13"
        public static let invalidPreparedStatementDefinition: Code = "42P14"
        public static let invalidSchemaDefinition: Code = "42P15"
        public static let invalidTableDefinition: Code = "42P16"
        public static let invalidObjectDefinition: Code = "42P17"
        
        // Class 44 — WITH CHECK OPTION Violation
        public static let withCheckOptionViolation: Code = "44000"
        
        // Class 53 — Insufficient Resources
        public static let insufficientResources: Code = "53000"
        public static let diskFull: Code = "53100"
        public static let outOfMemory: Code = "53200"
        public static let tooManyConnections: Code = "53300"
        public static let configurationLimitExceeded: Code = "53400"
        
        // Class 54 — Program Limit Exceeded
        public static let programLimitExceeded: Code = "54000"
        public static let statementTooComplex: Code = "54001"
        public static let tooManyColumns: Code = "54011"
        public static let tooManyArguments: Code = "54023"
        
        // Class 55 — Object Not In Prerequisite State
        public static let objectNotInPrerequisiteState: Code = "55000"
        public static let objectInUse: Code = "55006"
        public static let cantChangeRuntimeParam: Code = "55P02"
        public static let lockNotAvailable: Code = "55P03"
        
        // Class 57 — Operator Intervention
        public static let operatorIntervention: Code = "57000"
        public static let queryCanceled: Code = "57014"
        public static let adminShutdown: Code = "57P01"
        public static let crashShutdown: Code = "57P02"
        public static let cannotConnectNow: Code = "57P03"
        public static let databaseDropped: Code = "57P04"
        
        // Class 58 — System Error (errors external to PostgreSQL itself)
        public static let systemError: Code = "58000"
        public static let ioError: Code = "58030"
        public static let undefinedFile: Code = "58P01"
        public static let duplicateFile: Code = "58P02"
        
        // Class 72 — Snapshot Failure
        public static let snapshotTooOld: Code = "72000"
        
        // Class F0 — Configuration File Error
        public static let configFileError: Code = "F0000"
        public static let lockFileExists: Code = "F0001"
        
        // Class HV — Foreign Data Wrapper Error (SQL/MED)
        public static let fdwError: Code = "HV000"
        public static let fdwColumnNameNotFound: Code = "HV005"
        public static let fdwDynamicParameterValueNeeded: Code = "HV002"
        public static let fdwFunctionSequenceError: Code = "HV010"
        public static let fdwInconsistentDescriptorInformation: Code = "HV021"
        public static let fdwInvalidAttributeValue: Code = "HV024"
        public static let fdwInvalidColumnName: Code = "HV007"
        public static let fdwInvalidColumnNumber: Code = "HV008"
        public static let fdwInvalidDataType: Code = "HV004"
        public static let fdwInvalidDataTypeDescriptors: Code = "HV006"
        public static let fdwInvalidDescriptorFieldIdentifier: Code = "HV091"
        public static let fdwInvalidHandle: Code = "HV00B"
        public static let fdwInvalidOptionIndex: Code = "HV00C"
        public static let fdwInvalidOptionName: Code = "HV00D"
        public static let fdwInvalidStringLengthOrBufferLength: Code = "HV090"
        public static let fdwInvalidStringFormat: Code = "HV00A"
        public static let fdwInvalidUseOfNullPointer: Code = "HV009"
        public static let fdwTooManyHandles: Code = "HV014"
        public static let fdwOutOfMemory: Code = "HV001"
        public static let fdwNoSchemas: Code = "HV00P"
        public static let fdwOptionNameNotFound: Code = "HV00J"
        public static let fdwReplyHandle: Code = "HV00K"
        public static let fdwSchemaNotFound: Code = "HV00Q"
        public static let fdwTableNotFound: Code = "HV00R"
        public static let fdwUnableToCreateExecution: Code = "HV00L"
        public static let fdwUnableToCreateReply: Code = "HV00M"
        public static let fdwUnableToEstablishConnection: Code = "HV00N"
        
        // Class P0 — PL/pgSQL Error
        public static let plpgsqlError: Code = "P0000"
        public static let raiseException: Code = "P0001"
        public static let noDataFound: Code = "P0002"
        public static let tooManyRows: Code = "P0003"
        public static let assertFailure: Code = "P0004"
        
        // Class XX — Internal Error
        public static let internalError: Code = "XX000"
        public static let dataCorrupted: Code = "XX001"
        public static let indexCorrupted: Code = "XX002"
        
        public let raw: String
        
        public init(stringLiteral value: String) {
            self.raw = value
        }

        public init(raw: String) {
            self.raw = raw
        }
    }
    
    public var code: Code {
        switch self {
        case .protocol: return .internalError
        case .server(let server):
            guard let code = server.fields[.sqlState] else {
                return .internalError
            }
            return Code(raw: code)
        case .connectionClosed: return .internalError
        }
    }
}
