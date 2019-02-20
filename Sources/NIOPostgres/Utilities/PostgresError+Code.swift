extension PostgresError {
    public struct Code: ExpressibleByStringLiteral, Equatable {
        // Class 00 — Successful Completion
        public static let successful_completion: Code = "00000"
        
        // Class 01 — Warning
        public static let warning: Code = "01000"
        public static let dynamic_result_sets_returned: Code = "0100C"
        public static let implicit_zero_bit_padding: Code = "01008"
        public static let null_value_eliminated_in_set_function: Code = "01003"
        public static let privilege_not_granted: Code = "01007"
        public static let privilege_not_revoked: Code = "01006"
        public static let string_data_right_truncation: Code = "01004"
        public static let deprecated_feature: Code = "01P01"
        
        // Class 02 — No Data (this is also a warning class per the SQL standard)
        public static let no_data: Code = "02000"
        public static let no_additional_dynamic_result_sets_returned: Code = "02001"
        
        // Class 03 — SQL Statement Not Yet Complete
        public static let sql_statement_not_yet_complete: Code = "03000"
        
        // Class 08 — Connection Exception
        public static let connection_exception: Code = "08000"
        public static let connection_does_not_exist: Code = "08003"
        public static let connection_failure: Code = "08006"
        public static let sqlclient_unable_to_establish_sqlconnection: Code = "08001"
        public static let sqlserver_rejected_establishment_of_sqlconnection: Code = "08004"
        public static let transaction_resolution_unknown: Code = "08007"
        public static let protocol_violation: Code = "08P01"
        
        // Class 09 — Triggered Action Exception
        public static let triggered_action_exception: Code = "09000"
        
        // Class 0A — Feature Not Supported
        public static let feature_not_supported: Code = "0A000"
        
        // Class 0B — Invalid Transaction Initiation
        public static let invalid_transaction_initiation: Code = "0B000"
        
        // Class 0F — Locator Exception
        public static let locator_exception: Code = "0F000"
        public static let invalid_locator_specification: Code = "0F001"
        
        // Class 0L — Invalid Grantor
        public static let invalid_grantor: Code = "0L000"
        public static let invalid_grant_operation: Code = "0LP01"
        
        // Class 0P — Invalid Role Specification
        public static let invalid_role_specification: Code = "0P000"
        
        // Class 0Z — Diagnostics Exception
        public static let diagnostics_exception: Code = "0Z000"
        public static let stacked_diagnostics_accessed_without_active_handler: Code = "0Z002"
        
        // Class 20 — Case Not Found
        public static let case_not_found: Code = "20000"
        
        // Class 21 — Cardinality Violation
        public static let cardinality_violation: Code = "21000"
        
        // Class 22 — Data Exception
        public static let data_exception: Code = "22000"
        public static let array_subscript_error: Code = "2202E"
        public static let character_not_in_repertoire: Code = "22021"
        public static let datetime_field_overflow: Code = "22008"
        public static let division_by_zero: Code = "22012"
        public static let error_in_assignment: Code = "22005"
        public static let escape_character_conflict: Code = "2200B"
        public static let indicator_overflow: Code = "22022"
        public static let interval_field_overflow: Code = "22015"
        public static let invalid_argument_for_logarithm: Code = "2201E"
        public static let invalid_argument_for_ntile_function: Code = "22014"
        public static let invalid_argument_for_nth_value_function: Code = "22016"
        public static let invalid_argument_for_power_function: Code = "2201F"
        public static let invalid_argument_for_width_bucket_function: Code = "2201G"
        public static let invalid_character_value_for_cast: Code = "22018"
        public static let invalid_datetime_format: Code = "22007"
        public static let invalid_escape_character: Code = "22019"
        public static let invalid_escape_octet: Code = "2200D"
        public static let invalid_escape_sequence: Code = "22025"
        public static let nonstandard_use_of_escape_character: Code = "22P06"
        public static let invalid_indicator_parameter_value: Code = "22010"
        public static let invalid_parameter_value: Code = "22023"
        public static let invalid_regular_expression: Code = "2201B"
        public static let invalid_row_count_in_limit_clause: Code = "2201W"
        public static let invalid_row_count_in_result_offset_clause: Code = "2201X"
        public static let invalid_tablesample_argument: Code = "2202H"
        public static let invalid_tablesample_repeat: Code = "2202G"
        public static let invalid_time_zone_displacement_value: Code = "22009"
        public static let invalid_use_of_escape_character: Code = "2200C"
        public static let most_specific_type_mismatch: Code = "2200G"
        public static let null_value_not_allowed: Code = "22004"
        public static let null_value_no_indicator_parameter: Code = "22002"
        public static let numeric_value_out_of_range: Code = "22003"
        public static let string_data_length_mismatch: Code = "22026"
        public static let string_data_right_truncation_exception: Code = "22001"
        public static let substring_error: Code = "22011"
        public static let trim_error: Code = "22027"
        public static let unterminated_c_string: Code = "22024"
        public static let zero_length_character_string: Code = "2200F"
        public static let floating_point_exception: Code = "22P01"
        public static let invalid_text_representation: Code = "22P02"
        public static let invalid_binary_representation: Code = "22P03"
        public static let bad_copy_file_format: Code = "22P04"
        public static let untranslatable_character: Code = "22P05"
        public static let not_an_xml_document: Code = "2200L"
        public static let invalid_xml_document: Code = "2200M"
        public static let invalid_xml_content: Code = "2200N"
        public static let invalid_xml_comment: Code = "2200S"
        public static let invalid_xml_processing_instruction: Code = "2200T"
        
        // Class 23 — Integrity Constraint Violation
        public static let integrity_constraint_violation: Code = "23000"
        public static let restrict_violation: Code = "23001"
        public static let not_null_violation: Code = "23502"
        public static let foreign_key_violation: Code = "23503"
        public static let unique_violation: Code = "23505"
        public static let check_violation: Code = "23514"
        public static let exclusion_violation: Code = "23P01"
        
        // Class 24 — Invalid Cursor State
        public static let invalid_cursor_state: Code = "24000"
        
        // Class 25 — Invalid Transaction State
        public static let invalid_transaction_state: Code = "25000"
        public static let active_sql_transaction: Code = "25001"
        public static let branch_transaction_already_active: Code = "25002"
        public static let held_cursor_requires_same_isolation_level: Code = "25008"
        public static let inappropriate_access_mode_for_branch_transaction: Code = "25003"
        public static let inappropriate_isolation_level_for_branch_transaction: Code = "25004"
        public static let no_active_sql_transaction_for_branch_transaction: Code = "25005"
        public static let read_only_sql_transaction: Code = "25006"
        public static let schema_and_data_statement_mixing_not_supported: Code = "25007"
        public static let no_active_sql_transaction: Code = "25P01"
        public static let in_failed_sql_transaction: Code = "25P02"
        public static let idle_in_transaction_session_timeout: Code = "25P03"
        
        // Class 26 — Invalid SQL Statement Name
        public static let invalid_sql_statement_name: Code = "26000"
        
        // Class 27 — Triggered Data Change Violation
        public static let triggered_data_change_violation: Code = "27000"
        
        // Class 28 — Invalid Authorization Specification
        public static let invalid_authorization_specification: Code = "28000"
        public static let invalid_password: Code = "28P01"
        
        // Class 2B — Dependent Privilege Descriptors Still Exist
        public static let dependent_privilege_descriptors_still_exist: Code = "2B000"
        public static let dependent_objects_still_exist: Code = "2BP01"
        
        // Class 2D — Invalid Transaction Termination
        public static let invalid_transaction_termination: Code = "2D000"
        
        // Class 2F — SQL Routine Exception
        public static let sql_routine_exception: Code = "2F000"
        public static let function_executed_no_return_statement: Code = "2F005"
        public static let modifying_sql_data_not_permitted: Code = "2F002"
        public static let prohibited_sql_statement_attempted: Code = "2F003"
        public static let reading_sql_data_not_permitted: Code = "2F004"
        
        // Class 34 — Invalid Cursor Name
        public static let invalid_cursor_name: Code = "34000"
        
        // Class 38 — External Routine Exception
        public static let external_routine_exception: Code = "38000"
        public static let containing_sql_not_permitted: Code = "38001"
        public static let modifying_sql_data_not_permitted_external: Code = "38002"
        public static let prohibited_sql_statement_attempted_external: Code = "38003"
        public static let reading_sql_data_not_permitted_external: Code = "38004"
        
        // Class 39 — External Routine Invocation Exception
        public static let external_routine_invocation_exception: Code = "39000"
        public static let invalid_sqlstate_returned: Code = "39001"
        public static let null_value_not_allowed_external: Code = "39004"
        public static let trigger_protocol_violated: Code = "39P01"
        public static let srf_protocol_violated: Code = "39P02"
        public static let event_trigger_protocol_violated: Code = "39P03"
        
        // Class 3B — Savepoint Exception
        public static let savepoint_exception: Code = "3B000"
        public static let invalid_savepoint_specification: Code = "3B001"
        
        // Class 3D — Invalid Catalog Name
        public static let invalid_catalog_name: Code = "3D000"
        
        // Class 3F — Invalid Schema Name
        public static let invalid_schema_name: Code = "3F000"
        
        // Class 40 — Transaction Rollback
        public static let transaction_rollback: Code = "40000"
        public static let transaction_integrity_constraint_violation: Code = "40002"
        public static let serialization_failure: Code = "40001"
        public static let statement_completion_unknown: Code = "40003"
        public static let deadlock_detected: Code = "40P01"
        
        // Class 42 — Syntax Error or Access Rule Violation
        public static let syntax_error_or_access_rule_violation: Code = "42000"
        public static let syntax_error: Code = "42601"
        public static let insufficient_privilege: Code = "42501"
        public static let cannot_coerce: Code = "42846"
        public static let grouping_error: Code = "42803"
        public static let windowing_error: Code = "42P20"
        public static let invalid_recursion: Code = "42P19"
        public static let invalid_foreign_key: Code = "42830"
        public static let invalid_name: Code = "42602"
        public static let name_too_long: Code = "42622"
        public static let reserved_name: Code = "42939"
        public static let datatype_mismatch: Code = "42804"
        public static let indeterminate_datatype: Code = "42P18"
        public static let collation_mismatch: Code = "42P21"
        public static let indeterminate_collation: Code = "42P22"
        public static let wrong_object_type: Code = "42809"
        public static let undefined_column: Code = "42703"
        public static let undefined_function: Code = "42883"
        public static let undefined_table: Code = "42P01"
        public static let undefined_parameter: Code = "42P02"
        public static let undefined_object: Code = "42704"
        public static let duplicate_column: Code = "42701"
        public static let duplicate_cursor: Code = "42P03"
        public static let duplicate_database: Code = "42P04"
        public static let duplicate_function: Code = "42723"
        public static let duplicate_prepared_statement: Code = "42P05"
        public static let duplicate_schema: Code = "42P06"
        public static let duplicate_table: Code = "42P07"
        public static let duplicate_alias: Code = "42712"
        public static let duplicate_object: Code = "42710"
        public static let ambiguous_column: Code = "42702"
        public static let ambiguous_function: Code = "42725"
        public static let ambiguous_parameter: Code = "42P08"
        public static let ambiguous_alias: Code = "42P09"
        public static let invalid_column_reference: Code = "42P10"
        public static let invalid_column_definition: Code = "42611"
        public static let invalid_cursor_definition: Code = "42P11"
        public static let invalid_database_definition: Code = "42P12"
        public static let invalid_function_definition: Code = "42P13"
        public static let invalid_prepared_statement_definition: Code = "42P14"
        public static let invalid_schema_definition: Code = "42P15"
        public static let invalid_table_definition: Code = "42P16"
        public static let invalid_object_definition: Code = "42P17"
        
        // Class 44 — WITH CHECK OPTION Violation
        public static let with_check_option_violation: Code = "44000"
        
        // Class 53 — Insufficient Resources
        public static let insufficient_resources: Code = "53000"
        public static let disk_full: Code = "53100"
        public static let out_of_memory: Code = "53200"
        public static let too_many_connections: Code = "53300"
        public static let configuration_limit_exceeded: Code = "53400"
        
        // Class 54 — Program Limit Exceeded
        public static let program_limit_exceeded: Code = "54000"
        public static let statement_too_complex: Code = "54001"
        public static let too_many_columns: Code = "54011"
        public static let too_many_arguments: Code = "54023"
        
        // Class 55 — Object Not In Prerequisite State
        public static let object_not_in_prerequisite_state: Code = "55000"
        public static let object_in_use: Code = "55006"
        public static let cant_change_runtime_param: Code = "55P02"
        public static let lock_not_available: Code = "55P03"
        
        // Class 57 — Operator Intervention
        public static let operator_intervention: Code = "57000"
        public static let query_canceled: Code = "57014"
        public static let admin_shutdown: Code = "57P01"
        public static let crash_shutdown: Code = "57P02"
        public static let cannot_connect_now: Code = "57P03"
        public static let database_dropped: Code = "57P04"
        
        // Class 58 — System Error (errors external to PostgreSQL itself)
        public static let system_error: Code = "58000"
        public static let io_error: Code = "58030"
        public static let undefined_file: Code = "58P01"
        public static let duplicate_file: Code = "58P02"
        
        // Class 72 — Snapshot Failure
        public static let snapshot_too_old: Code = "72000"
        
        // Class F0 — Configuration File Error
        public static let config_file_error: Code = "F0000"
        public static let lock_file_exists: Code = "F0001"
        
        // Class HV — Foreign Data Wrapper Error (SQL/MED)
        public static let fdw_error: Code = "HV000"
        public static let fdw_column_name_not_found: Code = "HV005"
        public static let fdw_dynamic_parameter_value_needed: Code = "HV002"
        public static let fdw_function_sequence_error: Code = "HV010"
        public static let fdw_inconsistent_descriptor_information: Code = "HV021"
        public static let fdw_invalid_attribute_value: Code = "HV024"
        public static let fdw_invalid_column_name: Code = "HV007"
        public static let fdw_invalid_column_number: Code = "HV008"
        public static let fdw_invalid_data_type: Code = "HV004"
        public static let fdw_invalid_data_type_descriptors: Code = "HV006"
        public static let fdw_invalid_descriptor_field_identifier: Code = "HV091"
        public static let fdw_invalid_handle: Code = "HV00B"
        public static let fdw_invalid_option_index: Code = "HV00C"
        public static let fdw_invalid_option_name: Code = "HV00D"
        public static let fdw_invalid_string_length_or_buffer_length: Code = "HV090"
        public static let fdw_invalid_string_format: Code = "HV00A"
        public static let fdw_invalid_use_of_null_pointer: Code = "HV009"
        public static let fdw_too_many_handles: Code = "HV014"
        public static let fdw_out_of_memory: Code = "HV001"
        public static let fdw_no_schemas: Code = "HV00P"
        public static let fdw_option_name_not_found: Code = "HV00J"
        public static let fdw_reply_handle: Code = "HV00K"
        public static let fdw_schema_not_found: Code = "HV00Q"
        public static let fdw_table_not_found: Code = "HV00R"
        public static let fdw_unable_to_create_execution: Code = "HV00L"
        public static let fdw_unable_to_create_reply: Code = "HV00M"
        public static let fdw_unable_to_establish_connection: Code = "HV00N"
        
        // Class P0 — PL/pgSQL Error
        public static let plpgsql_error: Code = "P0000"
        public static let raise_exception: Code = "P0001"
        public static let no_data_found: Code = "P0002"
        public static let too_many_rows: Code = "P0003"
        public static let assert_failure: Code = "P0004"
        
        // Class XX — Internal Error
        public static let internal_error: Code = "XX000"
        public static let data_corrupted: Code = "XX001"
        public static let index_corrupted: Code = "XX002"
        
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
        case .protocol: return .internal_error
        case .server(let server):
            guard let code = server.fields[.sqlState] else {
                return .internal_error
            }
            return Code(raw: code)
        case .connectionClosed: return .internal_error
        }
    }
}
