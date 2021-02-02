//
//  File.swift
//  
//
//  Created by Fabian Fett on 02.02.21.
//

@testable import PostgresNIO
import Foundation

extension PSQLFrontendMessage.Encoder {
    static var forTests: Self {
        Self(jsonEncoder: JSONEncoder())
    }
}

extension PSQLDecodingContext {
    static func forTests(columnName: String = "unknown", columnIndex: Int = 0, jsonDecoder: PSQLJSONDecoder = JSONDecoder(), file: String = #file, line: Int = #line) -> Self {
        Self(jsonDecoder: JSONDecoder(), columnName: columnName, columnIndex: columnIndex, file: file, line: line)
    }
}

extension PSQLEncodingContext {
    static var forTests: Self {
        Self(jsonEncoder: JSONEncoder())
    }
}
