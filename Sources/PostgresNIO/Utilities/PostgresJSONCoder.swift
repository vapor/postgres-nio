//
//  PostgresJSONCoder.swift
//  
//
//  Created by Caleb Wren on 6/17/20.
//

import Foundation

public struct PostgresJSONCoder {
    public static var global: PostgresJSONCoder = .init()
            
    public var encoder: JSONEncoder = JSONEncoder()
    
    public var decoder: JSONDecoder = JSONDecoder()
}
