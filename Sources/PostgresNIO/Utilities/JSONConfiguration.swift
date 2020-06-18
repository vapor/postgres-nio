//
//  JSONConfiguration.swift
//  
//
//  Created by Caleb Wren on 6/17/20.
//

import Foundation

public struct JSONConfiguration {
    public static var global: JSONConfiguration = .init()
            
    public var encoder: JSONEncoder = JSONEncoder()
    
    public var decoder: JSONDecoder = JSONDecoder()
}
