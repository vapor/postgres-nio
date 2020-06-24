import Foundation

public struct PostgresJSONCoder {
    public static var global: PostgresJSONCoder = .init()
            
    public var encoder: JSONEncoder = JSONEncoder()
    
    public var decoder: JSONDecoder = JSONDecoder()
}
