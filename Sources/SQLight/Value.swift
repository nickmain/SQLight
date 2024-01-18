// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// The SQLite type codes
    enum ValueType: Int {
        /// 64 bit integer
        case int = 1

        /// 64 bit floating point
        case double = 2

        /// The SQLite TEXT type
        case string = 3

        /// The SQLite BLOB type
        case data = 4

        /// The column value is null
        case null = 5

        /// The SQLite storage class of the type
        public var sqlStorageClass: String {
            switch self {
            case .int:    "INTEGER"
            case .double: "REAL"
            case .string: "TEXT"
            case .data:   "BLOB"
            case .null:   "NULL"
            }
        }
    }

    /// A value of one of the possible SQLite types
    enum Value: Equatable {
        case null
        case int(Int)
        case double(Double)
        case string(String)
        case data(Data)

        // Special type that tells SQLite to copy string or blob binding
        internal static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

        // init from a sqlite3_value pointer
        internal init(from ptr: OpaquePointer) {

            let typeCode = SQLite3.sqlite3_value_type(ptr)
            let type = ValueType(rawValue: Int(typeCode)) ?? .null

            switch type {
            case .int:    self = .int(Int(SQLite3.sqlite3_value_int64(ptr)))
            case .double: self = .double(SQLite3.sqlite3_value_double(ptr))
            case .null:   self = .null
            case .string:
                if let bytePtr = SQLite3.sqlite3_value_text(ptr) {
                    let charPtr: UnsafePointer<CChar> = .init(OpaquePointer(bytePtr))
                    if let s = String(cString: charPtr, encoding: .utf8) {
                        self = .string(s)
                    } else {
                        self = .null
                    }
                } else {
                    self = .null
                }
            case .data:
                if let bytePtr = SQLite3.sqlite3_value_blob(ptr) {
                    let size = SQLite3.sqlite3_value_bytes(ptr)
                    self = .data(Data(bytes: bytePtr, count: Int(size)))
                } else {
                    self = .null
                }
            }
        }
    }
}
