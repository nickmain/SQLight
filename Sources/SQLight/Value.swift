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
    enum Value: Hashable {
        case null
        case int(Int)
        case double(Double)
        case string(String)
        case data(Data)

        // Special type that tells SQLite to copy string or blob binding
        internal static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

        // init from a sqlite3_value pointer
        internal init(from ptr: OpaquePointer?) {
            guard let ptr else {
                self = .null
                return
            }

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

public extension SQLight.Value {

    /// Coerce value to an Int, if it is a double, int or a parsable string
    var asInt: Int? {
        switch self {
        case .int(let value): value
        case .double(let value): Int(value)
        case .string(let value): Int(value)
        default: nil
        }
    }

    /// Coerce value to a Double, if it is a double, int or a parsable string
    var asDouble: Double? {
        switch self {
        case .int(let value): Double(value)
        case .double(let value): value
        case .string(let value): Double(value)
        default: nil
        }
    }

    /// Coerce value to a String, if it is a string or a number
    var asString: String? {
        switch self {
        case .int(let value): "\(value)"
        case .double(let value): "\(value)"
        case .string(let value): value
        default: nil
        }
    }

    /// Coerce value to a Data, only if it is already data
    var asData: Data? {
        switch self {
        case .data(let value): value
        default: nil
        }
    }

    /// Whether value is a null
    var isNull: Bool {
        switch self {
        case .null: true
        default: false
        }
    }

    /// Coerce value to an int value, if it is a double, int or a parsable string
    var asIntValue: SQLight.Value? {
        if let value = self.asInt { .int(value) } else { nil }
    }

    /// Coerce value to a double value, if it is a double, int or a parsable string
    var asDoubleValue: SQLight.Value? {
        if let value = self.asDouble { .double(value) } else { nil }
    }

    /// Coerce value to a string value, if it is a string or a number
    var asStringValue: SQLight.Value? {
        if let value = self.asString { .string(value) } else { nil }
    }

    /// Coerce value to a data value, only if it is already data
    var asDataValue: SQLight.Value? {
        if let value = self.asData { .data(value) } else { nil }
    }

    /// Coerce value to a null value, only if it is already null
    var asNullValue: SQLight.Value? {
        if self.isNull { .null } else { nil }
    }

    /// Coerce a value to the type of this one
    func coerce(value: SQLight.Value) -> SQLight.Value? {
        switch self {
        case .null:      value.asNullValue
        case .int(_):    value.asIntValue
        case .double(_): value.asDoubleValue
        case .string(_): value.asStringValue
        case .data(_):   value.asDataValue
        }
    }

    /// Compare against another value, coercing types as appropriate.
    ///
    /// - Returns: nil if the types are not comparable, or coercible
    func greaterThan(value: SQLight.Value) -> Bool? {
        switch self {
        case .int(let this): if case let .double(doubleValue) = value { Double(this) > doubleValue } 
                              else if let otherInt = value.asInt    { this > otherInt    } else { nil }
        case .double(let this): if let otherDouble = value.asDouble { this > otherDouble } else { nil }
        case .string(let this): if let otherString = value.asString { this > otherString } else { nil }
        default: nil
        }
    }

    /// Compare against another value, coercing types as appropriate.
    ///
    /// - Returns: nil if the types are not comparable, or coercible
    func lessOrEqual(value: SQLight.Value) -> Bool? {
        switch self {
        case .int(let this): if case let .double(doubleValue) = value { Double(this) > doubleValue }
                              else if let otherInt = value.asInt    { this <= otherInt    } else { nil }
        case .double(let this): if let otherDouble = value.asDouble { this <= otherDouble } else { nil }
        case .string(let this): if let otherString = value.asString { this <= otherString } else { nil }
        default: nil
        }
    }

    /// Compare against another value, coercing types as appropriate.
    ///
    /// - Returns: nil if the types are not comparable, or coercible
    func lessThan(value: SQLight.Value) -> Bool? {
        switch self {
        case .int(let this): if case let .double(doubleValue) = value { Double(this) > doubleValue }
                              else if let otherInt = value.asInt    { this < otherInt    } else { nil }
        case .double(let this): if let otherDouble = value.asDouble { this < otherDouble } else { nil }
        case .string(let this): if let otherString = value.asString { this < otherString } else { nil }
        default: nil
        }
    }

    /// Compare against another value, coercing types as appropriate.
    ///
    /// - Returns: nil if the types are not comparable, or coercible
    func greaterOrEqual(value: SQLight.Value) -> Bool? {
        switch self {
        case .int(let this): if case let .double(doubleValue) = value { Double(this) > doubleValue }
                              else if let otherInt = value.asInt    { this >= otherInt    } else { nil }
        case .double(let this): if let otherDouble = value.asDouble { this >= otherDouble } else { nil }
        case .string(let this): if let otherString = value.asString { this >= otherString } else { nil }
        default: nil
        }
    }

    /// Test a value against a constraint.
    ///
    /// TODO: flesh this out
    func testConstraint(op: SQLight.Table.Index.Constraint.Operator, arg: SQLight.Value) -> Bool {
        switch op {
        case .unknown: false
        case .equal: self == coerce(value: arg)
        case .greaterThan:    self.greaterThan(value: arg) ?? false
        case .lessOrEqual:    self.lessOrEqual(value: arg) ?? false
        case .lessThan:       self.lessThan(value: arg) ?? false
        case .greaterOrEqual: self.greaterOrEqual(value: arg) ?? false
        case .match: false // TODO:
        case .like: false // TODO:
        case .glob: false // TODO:
        case .regex: false // TODO:
        case .notEqual: self != coerce(value: arg)
        case .isNot: false // TODO:
        case .isNotNull: !self.isNull
        case .isNull: self.isNull
        case .is_:  false // TODO:
        case .limit: false   // Does not have a LHS
        case .offset: false  // Does not have a LHS
        case .function: false // TODO:
        }
    }
}
