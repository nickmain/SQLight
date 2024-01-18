// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight.PreparedStatement {

    // MARK: - Accessing result columns

    /// The number of columns in the current result row.
    var rowColumnCount: Int {
        Int(SQLite3.sqlite3_data_count(statementPtr))
    }

    /// Get the type of a column.
    ///
    /// The type is undefined if an automatic type conversion has occured on the column.
    ///
    /// Refer to [SQLite documentation "Result Values From A Query"](https://www.sqlite.org/c3ref/column_blob.html)
    /// for details of the underlying API and automatic type conversions.
    ///
    /// - Returns: the type or ``SQLight/ValueType/null`` if the call result is undefined.
    func columnType(at index: Int) -> SQLight.ValueType {
        .init(rawValue: Int(SQLite3.sqlite3_column_type(statementPtr, Int32(index)))) ?? .null
    }

    /// Whether a column is null
    func isColumnNull(at index: Int) -> Bool {
        SQLite3.sqlite3_column_type(statementPtr, Int32(index)) == SQLight.ValueType.null.rawValue
    }

    /// Get the value of a column as a Double
    func doubleValue(at index: Int) -> Double {
        SQLite3.sqlite3_column_double(statementPtr, Int32(index))
    }

    /// Get the value of a column as an Int
    func integerValue(at index: Int) -> Int {
        Int(SQLite3.sqlite3_column_int64(statementPtr, Int32(index)))
    }

    /// Get the value of a column as a String, using UTF-8 encoding.
    ///
    /// - Returns: nil if the actual value was null
    func stringValue(at index: Int) -> String? {
        let ptr = SQLite3.sqlite3_column_text(statementPtr, Int32(index))
        guard let ptr else { return nil }

        let charPtr: UnsafePointer<CChar> = .init(OpaquePointer(ptr))

        return String(cString: charPtr, encoding: .utf8)
    }

    /// Get the value of a column as Data
    ///
    /// - Returns: nil if the actual value was null
    func dataValue(at index: Int) -> Data? {
        let ptr = SQLite3.sqlite3_column_blob(statementPtr, Int32(index))
        guard let ptr else { return nil }

        let size = SQLite3.sqlite3_column_bytes(statementPtr, Int32(index))
        return Data(bytes: ptr, count: Int(size))
    }

    /// Get a column as a wrapped value
    ///
    /// - Returns: the value as its actual type or ``SQLight/Value/null`` if the column type is undefined
    func value(at index: Int) -> SQLight.Value {
        switch columnType(at: index) {
        case .int:    .int(integerValue(at: index))
        case .double: .double(doubleValue(at: index))
        case .string: if let value = stringValue(at: index) { .string(value) } else { .null }
        case .data:   if let value = dataValue(at: index) { .data(value) } else { .null }
        case .null:   .null
        }
    }
}
