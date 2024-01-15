// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight.PreparedStatement {

    // MARK: - Binding statement parameters

    /// Get the number of parameters in the statement.
    ///
    /// See [sqlite3 documentation](https://www.sqlite.org/c3ref/bind_parameter_count.html)
    /// for details and the special case of numeric parameter names.
    var parameterCount: Int { Int(SQLite3.sqlite3_bind_parameter_count(statementPtr)) }

    /// Get the index of a parameter from its name.
    ///
    /// - Returns: nil if the name is not found
    func indexOfParameter(named name: String) -> Int? {
        let index = Int(SQLite3.sqlite3_bind_parameter_index(statementPtr, name))
        guard index > 0 else { return nil }
        return index
    }

    /// Get the name of the parameter at an index.
    ///
    /// Parameter indexes are 1-based.
    ///
    /// - Returns: nil if the index is out of range or the parameter is unnamed
    func nameOfParameter(at index: Int) -> String? {
        let ptr = SQLite3.sqlite3_bind_parameter_name(statementPtr, Int32(index))
        guard let ptr else { return nil }
        return String(cString: ptr)
    }

    /// Bind a parameter to an integer value
    func bindParameter(at index: Int, to value: Int) throws {
        let rc = SQLite3.sqlite3_bind_int64(statementPtr, Int32(index), Int64(value))
        guard rc == SQLite3.SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }
    }

    /// Bind a parameter to a double value
    func bindParameter(at index: Int, to value: Double) throws {
        let rc = SQLite3.sqlite3_bind_double(statementPtr, Int32(index), value)
        guard rc == SQLite3.SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }
    }

    /// Bind a parameter to a string value
    func bindParameter(at index: Int, to value: String) throws {
        let rc = SQLite3.sqlite3_bind_text(statementPtr, Int32(index), value, -1, SQLight.Value.SQLITE_TRANSIENT)
        guard rc == SQLite3.SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }
    }

    /// Bind a parameter to a data value (blob)
    func bindParameter(at index: Int, to value: Data) throws {
        try value.withUnsafeBytes { (ptr: UnsafeRawBufferPointer) in
            let rc = SQLite3.sqlite3_bind_blob(statementPtr, Int32(index), ptr.baseAddress, Int32(ptr.count), SQLight.Value.SQLITE_TRANSIENT)
            guard rc == SQLite3.SQLITE_OK else {
                throw SQLight.Error.result(.fromSQLite(code: rc))
            }
        }
    }

    /// Bind a parameter to a zeroed blob value
    func bindParameter(at index: Int, toZeroBlobOfSize size: Int) throws {
        let rc = SQLite3.sqlite3_bind_zeroblob(statementPtr, Int32(index), Int32(size))
        guard rc == SQLite3.SQLITE_OK else {
            throw SQLight.Error.result(.fromSQLite(code: rc))
        }
    }
}
