// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight.Connection {

    /// Execute semicolon-separated SQL statements and call an optional closure for each result row.
    ///
    /// - Parameters:
    ///   - sql: the SQL statement(s) to execute
    ///   - callback: closure to receive each result row, return true to continue or false to abort.
    ///               Aborting will cause an error to be thrown.
    ///               The closure receives the 1-based row number and a dictionary of column values.
    ///
    /// - Throws: ``SQLight/Error`` if there was a problem or execution was deliberately aborted.
    ///
    func execute(sql: String, callback: ((Int, [String: String?]) -> Bool)? = nil) throws {
        var errorMessagePtr: UnsafeMutablePointer<CChar>? = nil

        var context: UnsafeMutableRawPointer? = nil
        var wrapper: ExecCallbackWrapper?  // this the only retained reference to the wrapper
        if let callback {
            wrapper = ExecCallbackWrapper(callback)
            context = UnsafeMutableRawPointer(Unmanaged.passUnretained(wrapper!).toOpaque())
        }

        let rc = SQLite3.sqlite3_exec(self.sqlite3ptr,
                                      sql,
                                      (callback != nil) ? execCallback : nil,
                                      context,
                                      &errorMessagePtr)

        guard rc == SQLite3.SQLITE_OK else {
            if let errorMessagePtr {
                let error = SQLight.Error.resultMessage(.fromSQLite(code: rc), String(cString: errorMessagePtr))
                SQLite3.sqlite3_free(errorMessagePtr)
                throw error
            } else {
                throw SQLight.Error.result(.fromSQLite(code: rc))
            }
        }
    }
}

// Wrapper for the callback closure to allow it to be passed through sqlite3_exec
fileprivate class ExecCallbackWrapper {
    private let callback: (Int, [String: String?]) -> Bool
    var rowNumber = 0
    init(_ callback: @escaping (Int, [String : String?]) -> Bool) {
        self.callback = callback
    }

    func callback(_ row: [String: String?]) -> Bool {
        rowNumber += 1
        return callback(rowNumber, row)
    }
}

// Function pointer passed to sqlite3_exec.
// Swift wrapper object for callback closure is passed as the context pointer.
fileprivate func execCallback(_ context: UnsafeMutableRawPointer?,
                              _ colCount: Int32,
                              _ colTexts: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?,
                              _ colNames: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> Int32 {

    guard let colTexts, let colNames, let context else {
        SQLight.logger.debug("Unexpected null(s) in sqlite3_exec callback args")
        return SQLite3.SQLITE_ABORT
    }

    let wrapper: ExecCallbackWrapper = Unmanaged.fromOpaque(context).takeUnretainedValue()

    var values = [String: String?]()

    if colCount > 0 {
        for col in 0..<Int(colCount) {
            let name: String
            if let colName = colNames[col] {
                name = String(cString: colName)
            } else {
                name = "<???>"
            }

            if let colValue = colTexts[col] {
                values.updateValue(String(cString: colValue), forKey: name)
            } else {
                values.updateValue(nil, forKey: name)
            }
        }
    }

    guard wrapper.callback(values) else {
        // callback requested no more rows
        return SQLite3.SQLITE_ABORT
    }

    return SQLite3.SQLITE_OK
}
