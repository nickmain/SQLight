// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight.Connection {

    /// Register a virtual table module with the connection. 
    ///
    /// This must be done before any virtual tables are created using the module or any previously
    /// created tables in the schema are used.
    ///
    /// The passed ``SQLight/Module`` instance is not strongly referenced by the registration and
    /// should be held onto elsewhere.
    ///
    /// See ["Register A Virtual Table Implementation"](https://www.sqlite.org/c3ref/create_module.html) for more detail.
    func register(module: SQLight.Module) throws {
        try module.register(with: sqlite3ptr)
    }

}

public extension SQLight {

    /// Base class for virtual table module implementations.
    ///
    /// Use ``Connection/register(module:)`` to add a module to a database connection.
    class Module {
        
        /// The name of the module
        public let name: String

        public init(name: String) {
            self.name = name
        }

        // Strong references to tables to avoid premature deinitialization since the table
        // is the thing that owns the sqlite3_vtab that is passed to SQLite
        internal var tables = [Table]()

        /// Override this method to respond to requests to create new virtual tables.
        ///
        /// Do not call super.
        ///
        /// - Parameters:
        ///   - name: the name of the table to create
        ///   - schema: the schema name of the new table, otherwise "main" for the main schema and "temp" for a temporary table
        ///   - args: the additional arguments passed in the [create-virtual-table](https://www.sqlite.org/syntax/create-virtual-table-stmt.html) SQL statement
        ///
        /// - Throws: an error message that will be passed to SQLite. Use ``SQLight/Error/message(_:)``
        ///           otherwise the error's localized description will be used.
        ///
        open func createTable(name: String, schema: String, args: [String]) throws -> Table {
            SQLight.logger.warning("Module.createTable base implementation called - please extend and override")
            return Table(name: name, schema: schema, arguments: args)
        }

        /// Override this method to respond to requests to connect to a virtual table that was created in the database schema,
        /// in a previous database connection.
        ///
        /// Do not call super.
        ///
        /// - Parameters:
        ///   - name: the name of the table to connect to
        ///   - schema: the schema name of the table, otherwise "main" for the main schema and "temp" for a temporary table
        ///   - args: the additional arguments passed in the original [create-virtual-table](https://www.sqlite.org/syntax/create-virtual-table-stmt.html) SQL statement
        ///
        /// - Throws: an error message that will be passed to SQLite. Use ``SQLight/Error/message(_:)``
        ///           otherwise the error's localized description will be used.
        ///
        open func connectToTable(name: String, schema: String, args: [String]) throws -> Table {
            SQLight.logger.warning("Module.connectToTable base implementation called - please extend and override")
            return Table(name: name, schema: schema, arguments: args)
        }

        /// Release the strong reference to the given table.
        ///
        /// Only do this if absolutely necessary since this may invalidate the table structure that was passed
        /// to SQLite and may cause a crash.
        final public func release(table: Table) {
            tables.removeAll { $0 === table }
        }

        internal func register(with connectionPtr: OpaquePointer) throws {
            // ptr to pass unmanaged weak reference to module registration
            let selfPtr = UnsafeMutableRawPointer(Unmanaged.passUnretained(self).toOpaque())

            let rc = SQLite3.sqlite3_create_module(connectionPtr, name, &sqlite3Module, selfPtr)
            guard rc == SQLite3.SQLITE_OK else {
                throw Error.result(.fromSQLite(code: rc))
            }
        }

        // get the Module from the aux data passed to xCreate or xConnect
        internal static func from(auxPtr: UnsafeMutableRawPointer?) -> Module? {
            guard let auxPtr else { return nil }
            let module: Module = Unmanaged.fromOpaque(auxPtr).takeUnretainedValue()
            return module
        }
    }

    /// Allocate an error message that SQLite will later free using sqlite3_free()
    static func allocate(errorMsg: String) -> UnsafeMutablePointer<CChar> {
        withVaList([]) { SQLite3.sqlite3_vmprintf(errorMsg, $0) }
    }
}

// common module definition struct
fileprivate var sqlite3Module: SQLite3.sqlite3_module = .init(
    iVersion: 3,

    // tables
    xCreate:     xCreate(_:_:_:_:_:_:),
    xConnect:    xConnect(_:_:_:_:_:_:),
    xBestIndex:  xBestIndex(_:_:),
    xDisconnect: xDisconnect(_:),
    xDestroy:    xDestroy(_:),

    // cursors
    xOpen:   xOpen(_:_:),
    xClose:  xClose(_:),
    xFilter: xFilter(_:_:_:_:_:),
    xNext:   xNext(_:),
    xEof:    xEof(_:),
    xColumn: xColumn(_:_:_:),
    xRowid:  xRowid(_:_:),

    // inserts/deletes/updates
    xUpdate: xUpdate(_:_:_:_:),

    // transaction support not yet implemented
    xBegin:    nil, // xBegin(_:),
    xSync:     nil, // xSync(_:),
    xCommit:   nil, // xCommit(_:),
    xRollback: nil, // xRollback(_:),

    xFindFunction: nil, // not implemented

    // table renaming not supported
    xRename: nil, // xRename(_:_:),

    // savepoints not yet implemented
    xSavepoint:  nil, // xSavepoint(_:_:),
    xRelease:    nil, // xRelease(_:_:),
    xRollbackTo: nil, // xRollbackTo(_:_:),

    xShadowName: xShadowName(_:)
)

// Set an error message in the given ptr and return SQLITE_ERROR
fileprivate func setError(message: String, in pzErr: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> Int32 {
    if let pzErr {
        pzErr.pointee = SQLight.allocate(errorMsg: message)
    }
    return SQLite3.SQLITE_ERROR
}

// common create/connect
fileprivate func createOrConnect(isConnect: Bool,
                                 _ connectionPtr: OpaquePointer?, _ auxPtr: UnsafeMutableRawPointer?,
                                 _ argCount: Int32, _ argPtrs: UnsafePointer<UnsafePointer<CChar>?>?,
                                 _ ppVTab: UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab>?>?,
                                 _ pzErr: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> Int32 {

    guard let module = SQLight.Module.from(auxPtr: auxPtr) else {
        return setError(message: "module ptr missing in createOrConnect(isConnect:\(isConnect)) aux data", in: pzErr)
    }
    guard let argPtrs, argCount >= 3, let arg1 = argPtrs[1], let arg2 = argPtrs[2] else {
        return setError(message: "insufficient arguments for createOrConnect(isConnect:\(isConnect))", in: pzErr)
    }
    guard let ppVTab else {
        return setError(message: "null ppVTab in createOrConnect(isConnect:\(isConnect))", in: pzErr)
    }

    // argPtrs[0] is the module name
    let schemaName = String(cString: arg1)
    let tableName  = String(cString: arg2)
    var otherArgs = [String]()
    for argIndex in 3..<Int(argCount) {
        if let argPtr = argPtrs[argIndex] {
            otherArgs.append(String(cString: argPtr))
        }
    }

    let table: SQLight.Table
    do {
        table = isConnect ? try module.connectToTable(name: tableName, schema: schemaName, args: otherArgs)
                          : try module.createTable(name: tableName, schema: schemaName, args: otherArgs)
    } catch SQLight.Error.message(let message) {
        return setError(message: message, in: pzErr)
    } catch {
        return setError(message: error.localizedDescription, in: pzErr)
    }

    // declare the schema of the table
    let declarationSql = table.declarationSql
    let rc = SQLite3.sqlite3_declare_vtab(connectionPtr, declarationSql)
    guard rc == SQLite3.SQLITE_OK else {
        return rc
    }

    // ensure strong reference to table
    module.tables.append(table)
    table.sqlite3VTab.table = table // weak ref

    // return the table structure
    withUnsafeMutablePointer(to: &table.sqlite3VTab.sqlite3_vtab) {
        ppVTab.pointee = $0
    }

    return SQLite3.SQLITE_OK
}

fileprivate func xCreate(_ connectionPtr: OpaquePointer?, _ auxPtr: UnsafeMutableRawPointer?,
                         _ argCount: Int32, _ argPtrs: UnsafePointer<UnsafePointer<CChar>?>?,
                         _ ppVTab: UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab>?>?,
                         _ pzErr: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> Int32 {

    return createOrConnect(isConnect: false, connectionPtr, auxPtr, argCount, argPtrs, ppVTab, pzErr)
}

fileprivate func xConnect(_ connectionPtr: OpaquePointer?, _ auxPtr: UnsafeMutableRawPointer?,
                          _ argCount: Int32, _ argPtrs: UnsafePointer<UnsafePointer<CChar>?>?,
                          _ ppVTab: UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab>?>?,
                          _ pzErr: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> Int32 {

    return createOrConnect(isConnect: true, connectionPtr, auxPtr, argCount, argPtrs, ppVTab, pzErr)
}

// Extract Table from pointer
fileprivate func table(from pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> SQLight.Table? {
    guard let pVTab else {
        SQLight.logger.debug("missing pVTab")
        return nil
    }

    let pSQLiteVTable = UnsafeMutableRawPointer(pVTab).assumingMemoryBound(to: SQLiteVTable.self)
    guard let table = pSQLiteVTable.pointee.table else {
        SQLight.logger.debug("missing table in SQLiteVTable struct")
        return nil
    }
    return table
}

// Extract Cursor from pointer
fileprivate func cursor(from pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?) -> SQLight.Cursor? {
    guard let pCursor else {
        SQLight.logger.debug("missing pCursor")
        return nil
    }

    let pSQLiteCursor = UnsafeMutableRawPointer(pCursor).assumingMemoryBound(to: SQLiteCursor.self)
    guard let cursor = pSQLiteCursor.pointee.cursor else {
        SQLight.logger.debug("missing cursor in SQLiteCursor struct")
        return nil
    }
    return cursor
}

fileprivate func xBestIndex(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ pIndexInfo: UnsafeMutablePointer<sqlite3_index_info>?) -> Int32 {
    guard let table = table(from: pVTab), let pIndexInfo else { return SQLite3.SQLITE_ERROR }

    var constraints  = [SQLight.Table.Index.Constraint]()
    let constraintCount = Int(pIndexInfo.pointee.nConstraint)
    if constraintCount > 0 {
        for constraintIndex in 0..<constraintCount {
            let constraint = pIndexInfo.pointee.aConstraint[constraintIndex]
            let colIndex = Int(constraint.iColumn)
            let isUsable = constraint.usable != 0
            let operation = SQLight.Table.Index.Constraint.Operator.from(op: Int32(constraint.op))

            var argument: SQLight.Value?
            var pArgument: OpaquePointer? = nil
            SQLite3.sqlite3_vtab_rhs_value(pIndexInfo, Int32(constraintIndex), &pArgument)
            if let pArgument {
                argument = SQLight.Value(from: pArgument)
            }

            constraints.append(.init(constraintIndex: constraintIndex, columnIndex: colIndex, isUsable: isUsable, operation: operation, argument: argument))
        }
    }

    var orderByTerms = [SQLight.Table.Index.OrderByTerm]()
    let termCount = Int(pIndexInfo.pointee.nOrderBy)
    if termCount > 0 {
        for termIndex in 0..<termCount {
            let pTerm = pIndexInfo.pointee.aOrderBy[termIndex]
            orderByTerms.append(.init(columnIndex: Int(pTerm.iColumn), isDescending: pTerm.desc != 0))
        }
    }

    let info = SQLight.Table.Index.Info(constraints: constraints,
                                        orderByTerms: orderByTerms,
                                        colUsedBits: pIndexInfo.pointee.colUsed)

    switch table.bestIndexCaching(info: info) {
    case .none: return SQLite3.SQLITE_CONSTRAINT // no solution
    case .index(let index):
        if index.orderByConsumed {
            pIndexInfo.pointee.orderByConsumed = 1
        }
        if let rowCount = index.estimatedRows {
            pIndexInfo.pointee.estimatedRows = Int64(rowCount)
        }
        if index.zeroOrOneRow {
            pIndexInfo.pointee.idxFlags = SQLite3.SQLITE_INDEX_SCAN_UNIQUE
        }
        pIndexInfo.pointee.estimatedCost = index.estimatedCost

        // set usage order for those constraints used in the index
        if let pConstraintUsage = pIndexInfo.pointee.aConstraintUsage {
            for (constraintIndex, constraint) in index.constraints.enumerated() {
                pConstraintUsage[constraint.constraintIndex].argvIndex = Int32(constraintIndex + 1)
            }
        }

        return SQLite3.SQLITE_OK
    }
}

fileprivate func xDisconnect(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
    guard let table = table(from: pVTab) else { return SQLite3.SQLITE_ERROR }
    table.disconnect()
    return SQLite3.SQLITE_OK
}

fileprivate func xDestroy(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
    guard let table = table(from: pVTab) else { return SQLite3.SQLITE_ERROR }
    table.destroy()
    return SQLite3.SQLITE_OK
}

fileprivate func xOpen(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ ppCursor: UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab_cursor>?>?) -> Int32 {
    guard let table = table(from: pVTab), let ppCursor else { return SQLite3.SQLITE_ERROR }
    let cursor = table.openCursor()

    withUnsafeMutablePointer(to: &cursor.sqliteCursor.sqlite3_vtab_cursor) {
        ppCursor.pointee = $0
    }

    return SQLite3.SQLITE_OK
}

fileprivate func xClose(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?) -> Int32 {
    guard let cursor = cursor(from: pCursor) else { return SQLite3.SQLITE_ERROR }
    cursor.close()
    cursor.sqliteCursor.cursor = nil // release the cursor
    return SQLite3.SQLITE_OK
}

fileprivate func xFilter(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?, _ indexNum: Int32, _ indexString: UnsafePointer<CChar>?, _ argCount: Int32, _ ppArgs: UnsafeMutablePointer<OpaquePointer?>?) -> Int32 {
    guard let cursor = cursor(from: pCursor), 
          let table = cursor.table,
          let index = table.getIndex(at: Int(indexNum))
    else { return SQLite3.SQLITE_ERROR }

    var args = [SQLight.Value]()

    // gather constraint args
    if let ppArgs, argCount == index.constraints.count {
        for argIndex in 0..<Int(argCount) {
            let value = SQLight.Value(from: ppArgs[argIndex])
            args.append(value)
        }
    } else {
        if index.constraints.count > 0 {
            SQLight.logger.debug("xFilter args do not match constraints in index")
            return SQLite3.SQLITE_ERROR
        }
    }

    cursor.filter(index: index, arguments: args)
    return SQLite3.SQLITE_OK
}

fileprivate func xNext(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?) -> Int32 {
    guard let cursor = cursor(from: pCursor) else { return SQLite3.SQLITE_ERROR }
    guard cursor.next() else { return SQLite3.SQLITE_ERROR }
    return SQLite3.SQLITE_OK
}

fileprivate func xEof(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?) -> Int32 {
    guard let cursor = cursor(from: pCursor) else { return 1 }
    return cursor.hasCurrentRow ? 0 : 1
}

fileprivate func xColumn(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?, _ pContext: OpaquePointer?, _ colIndex: Int32) -> Int32 {
    guard let cursor = cursor(from: pCursor), let pContext else { return SQLite3.SQLITE_ERROR }
    if let value = cursor.columnValue(at: Int(colIndex)) {
        value.setReturnValue(for: pContext)
    }

    return SQLite3.SQLITE_OK
}

fileprivate func xRowid(_ pCursor: UnsafeMutablePointer<sqlite3_vtab_cursor>?, _ pRowId: UnsafeMutablePointer<sqlite3_int64>?) -> Int32 {
    guard let cursor = cursor(from: pCursor), let pRowId else { return SQLite3.SQLITE_ERROR }
    let id = cursor.currentRowId
    pRowId.pointee = Int64(id)
    return SQLite3.SQLITE_OK
}

fileprivate func xUpdate(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ argCount: Int32, _ ppArgs:     UnsafeMutablePointer<OpaquePointer?>?, _ pRowId: UnsafeMutablePointer<sqlite3_int64>?) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xBegin(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xSync(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xCommit(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xRollback(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xRename(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ pNewName: UnsafePointer<CChar>?) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xSavepoint(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ savepoint: Int32) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xRelease(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ savepoint: Int32) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xRollbackTo(_ pVTab: UnsafeMutablePointer<sqlite3_vtab>?, _ savepoint: Int32) -> Int32 {
    // TODO
    return SQLite3.SQLITE_OK
}

fileprivate func xShadowName(_ name: UnsafePointer<CChar>?) -> Int32 {
    // TODO
    return 0 // false
}
