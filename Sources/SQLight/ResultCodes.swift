// Copyright (c) 2024 David N Main

import Foundation
import SQLite3

public extension SQLight {

    /// Errors that can arise during SQLite operations
    enum Error: LocalizedError, CustomStringConvertible {
        case result(ResultCode)
        case message(String)
        case resultMessage(ResultCode, String)

        public var description: String { errorDescription! }
        public var errorDescription: String? {
            switch self {
            case .result(let code): "SQLite error: \(code.description)"
            case .message(let msg): "SQLite error: \(msg)"
            case .resultMessage(let code, let msg): "SQLite error: \(code.description) - \(msg)"
            }
        }
    }

    /// The common SQLite result codes
    enum ResultCode {
        case other(Int32)
        case ok, error, internal_, perm, abort, busy, locked, nomem, readonly, interrupt,
             ioerr, corrupt, notfound, full, cantopen, protocol_, empty, schema, toobig,
             constraint, mismatch, misuse, nolfs, auth, format, range, notadb, notice,
             warning, row, done

        public static func fromSQLite(code: Int32) -> ResultCode {
            switch code {
            case SQLite3.SQLITE_OK:         .ok
            case SQLite3.SQLITE_ERROR:      .error
            case SQLite3.SQLITE_INTERNAL:   .internal_
            case SQLite3.SQLITE_PERM:       .perm
            case SQLite3.SQLITE_ABORT:      .abort
            case SQLite3.SQLITE_BUSY:       .busy
            case SQLite3.SQLITE_LOCKED:     .locked
            case SQLite3.SQLITE_NOMEM:      .nomem
            case SQLite3.SQLITE_READONLY:   .readonly
            case SQLite3.SQLITE_INTERRUPT:  .interrupt
            case SQLite3.SQLITE_IOERR:      .ioerr
            case SQLite3.SQLITE_CORRUPT:    .corrupt
            case SQLite3.SQLITE_NOTFOUND:   .notfound
            case SQLite3.SQLITE_FULL:       .full
            case SQLite3.SQLITE_CANTOPEN:   .cantopen
            case SQLite3.SQLITE_PROTOCOL:   .protocol_
            case SQLite3.SQLITE_EMPTY:      .empty
            case SQLite3.SQLITE_SCHEMA:     .schema
            case SQLite3.SQLITE_TOOBIG:     .toobig
            case SQLite3.SQLITE_CONSTRAINT: .constraint
            case SQLite3.SQLITE_MISMATCH:   .mismatch
            case SQLite3.SQLITE_MISUSE:     .misuse
            case SQLite3.SQLITE_NOLFS:      .nolfs
            case SQLite3.SQLITE_AUTH:       .auth
            case SQLite3.SQLITE_FORMAT:     .format
            case SQLite3.SQLITE_RANGE:      .range
            case SQLite3.SQLITE_NOTADB:     .notadb
            case SQLite3.SQLITE_NOTICE:     .notice
            case SQLite3.SQLITE_WARNING:    .warning
            case SQLite3.SQLITE_ROW:        .row
            case SQLite3.SQLITE_DONE:       .done
            default: .other(code)
            }
        }

        public var description: String {
            switch self {
            case .ok:         "Successful result"
            case .error:      "Generic error"
            case .internal_:  "Internal logic error in SQLite"
            case .perm:       "Access permission denied"
            case .abort:      "Callback routine requested an abort"
            case .busy:       "The database file is locked"
            case .locked:     "A table in the database is locked"
            case .nomem:      "A malloc() failed"
            case .readonly:   "Attempt to write a readonly database"
            case .interrupt:  "Operation terminated by sqlite3_interrupt()"
            case .ioerr:      "Some kind of disk I/O error occurred"
            case .corrupt:    "The database disk image is malformed"
            case .notfound:   "Unknown opcode in sqlite3_file_control()"
            case .full:       "Insertion failed because database is full"
            case .cantopen:   "Unable to open the database file"
            case .protocol_:  "Database lock protocol error"
            case .empty:      "Internal use only"
            case .schema:     "The database schema changed"
            case .toobig:     "String or BLOB exceeds size limit"
            case .constraint: "Abort due to constraint violation"
            case .mismatch:   "Data type mismatch"
            case .misuse:     "Library used incorrectly"
            case .nolfs:      "Uses OS features not supported on host"
            case .auth:       "Authorization denied"
            case .format:     "Not used"
            case .range:      "2nd parameter to sqlite3_bind out of range"
            case .notadb:     "File opened that is not a database file"
            case .notice:     "Notifications from sqlite3_log()"
            case .warning:    "Warnings from sqlite3_log()"
            case .row:        "sqlite3_step() has another row ready"
            case .done:       "sqlite3_step() has finished executing"
            case .other(let code): "other(\(code))"
            }
        }
    }
}
