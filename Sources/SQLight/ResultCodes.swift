// Copyright (c) 2026 David N Main

import Foundation
import SQLiteCore

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

        /// Get the underlying SQLite error code (or SQLITE_ERROR if none)
        public var sqliteCode: Int32 {
            switch self {
            case .result(let code): code.asSQLiteCode
            case .message(_): SQLITE_ERROR
            case .resultMessage(let code, _): code.asSQLiteCode
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

        public var asSQLiteCode: Int32 {
            switch self {
            case .ok        : SQLITE_OK
            case .error     : SQLITE_ERROR
            case .internal_ : SQLITE_INTERNAL
            case .perm      : SQLITE_PERM
            case .abort     : SQLITE_ABORT
            case .busy      : SQLITE_BUSY
            case .locked    : SQLITE_LOCKED
            case .nomem     : SQLITE_NOMEM
            case .readonly  : SQLITE_READONLY
            case .interrupt : SQLITE_INTERRUPT
            case .ioerr     : SQLITE_IOERR
            case .corrupt   : SQLITE_CORRUPT
            case .notfound  : SQLITE_NOTFOUND
            case .full      : SQLITE_FULL
            case .cantopen  : SQLITE_CANTOPEN
            case .protocol_ : SQLITE_PROTOCOL
            case .empty     : SQLITE_EMPTY
            case .schema    : SQLITE_SCHEMA
            case .toobig    : SQLITE_TOOBIG
            case .constraint: SQLITE_CONSTRAINT
            case .mismatch  : SQLITE_MISMATCH
            case .misuse    : SQLITE_MISUSE
            case .nolfs     : SQLITE_NOLFS
            case .auth      : SQLITE_AUTH
            case .format    : SQLITE_FORMAT
            case .range     : SQLITE_RANGE
            case .notadb    : SQLITE_NOTADB
            case .notice    : SQLITE_NOTICE
            case .warning   : SQLITE_WARNING
            case .row       : SQLITE_ROW
            case .done      : SQLITE_DONE
            case .other(let code): code
            }
        }

        public static func fromSQLite(code: Int32) -> ResultCode {
            switch code {
            case SQLITE_OK:         .ok
            case SQLITE_ERROR:      .error
            case SQLITE_INTERNAL:   .internal_
            case SQLITE_PERM:       .perm
            case SQLITE_ABORT:      .abort
            case SQLITE_BUSY:       .busy
            case SQLITE_LOCKED:     .locked
            case SQLITE_NOMEM:      .nomem
            case SQLITE_READONLY:   .readonly
            case SQLITE_INTERRUPT:  .interrupt
            case SQLITE_IOERR:      .ioerr
            case SQLITE_CORRUPT:    .corrupt
            case SQLITE_NOTFOUND:   .notfound
            case SQLITE_FULL:       .full
            case SQLITE_CANTOPEN:   .cantopen
            case SQLITE_PROTOCOL:   .protocol_
            case SQLITE_EMPTY:      .empty
            case SQLITE_SCHEMA:     .schema
            case SQLITE_TOOBIG:     .toobig
            case SQLITE_CONSTRAINT: .constraint
            case SQLITE_MISMATCH:   .mismatch
            case SQLITE_MISUSE:     .misuse
            case SQLITE_NOLFS:      .nolfs
            case SQLITE_AUTH:       .auth
            case SQLITE_FORMAT:     .format
            case SQLITE_RANGE:      .range
            case SQLITE_NOTADB:     .notadb
            case SQLITE_NOTICE:     .notice
            case SQLITE_WARNING:    .warning
            case SQLITE_ROW:        .row
            case SQLITE_DONE:       .done
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
