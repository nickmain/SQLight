import XCTest
@testable import SQLight

final class SQLightTests: XCTestCase {

    func testInfo() {
        print("Version string: \(SQLight.versionString)")
        print("Version number: \(SQLight.versionNumber)")
        print("Source id     : \(SQLight.sourceId)")
        print("Thread safety : \(SQLight.isThreadsafe)")
    }

    func testAggregateFunction() async throws {
        class FooFunction: SQLight.AggregateFunction {
            static let argErrorMessage = "unexpected args" // used to test error throwing

            override init() {
                print("🟠 FooFunction factory")
            }

            var gatheredArgs = [String]()

            override func stepCall(args: [SQLight.Value]) throws {
                print("🟠 FooFunction stepCall: \(args)")
                guard case let .string(firstName) = args[0],
                      case let .int(actorId) = args[1]
                else {
                    throw SQLight.Error.message(Self.argErrorMessage)
                }
                gatheredArgs.append("\(firstName)(\(actorId))")
            }

            override func finalCall() -> SQLight.Value {
                print("🟠 FooFunction finalCall")
                return .string(gatheredArgs.joined(separator: ", "))
            }

            deinit { print("🟠 FooFunction deinit") }
        }

        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.createFunction(named: "foo", factory: FooFunction.init)

        try db.execute(sql: """
            CREATE TABLE IF NOT EXISTS "test_table" (
              "actor_id" numeric PRIMARY KEY NOT NULL,
              "first_name" varchar(45) NOT NULL,
              "last_name" varchar(45) NOT NULL
            );
            INSERT INTO "test_table" VALUES
                (1, "Foo", "Bar"),
                (2, "B", "dslkjfsd"),
                (3, "C", "Bar"),
                (4, "D", "Bar"),
                (5, "E", "")
        """)

        var expectedResult = ""
        try db.execute(sql: "SELECT foo(first_name, actor_id) as test_result FROM test_table") { _, cols in
            let value = cols["test_result"] ?? nil
            expectedResult = value ?? "MISSING"
            print("Expected result = \"\(expectedResult)\"")
            return true
        }

        XCTAssertEqual(expectedResult, "Foo(1), B(2), C(3), D(4), E(5)")

        // test that step function can return an error message that gets thrown as an error
        do {
            try db.execute(sql: "SELECT foo(24, actor_id) as test_result FROM test_table")
        } catch SQLight.Error.resultMessage(.error, let message) {
            // error is expected
            XCTAssertTrue(message.hasSuffix(FooFunction.argErrorMessage))
            return
        }

        XCTFail("expected error to be thrown")
    }

    func testScalarFunction() async throws {
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.createFunction(named: "foo") { args in
            XCTAssertEqual(args, [.int(34), .string("Hello"), .null, .double(3.4)])
            return .string("Hello World")
        }

        var expectedResult = ""
        try db.execute(sql: "SELECT foo(34,'Hello',null,3.4) as test_result") { _, cols in
            let value = cols["test_result"] ?? nil
            expectedResult = value ?? "MISSING"
            return true
        }

        XCTAssertEqual(expectedResult, "Hello World")
    }

    func testBindings() async throws {
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.execute(sql: """
            CREATE TABLE IF NOT EXISTS "test_table" (
              "actor_id" numeric PRIMARY KEY NOT NULL,
              "first_name" varchar(45) NOT NULL,
              "last_name" varchar(45) NOT NULL,
              "last_update" timestamp
            );
            INSERT INTO "test_table" VALUES
                (1, "Foo", "Bar", CURRENT_TIMESTAMP),
                (2, "B", "dslkjfsd", CURRENT_TIMESTAMP),
                (3, "C", "Bar", CURRENT_TIMESTAMP),
                (4, "D", "Bar", CURRENT_TIMESTAMP),
                (5, "E", "", CURRENT_TIMESTAMP)
        """)

        let query = try db.prepare(statement: """
            SELECT first_name FROM \"test_table\"
                where last_name = $lastName
                  and actor_id < $maxId
        """)

        XCTAssertEqual(query.parameterCount, 2)
        XCTAssertEqual(query.indexOfParameter(named: "$lastName"), 1)
        XCTAssertEqual(query.indexOfParameter(named: "$foo"), nil)
        XCTAssertEqual(query.nameOfParameter(at: 1), "$lastName")

        try query.bindParameter(at: 1, to: "Bar")
        try query.bindParameter(at: 2, to: 4)

        var rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.stringValue(at: 0), "Foo")
        rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.stringValue(at: 0), "C")
        rc = try query.step()
        XCTAssertEqual(rc, .done)

        // try again with larger max actor_id value
        try query.reset()
        try query.bindParameter(at: 2, to: 5)

        rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.stringValue(at: 0), "Foo")
        rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.stringValue(at: 0), "C")
        rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.stringValue(at: 0), "D")
        rc = try query.step()
        XCTAssertEqual(rc, .done)
    }

    func testStepResults() async throws {
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.execute(sql: """
            CREATE TABLE IF NOT EXISTS "test_table" (
              "actor_id" numeric PRIMARY KEY NOT NULL,
              "first_name" varchar(45) NOT NULL,
              "last_name" varchar(45) NOT NULL,
              "last_update" timestamp
            );
            INSERT INTO "test_table" VALUES
                (1, "Foo", "Bar", CURRENT_TIMESTAMP),
                (2, "B", "dslkjfsd", CURRENT_TIMESTAMP),
                (3, "C", "askdhashd", CURRENT_TIMESTAMP),
                (4, "D", "akjsdhkajsh", CURRENT_TIMESTAMP),
                (5, "E", "", CURRENT_TIMESTAMP)
        """)

        let query = try db.prepare(statement: "SELECT * FROM \"test_table\"")
        let columnCount = query.columnCount
        XCTAssertEqual(columnCount, 4)

        try query.step()
        XCTAssertEqual(query.columnCount, 4)
        XCTAssertEqual(query.columnType(at: 0), .int)
        XCTAssertEqual(query.columnType(at: 1), .string)
        XCTAssertEqual(query.columnType(at: 2), .string)
        XCTAssertEqual(query.columnType(at: 3), .string)
        XCTAssertEqual(query.integerValue(at: 0), 1)
        XCTAssertEqual(query.stringValue(at: 1), "Foo")
        XCTAssertEqual(query.stringValue(at: 2), "Bar")

        var rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.integerValue(at: 0), 2)
        rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.integerValue(at: 0), 3)
        rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.integerValue(at: 0), 4)
        rc = try query.step()
        XCTAssertEqual(rc, .row)
        XCTAssertEqual(query.integerValue(at: 0), 5)
        rc = try query.step()
        XCTAssertEqual(rc, .done)
    }

    func testOpenFile() async throws {
        let dbPath = try pathFor(sample: "Sakila")
        print("dbPath = \(dbPath)")
        let db = try SQLight.Connection.open(file: dbPath)
        try db.execute(sql: "select * from sqlite_master") { rowNumber, row in
            print("[\(rowNumber)]: \(row)")
            return true
        }
    }

    func testExec() async throws {
        var rowCount = 0
        let db = try SQLight.Connection.createInMemoryDatabase()
        try db.execute(sql: """
            CREATE TABLE IF NOT EXISTS "actor" (
              "actor_id" numeric PRIMARY KEY NOT NULL,
              "first_name" varchar(45) NOT NULL,
              "last_name" varchar(45) NOT NULL,
              "last_update" timestamp
            )
        """)

        try db.execute(sql: "select * from sqlite_master") { rowNumber, row in
            rowCount = rowNumber
            return true
        }

        XCTAssertEqual(rowCount, 2)
    }

    func testExecThrows() async throws {
        let db = try SQLight.Connection.createInMemoryDatabase()
        do {
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS "actor" (
                  "actor_id" numeric PRIMARY KEY NOT NULL,
                  "first_name" varchar(45) NOT NULL,
                  "last_name" varchar(45) NOT NULL,
                  "last_update" timestamp
                );
                select * from sqlite_master
            """) { rowNumber, row in
                return false // abort
            }
        } catch SQLight.Error.resultMessage(.abort, _) {
            // success
            return
        } catch {
            XCTFail("Unexpected error \(error)")
            return
        }

        XCTFail("Expected error was not thrown")
    }
}
