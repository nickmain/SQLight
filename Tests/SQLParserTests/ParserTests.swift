// Copyright (c) 2024 David N Main

import XCTest
@testable import SQLParser

final class ParserTests: XCTestCase {

    func testSanity() {
        let sql = """
            INSERT INTO foobar VALUES
                (1, 1.1, "Bar"),
                (2, 2.2, "dslkjfsd"),
                (3, 3.3, "Bar"),
                (4, ?34, "Bar"),
                (5, @param3, null)
        """

        let tokenizer = Tokenizer(text: sql)
        loop: while true {
            let token = tokenizer.readToken()
            print(token)

            switch token.content {
            case .end: break loop
            case .badToken: break loop
            default: break
            }
        }
    }

}
