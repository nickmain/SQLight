// Copyright (c) 2024 David N Main

import Foundation

/// A SQL token.
struct Token {

    /// The 1-based line number
    let line: Int

    /// The 1-based column number
    let column: Int

    /// The token type and content
    let content: Content

    internal init(line: Int, column: Int, content: Content) {
        self.line = line
        self.column = column
        self.content = content
    }

    /// Token type and content
    enum Content: Equatable {
        case openParen, closeParen
        case parameterName(String)
        case parameterNumber(Int)
        case word(String)
        case string(String)
        case numeric(String)
        case semicolon
        case comma
        case period
        case operator_(String)
        case badToken
        case end  // end of input
    }
}

class Tokenizer {

    let lines: [String.SubSequence]
    var currentLine: [Character]
    var line = 0     // curent read position
    var column = 0   // curent read position
    var startLine = 0 // start position of token
    var startCol = 0  // start position of token
    var buffer = ""

    init(text: String) {
        lines = text.split(separator: "\n")
        currentLine = Array(lines.first ?? "")
    }

    // read the next token
    func readToken() -> Token {
        skipWhitespace()  // TODO: skip comments
        if isAtEnd {
            startCol = 0
            return makeToken(.end)
        }

        // make a token and clear the buffer
        // assume that current char is last and advance to next
        func makeToken(_ content: Token.Content) -> Token {
            let token = Token(line: startLine + 1, column: startCol + 1, content: content)
            advance()
            startCol = column
            startLine = line
            buffer = ""
            return token
        }

        let c = currentChar

        if c == "(" { return makeToken(.openParen) }
        if c == ")" { return makeToken(.closeParen) }
        if c == "," { return makeToken(.comma) }
        if c == ";" { return makeToken(.semicolon) }
        if c == "." && !nextChar.isNumber { return makeToken(.period) }

        if c.isLetter {
            gatherWord()
            return makeToken(.word(buffer))
        }

        if c == "@" || c == ":" || c == "$" {
            gatherWord()
            return makeToken(.parameterName(buffer))
        }

        if c == "?" && nextChar.isNumber {
            advance()
            gatherDigits()
            return makeToken(.parameterNumber(Int(buffer) ?? 0))
        }

        if c == "\"" || c == "'" {
            gatherString()
            return makeToken(.string(buffer))
        }

        if c.isNumber || (c == "." && nextChar.isNumber) {
            gatherNumber()
            return makeToken(.numeric(buffer))
        }

        if gatherChars("->>") { return makeToken(.operator_("->>")) }
        if gatherChars("->") { return makeToken(.operator_("->")) }
        if gatherChars("||") { return makeToken(.operator_("||")) }
        if gatherChars(">>") { return makeToken(.operator_(">>")) }
        if gatherChars("<<") { return makeToken(.operator_("<<")) }
        if gatherChars("<=") { return makeToken(.operator_("<=")) }
        if gatherChars(">=") { return makeToken(.operator_(">=")) }
        if gatherChars("==") { return makeToken(.operator_("==")) }
        if gatherChars("<>") { return makeToken(.operator_("<>")) }
        if gatherChars("!=") { return makeToken(.operator_("!=")) }
        if gatherChars("~") { return makeToken(.operator_("~")) }
        if gatherChars("+") { return makeToken(.operator_("+")) }
        if gatherChars("-") { return makeToken(.operator_("-")) }
        if gatherChars("%") { return makeToken(.operator_("%")) }
        if gatherChars("*") { return makeToken(.operator_("*")) }
        if gatherChars("/") { return makeToken(.operator_("/")) }
        if gatherChars("&") { return makeToken(.operator_("&")) }
        if gatherChars("|") { return makeToken(.operator_("|")) }
        if gatherChars("<") { return makeToken(.operator_("<")) }
        if gatherChars(">") { return makeToken(.operator_(">")) }
        if gatherChars("=") { return makeToken(.operator_("=")) }

        return makeToken(.badToken)
    }

    // gather the given chars and return true if successful
    func gatherChars(_ chars: String) -> Bool {
        for (index, char) in chars.enumerated() {
            if charAt(offset: index) != char { return false }
        }

        advance(by: chars.count - 1)
        return true
    }

    func gatherNumber() {
        if currentChar == "." {
            buffer.append(".")
            advance()
            gatherDigits()
        } else if currentChar == "0" && (nextChar == "x" || nextChar == "X") {
            buffer.append("0x")
            advance()
            advance()
            gatherHexDigits()
            return // skip exponent

        } else if currentChar.isNumber {
            gatherDigits()
            if nextChar == "." {
                buffer.append(".")
                advance()
                advance()
                gatherDigits()
            }
        }

        // exponent part
        if nextChar == "E" || nextChar == "e" {
            buffer.append("e")
            advance()
            advance()
            gatherDigits()
        }
    }

    func skipWhitespace() {
        while !isAtEnd && currentChar.isWhitespace {
            advance()
        }

        startCol = column
        startLine = line
    }

    // gather content of a string without the delimiters
    func gatherString() {
        let delimiter = currentChar
        advance()
        while true {
            if currentChar == delimiter && nextChar == delimiter {
                buffer.append(delimiter) // delimiter escape
            } else if currentChar == delimiter || currentChar.isNewline {
                return // end of string
            }

            buffer.append(currentChar)
            advance()
        }
    }

    // gather digits and leave curr position at last char
    func gatherDigits() {
        while true {
            buffer.append(currentChar)
            if nextChar.isNumber {
                advance()
            } else {
                return
            }
        }
    }

    // gather hex digits and leave curr position at last char
    func gatherHexDigits() {
        while true {
            buffer.append(currentChar)
            if nextChar.isHexDigit {
                advance()
            } else {
                return
            }
        }
    }

    // gather alphanumerics and leave curr position at last char
    func gatherWord() {
        while true {
            buffer.append(currentChar)
            let c = nextChar
            if c.isLetter || c.isNumber || c == "_" {
                advance()
            } else {
                return
            }
        }
    }

    var currentChar: Character { currentLine[column] }

    var nextChar: Character { charAt(offset: 1) }

    // whether at end of the input
    var isAtEnd: Bool { line >= lines.count }

    // get the char at offset from current or newline if past EOL
    func charAt(offset: Int) -> Character {
        if column + offset < currentLine.count {
            currentLine[column + offset]
        } else {
            "\n"
        }
    }

    // Advance by the given offset, skip to next line if past EOL
    func advance(by offset: Int = 1) {
        column += offset
        if column >= currentLine.count {
            line += 1
            if line < lines.count {
                currentLine = Array(lines[line])
                column = 0
            }
        }
    }
}
