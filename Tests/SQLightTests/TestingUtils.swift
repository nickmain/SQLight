// Copyright (c) 2024 David N Main

import Foundation

struct CouldNotFindResource: LocalizedError {
    let filename: String
    public var errorDescription: String? { "Could not find resource: \(filename)" }
}

func resourceFolder() throws -> String {
    guard let resourceFolder = Bundle.module.resourceURL?.path(percentEncoded: false)
    else {
        throw CouldNotFindResource(filename: "<RESOURCE FOLDER>")
    }
    return resourceFolder
}

// Get the path to a bundled sample database
func pathFor(sample filename: String, ext: String = "sqlite3") throws -> String {
    guard let doc = Bundle.module.url(forResource: filename,
                                      withExtension: ext,
                                      subdirectory: "databases")
    else {
        throw CouldNotFindResource(filename: filename)
    }

    return doc.path(percentEncoded: false)
}
