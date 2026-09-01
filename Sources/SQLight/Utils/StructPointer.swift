//
//  StructPointer.swift
//  SQLight
//
//  Created by Nick Main on 2026-09-01.
//

class StructPointer<T> {
    let pointer: UnsafeMutablePointer<T>

    init() {
        pointer = UnsafeMutablePointer<T>.allocate(capacity: 1)
    }

    deinit {
        pointer.deallocate()
    }
}
