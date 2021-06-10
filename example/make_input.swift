#!/usr/bin/swift
import Foundation

let program_id = Int(CommandLine.arguments[1])!
let N = Int(CommandLine.arguments[2])!

struct JobInput: Codable {
    var arguments: [String]
}

struct Request: Codable {
    var program_id: Int
    var inputs: [JobInput]
}

let inputs = (0..<N).map { _ in
    [14, 13, 0]
}
.map {
    JobInput(arguments: ["main.wasm"] + $0.map(\.description))
}


let encoder = JSONEncoder()
let request = Request(program_id: program_id, inputs: inputs)

let bytes = try encoder.encode(request)
print(String(data: bytes, encoding: .utf8)!)
