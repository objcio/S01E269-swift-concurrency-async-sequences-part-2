//
//  Async.swift
//  Async
//
//  Created by Chris Eidhof on 23.08.21.
//

import Foundation

func sample() async throws {
    let start = Date.now
    let url = Bundle.main.url(forResource: "enwik8", withExtension: "zlib")!
    var counter = 0
    let fileHandle = try FileHandle(forReadingFrom: url)
    for try await event in fileHandle.bytes.chunked.decompressed.xmlEvents {
        guard case let .didStart(name) = event else { continue }
        print(name)
        counter += 1
    }
    print(counter)
    print("Duration: \(Date.now.timeIntervalSince(start))")
}

extension AsyncSequence where Element == UInt8 {
    var chunked: Chunked<Self> {
        Chunked(base: self)
    }
}

struct Chunked<Base: AsyncSequence>: AsyncSequence where Base.Element == UInt8 {
    var base: Base
    var chunkSize: Int = Compressor.bufferSize // todo
    typealias Element = Data
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        var chunkSize: Int
        
        mutating func next() async throws -> Data? {
            var result = Data()
            while let element = try await base.next() {
                result.append(element)
                if result.count == chunkSize { return result }
            }
            return result.isEmpty ? nil : result
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: base.makeAsyncIterator(), chunkSize: chunkSize)
    }
}

extension AsyncSequence where Element == Data {
    var decompressed: Compressed<Self> {
        Compressed(base: self, method: .decompress)
    }
}

struct Compressed<Base: AsyncSequence>: AsyncSequence where Base.Element == Data {
    var base: Base
    var method: Compressor.Method
    typealias Element = Data
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        var compressor: Compressor
        
        mutating func next() async throws -> Data? {
            if let chunk = try await base.next() {
                return try compressor.compress(chunk)
            } else {
                let result = try compressor.eof()
                return result.isEmpty ? nil : result
            }
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        let c = Compressor(method: method)
        return AsyncIterator(base: base.makeAsyncIterator(), compressor: c)
    }
}

extension AsyncSequence where Element: Sequence {
    var flattened: Flattened<Self> {
        Flattened(base: self)
    }
}

struct Flattened<Base: AsyncSequence>: AsyncSequence where Base.Element: Sequence {
    var base: Base
    typealias Element = Base.Element.Element
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        var buffer: Base.Element.Iterator?
        
        mutating func next() async throws -> Element? {
            if let el = buffer?.next() {
                return el
            }
            buffer = try await base.next()?.makeIterator()
            guard buffer != nil else { return nil }
            return try await next()
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: base.makeAsyncIterator())
    }
}

extension AsyncSequence where Element == Data {
    var xmlEvents: XMLEvents<Self> {
        XMLEvents(base: self)
    }
}

struct XMLEvents<Base: AsyncSequence>: AsyncSequence where Base.Element == Data {
    var base: Base
    typealias Element = XMLEvent
    
    struct AsyncIterator: AsyncIteratorProtocol {
        var base: Base.AsyncIterator
        let parser = PushParser()
        var buffer: [XMLEvent] = []
        
        mutating func next() async throws -> Element? {
            if !buffer.isEmpty {
                return buffer.removeFirst()
            }
            if let data = try await base.next() {
                var newEvents: [XMLEvent] = []
                parser.onEvent = { event in
                    newEvents.append(event)
                }
                parser.process(data)
                buffer = newEvents
                return try await next()
            }
            parser.finish()
            return nil
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: base.makeAsyncIterator())
    }
}
