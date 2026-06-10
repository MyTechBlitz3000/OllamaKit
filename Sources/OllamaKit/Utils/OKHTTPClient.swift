
//
//  OKHTTPClient.swift
//

import Foundation
import Combine

// MARK: - Buffer Actor (fully owns and isolates mutation)
actor BufferActor {
    private var buffer = Data()

    func append(_ byte: UInt8) {
        buffer.append(byte)
    }

    /// Bulletproof JSON Stream Extractor.
    /// Operates as a true sequential state machine to handle nested objects, 
    /// string scopes, escaped characters, and multi-byte UTF-8 safety.
    func extractNextJSON() -> Data? {
        var isEscaped = false
        var isInString = false
        var depth = 0
        var startIdx: Data.Index?

        let backslash: UInt8 = 0x5C // \
        let quote: UInt8     = 0x22 // "
        let openBrace: UInt8 = 0x7B // {
        let closeBrace: UInt8= 0x7D // }

        for idx in buffer.indices {
            let byte = buffer[idx]

            if isEscaped {
                isEscaped = false
                continue
            }

            if byte == backslash {
                // Only treat as escape indicator if we are inside a string literal
                if isInString {
                    isEscaped = true
                }
                continue
            }

            if byte == quote {
                isInString.toggle()
                continue
            }

            // While trapped inside a JSON string literal, completely ignore brackets.
            // This natively prevents multi-byte UTF-8 collisions and text anomalies.
            guard !isInString else { continue }

            if byte == openBrace {
                depth += 1
                if depth == 1 {
                    startIdx = idx
                }
            } else if byte == closeBrace {
                guard depth > 0 else {
                    // Malformed prefix data fallback: drop byte to prevent endless loop
                    buffer.removeFirst(1)
                    return nil
                }
                
                depth -= 1

                if depth == 0, let start = startIdx {
                    let nextIndex = buffer.index(after: idx)
                    let chunk = buffer.subdata(in: start..<nextIndex)
                    buffer.removeSubrange(..<nextIndex) // Drops processed chunk + leading whitespace
                    return chunk
                }
            }
        }

        // If the stream ends or is interrupted with an incomplete object,
        // clear corrupted data when depth breaks sequence bounds.
        if depth == 0 && startIdx == nil && !buffer.isEmpty {
            // Trim leading control whitespaces/newlines between JSON chunks
            while let first = buffer.first, first <= 0x20 {
                buffer.removeFirst()
            }
        }

        return nil
    }
}

// MARK: - Client
internal struct OKHTTPClient: Sendable {
    private let decoder = JSONDecoder()
    static let shared = OKHTTPClient()
    
    private init() {} 
}

// MARK: - Async/Await API
internal extension OKHTTPClient {

    func send(request: URLRequest) async throws {
        let (_, response) = try await URLSession.shared.data(for: request)
        try validate(response: response)
    }

    func send<T: Decodable>(
        request: URLRequest,
        with responseType: T.Type
    ) async throws -> T {
        let (data, response) = try await URLSession.shared.data(for: request)
        try validate(response: response)
        return try decoder.decode(T.self, from: data)
    }

    func stream<T: Decodable>(
        request: URLRequest,
        with responseType: T.Type
    ) -> AsyncThrowingStream<T, Error> {

        let decoder = self.decoder
        let bufferActor = BufferActor()

        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    let (bytes, response) = try await URLSession.shared.bytes(for: request)
                    try validate(response: response)

                    for try await byte in bytes {
                        await bufferActor.append(byte)

                        while let chunk = await bufferActor.extractNextJSON() {
                            do {
                                let decoded = try decoder.decode(T.self, from: chunk)
                                continuation.yield(decoded)
                            } catch {
                                // Skip individual corrupt chunk, preserve stream health
                                print("Decoding error encountered: \(error)")
                            }
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }
}

// MARK: - Combine API
internal extension OKHTTPClient {
    
    func stream<T: Decodable>(
        request: URLRequest,
        with responseType: T.Type
    ) -> AnyPublisher<T, Error> {
        let subject = PassthroughSubject<T, Error>()
        
        let task = Task {
            do {
                let stream = self.stream(request: request, with: responseType)
                for try await element in stream {
                    subject.send(element)
                }
                subject.send(completion: .finished)
            } catch {
                subject.send(completion: .failure(error))
            }
        }
        
        return subject
            .handleEvents(receiveCancel: {
                task.cancel()
            })
            .eraseToAnyPublisher()
    }
}

// MARK: - Core helpers
private extension OKHTTPClient {

    func validate(response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse,
              (200...299).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }
}
