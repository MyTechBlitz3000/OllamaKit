import Foundation
import Combine

// MARK: - Buffer Actor (fully owns and isolates mutation)
actor BufferActor {
    private var buffer = Data()

    func append(_ byte: UInt8) {
        buffer.append(byte)
    }

    /// Extracted into the actor to avoid crossing isolation boundaries with mutable state
    func extractNextJSON() -> Data? {
        var escaped = false
        var inString = false
        var depth = 0
        var start: Data.Index?

        // Using byte literals avoids high-overhead Character conversions
        let backslash: UInt8 = 0x5C // "\\"
        let quote: UInt8 = 0x22 // "\""
        let openBrace: UInt8 = 0x7B // "{"
        let closeBrace: UInt8 = 0x7D // "}"

        for idx in buffer.indices {
            let byte = buffer[idx]

            if escaped {
                escaped = false
                continue
            }

            if byte == backslash {
                escaped = true
                continue
            }

            if byte == quote {
                inString.toggle()
                continue
            }

            guard !inString else { continue }

            if byte == openBrace {
                depth += 1
                if depth == 1 {
                    start = idx
                }
            }

            if byte == closeBrace {
                depth -= 1

                if depth == 0, let startIndex = start {
                    let nextIndex = buffer.index(after: idx)
                    let chunk = buffer.subdata(in: startIndex..<nextIndex)
                    buffer.removeSubrange(startIndex..<nextIndex)
                    return chunk
                }
            }
        }

        return nil
    }
}

// MARK: - Client
internal struct OKHTTPClient: Sendable {
    private let decoder = JSONDecoder()
    static let shared = OKHTTPClient()
    
    private init() {} // Prevent arbitrary instances
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

                        // Keep extracting until no more valid objects can be constructed
                        while let chunk = await bufferActor.extractNextJSON() {
                            do {
                                let decoded = try decoder.decode(T.self, from: chunk)
                                continuation.yield(decoded)
                            } catch {
                                // Skip bad chunks but do not crash/kill the stream
                                print("Decoding error:", error)
                            }
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            // Clean handling of stream cancellation
            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }
}

// MARK: - Combine API
internal extension OKHTTPClient {
    
    /// Bridges the AsyncThrowingStream elegantly into Combine without manual delegate hell
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
