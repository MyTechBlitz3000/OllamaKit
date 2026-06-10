//
//  OKHTTPClient.swift
//

import Foundation
import Combine

// MARK: - Buffer Actor (fully owns mutation)
actor BufferActor {
    private var buffer = Data()

    func append(_ byte: UInt8) {
        buffer.append(byte)
    }

    func extractNextJSON(_ extractor: (inout Data) -> Data?) -> Data? {
        extractor(&buffer)
    }
}

// MARK: - Client
internal struct OKHTTPClient: Sendable {
    private let decoder = JSONDecoder()
    static let shared = OKHTTPClient()
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
            Task {
                do {
                    let (bytes, response) = try await URLSession.shared.bytes(for: request)
                    try validate(response: response)

                    continuation.onTermination = { termination in
                        if case .cancelled = termination {
                            bytes.task.cancel()
                        }
                    }

                    for try await byte in bytes {
                        await bufferActor.append(byte)

                        // IMPORTANT: keep extracting until no progress
                        while true {
                            guard let chunk = await bufferActor.extractNextJSON(self.extractNextJSON) else {
                                break
                            }

                            do {
                                let decoded = try decoder.decode(T.self, from: chunk)
                                continuation.yield(decoded)
                            } catch {
                                // skip bad chunk but DO NOT kill stream
                                print("Decoding error:", error)
                            }
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }
}

// MARK: - Combine API
func stream<T: Decodable>(
    request: URLRequest,
    with responseType: T.Type
) -> AnyPublisher<T, Error> {

    let delegate = StreamingDelegate()
    let session = URLSession(configuration: .default,
                             delegate: delegate,
                             delegateQueue: .main)

    session.dataTask(with: request).resume()

    let bufferActor = BufferActor()

    return delegate.publisher()
        .flatMap { newData -> AnyPublisher<T, Error> in
            Future { promise in
                Task {
                    for byte in newData {
                        await bufferActor.append(byte)
                    }

                    if let chunk = await bufferActor.extractNextJSON(self.extractNextJSON) {
                        do {
                            let decoded = try self.decoder.decode(T.self, from: chunk)
                            promise(.success(decoded))
                        } catch {
                            promise(.failure(error))
                        }
                    }
                }
            }
            .eraseToAnyPublisher()
        }
        .eraseToAnyPublisher()
}

// MARK: - Core helpers
private extension OKHTTPClient {

    func validate(response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse,
              (200...299).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }

    func extractNextJSON(from buffer: inout Data) -> Data? {
        var escaped = false
        var inString = false
        var depth = 0
        var start: Data.Index?

        for idx in buffer.indices {
            let byte = buffer[idx]
            let char = Character(UnicodeScalar(byte))

            if escaped {
                escaped = false
                continue
            }

            if char == "\\" {
                escaped = true
                continue
            }

            if char == "\"" {
                inString.toggle()
                continue
            }

            guard !inString else { continue }

            if char == "{" {
                depth += 1
                if depth == 1 {
                    start = idx
                }
            }

            if char == "}" {
                depth -= 1

                if depth == 0, let startIndex = start {
                    let range = startIndex...idx
                    let chunk = buffer.subdata(in: range)
                    buffer.removeSubrange(range)
                    return chunk
                }
            }
        }

        return nil
    }
}
