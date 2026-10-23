import { Crypto, Effect, Layer } from "effect";

/**
 * Default `Crypto` service backed by the Web Crypto API (available in Bun, Node
 * and browsers). Hosts that need a different source of randomness or digests
 * can provide their own `Crypto` layer instead.
 */
export const layerWebCrypto: Layer.Layer<Crypto.Crypto> = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, data) =>
      Effect.promise(
        async () => new Uint8Array(await crypto.subtle.digest(algorithm, new Uint8Array(data))),
      ),
  }),
);
