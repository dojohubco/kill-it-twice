# M3 early protocol experiment — 2026-09-17

This is developmental protocol evidence, not M3 acceptance. Design commit `6826423` preceded production changes. The original clean baseline gate passed at `c05f3dc`; later protocol runs used an explicitly dirty development tree.

The real Elasticsearch 9.5.4 server accepted strict external versions **9007199254740993** and **9223372036854775807**. Repeating each version and sending its predecessor produced actual 409 conflicts; realtime GET returned the exact original version token. An integer mapping with coerce=false/ignore_malformed=false rejected `"not-a-number"` with actual HTTP-item 400/document_parsing_exception. These are isolated synthetic adapter revisions, not claimed source update counts.

Selected production boundary: official client **9.5.1**, lossless-json **4.3.1**, documented Serializer hook returning unknown for explicit narrowing, pre-serialized NDJSON with raw decimal version tokens and final newline, verified TLS. No external_gte in delivery. Image: `docker.elastic.co/elasticsearch/elasticsearch:9.5.4@sha256:82ac14f43fe701992e601f4cc81e1c0d7dbc5a2576d8cd736006452925df4026`.

Actual development results:

- First run failed HTTP 406: request content-type had been placed on the transport parameter instead of the documented request options. Corrected that call site; no protocol guarantee was relaxed. Log: `artifacts/m3/protocol-development.log`.
- A trial client 9.3.4 passed the runtime experiment (`artifacts/m3/m3-protocol-1789602272529/protocol.json`) but did not avoid the optional Arrow declaration problem; it was not retained.
- Client 9.5.1 passed the corrected real experiment, exit 0, at `artifacts/m3/m3-protocol-1789602809176/protocol.json`; log `artifacts/m3/protocol-development-3.log`. Owned service/volume cleanup completed.
- Strict TypeScript checking exposed the official client's public helper declaration importing the optional `apache-arrow/Arrow.node.js` path. Installing its declared Arrow peer alone did not fix ESM export resolution. Exact **apache-arrow 21.1.0** is a development-only compatibility dependency, with one TypeScript path mapping to its real checked declaration file. There is no Arrow application import, new query format, fabricated type stub or blanket skipLibCheck. The compiler continues to check dependencies. This is a concrete client declaration compatibility accommodation; the existing compiler/tool versions are unchanged.

The full M3 profiles independently repeat version checks and real projection/mapping tests. Their final committed-code identity and input hashes will supersede these developmental results for acceptance. These local artifact paths are not published URLs.
