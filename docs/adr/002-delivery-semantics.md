# ADR 002: At-least-once transport and defined database effects

Reading note: the adoption-era status and future-tense verification text below are historical. Later implementation evidence is recorded in the [current acceptance matrix](../acceptance-matrix.md); the original decision and chronology are preserved.

Status: accepted design; implementation verification pending.
Origin: pre-implementation architectural review.

## Context

A remote operation can succeed before its local acknowledgement commits. Source, pipeline, Elasticsearch, RabbitMQ, and consumer state do not share one atomic transaction.

## Decision

Retry unresolved transport work under stable event identity. Claim at-least-once transport, monotonic projections, and effectively-once defined consumer database effects only when tests support them. Keep separate sink obligations. Commit consumer inbox deduplication, one audit effect/aggregate unit per unique mutation, and projection changes before ACK. Baseline observations are not new business actions.

## Alternatives

At-most-once would permit loss after ambiguity. End-to-end exactly-once and atomic cross-sink visibility are not claimed. Distributed transactions and arbitrary external-side-effect guarantees are outside scope.

## Consequences and validation

Physical duplicate publication/delivery is expected. Test real remote-success/local-acknowledgement gaps and consumer COMMIT-before-ACK. Verify effects, not only entity counts. Document assumptions and permanent failures. No complete transport or sink implementation exists at adoption.

## Reference basis

https://www.rabbitmq.com/docs/confirms
