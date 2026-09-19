# M4 chronological development observations

These are development observations, not final M4 acceptance. The current baseline is recorded in the milestone scope and ignored artifacts/m4/baseline-47e1492.

## Selected-version protocol probe — 2026-09-19

RabbitMQ 4.3.6-management was actually pulled at sha256:de62d9901fb73aaa8767b9c1e9ea32f2938f69d1b83e430f7c5ca854081d2128. amqplib 2.0.1 was installed exactly; its bundled types avoid another dependency. The root lockfile adds only that package and all preceding pins remain unchanged.

The first isolated startup failed because the attempted management.tcp.port=none value is not an integer. The real server emitted its configuration-transform failure before boot. artifacts/m4/protocol-start.log retains the actual diagnostic. The correction uses only the configured HTTPS listener; the subsequent real probe verifies HTTPS and AMQP/TLS, restricts TLS to 1.2/1.3 and disables the image's unnecessary Prometheus plugin. Erlang distribution remains on the private container network, with no published clustering port. The application endpoints use password authentication over CA-verified TLS, not mutual TLS; verify_none on the server concerns optional client certificates, not client-side server verification.

Run m4-protocol-1789826781732 exited 0 and removed its owned container/volume. Its result.json records RabbitMQ 4.3.6, actual queue arguments, application registration identity, byte-exact high-number payload delivery, positive confirm, mandatory unroutable return, denied publisher queue creation, passive declaration with write/read-only permissions, and metadata inspection using a management user with no AMQP configure/write/read grants. The real highWaterMark=1 write returned false and still received a successful broker confirm. This is a selected-version probe, not a substitute for MQ01–MQ16.

The client forwards documented Node socket options, but its bundled SocketOptions type omits AbortSignal. A narrow typed intersection supplies the public Node signal option without a cast or undocumented driver access. Retirement behavior is still required by the actual fault tests. Automatic amqplib recovery is explicitly false. The source envelope and its arbitrary-precision payload text are untouched.
