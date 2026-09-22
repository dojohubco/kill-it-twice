"""Explicit test-project replica bounds; production service defaults are unchanged."""
SINK_ROLES = ('es-worker', 'publisher', 'consumer', 'observer')

def replica_counts(scanners, sinks):
    if type(scanners) is not int or not 1 <= scanners <= 4:
        raise ValueError('Capacity scanners must be an integer from one to four')
    if type(sinks) is not int or not 1 <= sinks <= 2:
        raise ValueError('Capacity sink workers must be one or two per role')
    return {'backfill': scanners, **{role: sinks for role in SINK_ROLES}}

def compose_scales(scanners, sinks):
    return [argument for role, count in replica_counts(scanners, sinks).items()
            for argument in ('--scale', role + '=' + str(count))]
