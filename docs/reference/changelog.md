# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-02-14

### Added
- Queue TTL (`x-expires`): queues auto-expire when no worker refreshes them, via periodic PEXPIRE with dynamic interval (TTL/2)
- Message TTL (`x-message-ttl`): per-queue message expiry via shorter EXPIRE on message hashes
- `prepare_queue_arguments` override using kombu's `to_rabbitmq_queue_arguments` for RabbitMQ-compatible queue argument handling

### Changed
- Split global `messages_index` sorted set into per-queue `messages_index:{queue}` keys for scoped recovery, clean queue lifecycle, and correct `global_keyprefix` behavior with Lua scripts
- Renamed internal redis-specific naming to client-library-agnostic (`client_lib`, `_client_exceptions`) for better redis-py/valkey-py compatibility
- Default message TTL changed from 3 days to `-1` (no TTL); configurable via `message_ttl` channel attribute
- CI/CD: tag workflow now gates on CI success instead of running on every push

### Fixed
- `EXPIRE` and `PEXPIRE` commands now correctly prefixed when `global_keyprefix` is set
- `_bzmpop_read` and `_get` now skip expired message hashes and try the next message instead of raising `Empty`
- `x-expires` below minimum (10s) now clamped with warning instead of raising `ValueError`
- Removed redundant redis-specific getter functions (`get_redis_error_classes`, `get_redis_ConnectionError`, `_get_response_error`)

## [0.2.5] - 2026-02-14

### Fixed
- Fanout/broadcast (events, Flower) now works: added dedicated subclient for XREAD and fixed per-routing-key stream splitting

### Added
- Example project in `examples/simple/` demonstrating tasks, delayed delivery, priority, retries, and Flower

## [0.2.4] - 2026-01-31

### Added
- Migration support from standard Redis transport

### Fixed
- Simplified transport configuration in docs

## [0.2.3] - 2026-01-29

### Added
- Support for both redis-py and valkey-py client libraries (optional dependencies)
- `valkey://` and `valkeys://` URL scheme support for easier configuration
- SSL/TLS detection from `valkeys://` URL scheme
- Priority clamping for out-of-range values (clamps to 0-255 range with warning)

### Fixed
- Documentation site 404 by setting dev as default version

## [0.2.2] - 2025-01-22

### Changed
- Updated celery-types-ng to 0.25.4 and fixed typing errors

## [0.2.1] - 2025-01-21

### Changed
- Added `queue:` prefix to avoid collision with list-based queues

## [0.2.0] - 2025-01-20

### Added
- Native delayed delivery support
- Full priority support (0-255)
- Reliable fanout via Redis Streams
- Visibility timeout tracking

### Changed
- Switched from Redis lists to sorted sets for queues
- Improved message reliability with per-message hashes

## [0.1.0] - 2025-01-15

### Added
- Initial release
- Custom Kombu transport for Redis/Valkey
- Basic queue operations with sorted sets
