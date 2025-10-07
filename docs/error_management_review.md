# Error Management Review

This document summarizes observed gaps in error reporting and handling across the codebase. Each section lists the risky behavior and potential remediations.

## Node Registry Adapter

* `buildArguments` swallows JSON decoding failures by silently substituting zero values when state or config unmarshalling fails, giving nodes no signal that their inputs were malformed. This can hide schema drift or user mistakes and lead to undefined execution paths.【F:internal/adapters/node_registry/adapter.go†L210-L246】
  * **Suggested Code Changes:**
    * Return the `json.Unmarshal` error instead of defaulting to zero values and thread that failure through `Execute`, ultimately surfacing it to the caller so the workflow run fails fast.
    * Add context to the error with node ID and parameter index (e.g., `fmt.Errorf("decode %s %d: %w", source, idx, err)`) to simplify debugging.
    * Cover the new behavior with tests that assert `buildArguments` fails when handed malformed state or config payloads.

## Queue Adapter

* While dequeuing, `ProcessAfter` metadata that fails to decode is treated as zero time, immediately making the item eligible for execution even if the payload is corrupt. This can cause premature retries or poison-pill loops.【F:internal/adapters/queue/queue.go†L158-L220】
  * **Suggested Code Changes:**
    * When `json.Unmarshal` fails, return an error that includes the queue name and sequence to allow operators to locate the bad payload.
    * Update the caller (likely `Next`) to treat this error the same as a storage failure—ensuring the item is either requeued with backoff or moved to a DLQ instead of being considered ready.
    * Add regression tests to confirm corrupted metadata blocks dispatch and increments a metric or log for observability.

## Raft FSM

* Version metadata failures inside `applyAtomicIncrement` and `getAuthoritativeVersion` are ignored; the code proceeds with zero-valued versions if deserialization fails. That risks monotonicity violations and corrupted version tracking after Badger data issues.【F:internal/adapters/raft/fsm.go†L225-L260】【F:internal/adapters/raft/fsm.go†L482-L504】
  * **Suggested Code Changes:**
    * Propagate the error returned from `json.Unmarshal` to the caller and prevent further state mutation when version metadata cannot be decoded.
    * Wrap errors with the key/version identifier to aid root-cause investigation.
    * Extend FSM tests to cover corrupted version metadata inputs and assert that state is left unchanged.

## Engine State Manager

* Multiple helpers marshal and unmarshal workflow instances without checking errors. Any serialization issue (invalid JSON tags, new fields with custom marshalers) will be ignored, leading to inconsistent statistics, clone, or snapshot data.【F:internal/adapters/engine/state_manager.go†L454-L520】
  * **Suggested Code Changes:**
    * Return marshaling/unmarshaling failures to the caller functions and ensure the state manager stops persisting partially computed state when serialization fails.
    * Emit structured logs (workflow ID, attempt, node) before returning to preserve visibility.
    * Add unit tests that inject invalid JSON and assert the manager reports the error and avoids updating Badger.

## Domain Commands

* `DevCommand.ToInternalCommand` discards JSON marshaling errors and still emits a command, leaving downstream handlers to process a malformed payload.【F:internal/domain/commands.go†L178-L194】
  * **Suggested Code Changes:**
    * Change `DevCommand.ToInternalCommand` to return `(domain.InternalCommand, error)` and bubble the marshaling error, updating call sites accordingly.
    * Guard callers with tests verifying that invalid JSON now yields an error and no command is enqueued.

## App Storage

* TTL metadata reads and writes ignore JSON failures. When parsing stored TTLs fails, the expiration is treated as absent, and writes never report marshal failures, hiding Badger corruption or clock issues.【F:internal/adapters/storage/app_storage.go†L95-L455】【F:internal/adapters/storage/app_storage.go†L392-L596】
  * **Suggested Code Changes:**
    * Return marshaling/unmarshaling errors from TTL helper functions and short-circuit updates when metadata cannot be validated.
    * Introduce structured logging or metrics increments tied to the storage namespace to flag corruption.
    * Add tests for corrupted TTL payloads verifying the operation aborts and no data is written.
  
* Transaction-level deletes ignore the error return value from Badger's `Delete`, so disk or transaction errors are silently suppressed.【F:internal/adapters/storage/app_storage.go†L587-L595】
  * **Suggested Code Changes:**
    * Capture and return the result of `txn.Delete` operations so callers can react to Badger errors.
    * Update transactional helper tests to assert delete failures are surfaced.

## Next Steps

* Decide on a consistent error-handling policy (e.g., fail fast vs. soft fail with logging) and update these hotspots accordingly.
* Consider adding structured logging around adapter boundaries so serialization issues are captured with workflow/node identifiers.
* Add unit tests that force decode failures to ensure the new behavior surfaces errors instead of proceeding silently.
