# go-ads Code Review Protocol

Three-pass protocol. Each pass finds bugs the others can't. A complete review runs all three passes.

## Pass 1 — Structural review

**Goal**: catch bugs in code shape before considering what the code is supposed to do.

**Looks at**: type safety, lock ordering, error paths, resource management, naming, dead code, duplication.

**Tools**:
- `golangci-lint run ./...` — must exit zero
- `go test -race -count=1 ./...` — must pass
- Multi-agent review (Claude general-purpose × 3 + CodeRabbit) per `/code-review-full` — covers naming, structure, edge cases
- Optional: Qwen3-Coder via Ollama for cross-model second opinion

**Catches**:

- Nil pointer dereferences
- Goroutine leaks (missing waitGroup tracking, ctx-not-honored loops)
- Lock-ordering violations (cache.lock + notifs.lock simultaneously held)
- Dead branches (post-CAS path that can never execute)
- Missing error checks (unchecked `_ = something()`)
- Duplicated logic that's obvious to extract
- Naming inconsistency (initialism caps, stutter, semantic clash like `Valid` field vs `IsValid()` method)
- Off-by-one in loops or buffer indexing
- Race conditions visible in code structure (lock-then-network-call patterns)

**Misses**:

- "Code looks fine, but is it the BEHAVIOR we wanted?" — pass 2's job
- Interactions between two correct-looking pieces of code — pass 3's job

**Output**: per-finding entry with file:line, severity, description, suggested fix, confidence. Severity per `02-quality-constitution.md` (CRITICAL/HIGH/MEDIUM/LOW/NIT).

## Pass 2 — Requirement verification

**Goal**: walk every requirement in `01-requirements.md` and verify the code satisfies it.

**Looks at**: trace from R-XXX-NNN to the code path that implements it, verify invariants hold, identify gaps.

**Procedure**:

For each requirement R-XXX-NNN:

1. **Identify the code path**. Where in the codebase is this requirement implemented? Read the relevant function(s).
2. **Verify invariants**. Each requirement lists invariants. Does the code maintain them? Trace the data flow.
3. **Verify the verification**. The requirement names test IDs (T-X-NNN) that should prove satisfaction. Do those tests exist? Do they actually exercise the requirement (not coverage theater per §02)?
4. **Confirm priority**. Is the implementation effort proportional to priority? CRITICAL requirements need explicit testing; LOW may not.
5. **Cite evidence**. In the review output, cite file:line for each requirement entry.

**Catches**:

- Specification gaps: a documented behavior with no implementing code
- Implementation gaps: code that doesn't actually satisfy the requirement (e.g., R-NOT-003 says "re-check post-roundtrip" but code only pre-checks)
- Test gaps: a requirement with no test
- Outdated requirements: documented behavior that current code no longer implements (delete or restore)

**Misses**:

- Bugs in unspecified code paths — file new requirements for those
- Cross-requirement inconsistency — pass 3's job

**Output**: requirement-by-requirement table with status (`SATISFIED` / `GAP / TEST MISSING` / `OUTDATED`) and evidence (file:line + test ID).

## Pass 3 — Cross-requirement consistency

**Goal**: verify requirements that interact don't contradict each other in the implementation.

**Looks at**: code paths that satisfy multiple requirements simultaneously; edge cases at requirement boundaries; emergent behavior from interaction.

**Procedure**:

Build a matrix of interacting requirements (interactions are non-trivial when two requirements constrain the same code or data). For each pair:

1. **Identify the shared code path** if any.
2. **Verify both requirements hold**. Does the code path simultaneously satisfy both? Construct an adversarial scenario (Section 3 of the constitution's fitness-to-purpose scenarios are pre-built adversarial scenarios).
3. **Look for ordering hazards**. If two requirements impose ordering constraints on the same lock or atomic, do they agree?

**Known interaction matrix** (non-exhaustive — extend as new requirements land):

| Requirement A           | Requirement B                | Shared concern              | Hazard                       |
|-------------------------|------------------------------|-----------------------------|------------------------------|
| R-CACHE-008 (NEVER both) | R-NOT-003 (TOCTOU re-check) | cache.lock + notifications.lock | re-check must release-then-acquire; tested |
| R-NOT-004 (epoch)       | R-NOT-005 (handleNotif re-resolve) | sessionFSM.epoch        | both must use SAME epoch source (see 01-requirements.md R-RECON-005) |
| R-NOT-006 (non-blocking dispatch) | R-TX-006 (recvWorker pool) | listen → recvWorker         | drop-on-overflow consistency |
| R-RECON-008 (waitGroup) | R-SES-003 (Close idempotent) | lifecycle.waitGroup         | Close→Wait order vs reconnect Add ordering |
| R-NOT-013 (resubscribe retry) | R-NOT-014 (rollback) | notificationConfigs         | re-append vs restore — both must converge |
| R-VIEW-001 (snapshot)   | R-NOT-005 (live re-resolve) | symbol pointer staleness    | view stale OK; notif must use live |
| R-CACHE-007 (on-demand) | R-CACHE-003 (epoch bump)   | cache.symbols mutation     | insert ≠ swap; epoch NOT bumped on insert |
| R-SUM-003 (per-opcode state) | R-NOT-014 (rollback)      | resubscribe Sum*Notification flow | both opcodes probed independently |
| R-PARSE-001 (parse walks tree) | R-CACHE-002 (cache.lock guards) | Symbol Value mutation   | parse must run under cache.lock |

**Catches**:

- Bugs that emerge only when two correct features interact (e.g., R-NOT-004's epoch re-check would be useless if R-NOT-005 didn't ALSO re-resolve via FullName).
- Inconsistencies in lock ordering across multiple paths.
- Requirements that say "X SHALL bump epoch" while another path forgets to.
- Test gaps for the interaction (each individual requirement tested, but not the pair).

**Misses**:

- Scope-bounded by the matrix; new interactions need to be added to the matrix as the codebase evolves.
- Emergent bugs at higher-than-pairwise interaction (rare).

**Output**: matrix table with each cell marked `OK` / `BUG` / `TEST MISSING`.

## Combined review checklist

Use this for `/code-review-full` runs against `dev` HEAD.

### Pre-review setup

- [ ] On a clean dev branch HEAD; no uncommitted changes
- [ ] `go build ./...` exits zero
- [ ] `golangci-lint run ./...` exits zero
- [ ] `go test -count=1 -race ./...` passes
- [ ] `go test -tags integration -run NONEXISTENT_TEST ./...` builds (integration tests compile)

### Pass 1 — Structural

- [ ] Multi-agent review dispatched (Architecture, Errors+Concurrency, Devil's Advocate, CodeRabbit)
- [ ] Optional: Qwen3-Coder via Ollama
- [ ] All findings consolidated, deduplicated by file+line+issue
- [ ] Each finding scored: HIGH (multiple agents agree) / MEDIUM (one agent + clear evidence) / LOW (one agent, subjective)
- [ ] Severity mapped per `02-quality-constitution.md`

### Pass 2 — Requirement verification

- [ ] For each module in `01-requirements.md`, walk every R-XXX-NNN
- [ ] Cite file:line for the implementation
- [ ] Confirm test IDs are real and pass
- [ ] Mark requirement state: SATISFIED / GAP / TEST MISSING / OUTDATED
- [ ] File new requirements for unspecified behaviors found

### Pass 3 — Cross-requirement consistency

- [ ] Walk the interaction matrix
- [ ] For each cell, mark OK / BUG / TEST MISSING
- [ ] For BUG cells: open issue or file under `07-bug-report.md` as B-NNN
- [ ] Verify that fixing one requirement didn't break another

### Output

A review report contains:

1. Pass 1 findings (severity-grouped)
2. Pass 2 verification table
3. Pass 3 interaction matrix
4. Aggregate verdict: READY-TO-MERGE / NEEDS-FIX / NEEDS-NEW-REQUIREMENTS

A review is not done until all three passes are complete.

## When to skip a pass

Pass 1 is non-negotiable. Pass 2 and Pass 3 may be scoped:

- **Pass 2 scoped to changed modules**: a 5-file PR doesn't need to walk all 11 modules; only walk modules whose requirements the change might affect.
- **Pass 3 scoped to interactions touching changed modules**: only matrix cells where one of the two requirements is in a changed module.

Never skip Pass 1 entirely. Never skip Pass 2 for changes that touch CRITICAL requirements (R-SES-002, R-CACHE-002, R-CACHE-008, R-RECON-002, R-RECON-008, R-NFR-002).
