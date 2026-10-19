# local-agent-orchestrator

A tiny local runner for workflows of headless Claude Code sessions. One Python file, no server, no database.

- **Block**: a reusable step in `blocks/<name>/`. `block.yaml` holds the answer format (JSON Schema), allowed tools,
  an optional check script, an optional reviewer rubric, and `once: true` for steps with real-world effects.
  `prompt.md` holds the instructions.
- **Workflow**: `workflows/<name>.yaml` wires blocks into a graph. Routes are checked top to bottom; the first match
  wins. `when:` is a small safe expression over the step's answer (`out`) and the item's data (`data`). `each:` fans
  out, one new item per element. `gate: true` waits for a human.
- **Item**: one unit of work moving through a workflow, with its own state file and work folder.

```yaml
start: draft
steps:
  draft:
    block: draft-reply
    gate: true
    routes:
      - when: out.intent == "decline"
        to: archive
      - to: send
  send:    {block: send, routes: [{to: end}]}
  archive: {block: archive, routes: [{to: end}]}
```

An **external** block (`external: chrome`, `external: human`) is done by someone outside the engine: the item waits
in status `external` until `flow done ITEM JSON` or `flow fail ITEM REASON` brings the result, which then goes
through the same format check, check script and reviewer as a worker's answer. This is how a step that needs a
logged-in browser or a person plugs into an otherwise headless workflow.

A **driver** (`driver: connect.py` in block.yaml) is a script that does the step for zero tokens: the item's data
in on stdin, the answer out as JSON, then the same format check, check script, reviewer and record as any
answer. Exit 3 means "I can't handle this one" and the item falls back to the block's `external` owner (an agent
or a person), so the pattern is: let the agent do it once or twice, distill the script into the block folder, keep
the agent as the fallback. One script per block; the block boundary is the script boundary.

A check script exits 0 to pass, 1 to fail with a reason the worker can fix (printed on stdout), or 2 to fail for a
reason no retry can change, such as a do-not-contact rule: the item stops at once.

A **record** script (`record: record.py` in block.yaml) runs after a step passes and before it routes on. It gets
the answer and the item's data on stdin and writes whatever the workflow must never forget (a message sent, a
verdict) to the workflow's own store, such as a database. If it fails, the item stops: nothing moves on unrecorded.

**Parallelism** is not in the workflow files. The engine runs up to `FLOW_PARALLEL` ready items at once (default: no cap; set a number to throttle) 
once, oldest first; each still gets its own session, checks, reviewer and record. A block with `lane: <name>` runs
one item at a time in that lane, for a resource that cannot be shared (a logged-in browser, a printer); the name is
the workflow's own. A route whose `to` is a list forks the item, one child per branch, each carrying the data so far.

**Batching** is the opposite: one step handles many things in one go (one search that finds twenty people, one
inbox read that answers ten mails) and answers with a list. The record script sees the whole list once; an `each:`
route then makes one item per element for the steps that follow, where parallelism and lanes take over. Each child
gets its element as `data.input` and the batch's own input as `data.batch`, so a preset set on the batch is visible
downstream (`when: data.batch.channel == "email"`).

**Fan-in** closes a fan-out. A step with `join: true` and no block gathers the children of one `each:` fan-out
(or one fork). Each child's result is written into ONE merged item the moment it arrives, so finished work is on
disk at once: `data.<join step>` = `{parts, items, missing}` (arrived children's data, their ids, siblings that got
stuck). With `gate: true` you can approve the merge any time, with whatever has arrived; without a gate it moves on
once no sibling can still arrive. Nobody is too late: a child arriving after its merge was approved or moved on
opens the next round (`ID.STEP-2`, `-3`, ...), so you can stop whenever and continue from what is already done.

```yaml
classify: {block: classify, routes: [{to: merge}]}     # many children, in parallel
merge:    {join: true, gate: true, routes: [{to: apply}]}   # one item, one approval
```

## How a step runs

worker turn → answer matches the format? → check script → fresh reviewer → pass: follow the routes, or wait at the
gate. Fail: retry in the same session with the reason. After `max_attempts` (default 3) the item is `stuck`.

Every item-step pair keeps its own Claude session, so its conversation history is its own. The item's state file
holds the session pointers; retry, loop back, reject and jump all resume the target step's session. The reviewer is
always a fresh session.

## Safety

Sessions run as a locked-down user (`setup.sh`) that cannot read your home folder or write anything except the item's
work folder. Checks, prompts and state belong to the runner, so a session cannot tamper with how it is judged. Each
block gets only its listed tools. A `once` step cut off mid-run is never retried blind.

## Use

```
sudo bash setup.sh          # once: sandbox user, Claude token, the `flow` command, a 15-minute schedule
flow new block NAME [--external WHO] [--check] [--record]   # scaffold a block; flow new workflow NAME BLOCK... chains blocks
flow validate               # check every workflow and its blocks
flow add WORKFLOW '{"name": "Sam"}' --key sam
flow add WORKFLOW @preset --key sam    # a named input from flavors.yaml ({workflow: {preset: input}}): one graph, many presets
flow tick                   # run ready items (cron does this)
flow status | flow show ITEM
flow report [WORKFLOW] [--since DATE]   # items, Claude calls, cost, retries, wall time, and per-step waits, from the events log
flow approve ITEM | flow reject ITEM NOTE | flow jump ITEM STEP [NOTE]
flow done ITEM '{"sent": true}' | flow fail ITEM REASON     # results of external steps
```

Every command prints JSON, so an agent can operate it.

## Tests

Three layers, so the fake can never drift from the real thing:

- `tests/test_flow.py`: every behavior and failure case, against `tests/fake_claude.py`, a fake that behaves like the
  real CLI where the runner depends on it: the same reply fields, real session files, and the same errors for a
  missing session, a reused or malformed session ID, and an unknown flag.
- `tests/test_contract.py`: real replies captured from Claude Code (`tests/fixtures/`) parse correctly, the fake fails
  exactly like the real CLI, and every flag the runner passes exists in the installed `claude` (runs wherever one is
  installed; no login needed).
- `tests/test_live.py`: the same core behaviors against real Claude (structured output, resume memory, tool limits,
  sandbox escape attempts). Opt in with `FLOW_LIVE=1`; add `FLOW_RECORD=tests/fixtures/recorded` to keep the raw
  replies, which the contract tests then check too.

```
python3 -m unittest discover -s tests                     # fake + contract
FLOW_LIVE=1 FLOW_LIVE_SANDBOX=1 FLOW_CLAUDE="sudo -n -u flowagent /usr/local/bin/flow-claude" \
  FLOW_RECORD=tests/fixtures/recorded python3 -m unittest tests.test_live
```
