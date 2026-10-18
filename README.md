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
flow validate               # check every workflow and its blocks
flow add WORKFLOW '{"name": "Sam"}' --key sam
flow tick                   # run ready items (cron does this)
flow status | flow show ITEM
flow approve ITEM | flow reject ITEM NOTE | flow jump ITEM STEP [NOTE]
```

Every command prints JSON, so an agent can operate it. Tests run against a fake Claude:
`python3 -m unittest discover -s tests`
