#!/usr/bin/env python3
"""A stand-in for `claude -p` that behaves like the real CLI wherever the runner depends on it.

Matched against Claude Code 2.1.286 (see tests/fixtures/ and test_contract.py):
  - replies are one JSON object with the real top-level fields; an error is is_error=true with subtype "success"
  - --resume of a session that does not exist: stderr "No conversation found with session ID: <id>", exit 1, no stdout
  - --session-id that is not a UUID: stderr "Error: Invalid session ID. Must be a valid UUID.", exit 1
  - --session-id that is already in use: stderr "Error: Session ID <id> is already in use.", exit 1
  - an unknown option: stderr "error: unknown option '<flag>'", exit 1
Sessions are real files under $FAKE_DIR/sessions, holding each turn, so tests can check who saw what history.

Answers come from $FAKE_DIR/script.json, a list consumed in order. An entry is the structured output to return, or:
  {"__error": "text"}       is_error reply (like "Not logged in"); the session is NOT saved, as with a failed start
  {"__subtype": "name"}     is_error reply with that subtype (e.g. error_max_structured_output_retries)
  {"__raw": "text"}         success with plain result text and no structured_output
  {"__crash": "text", "code": 2}   no JSON at all: text on stderr, that exit code
  {"__sleep": 3}            sleep, then answer {}
Every call is logged to $FAKE_DIR/calls.jsonl."""
import fcntl, json, os, re, sys, time, uuid

D = os.environ["FAKE_DIR"]
VALUE = {"-p", "--output-format", "--json-schema", "--permission-mode", "--tools", "--allowedTools", "--mcp-config",
         "--model", "--resume", "--session-id"}
BOOL = {"--strict-mcp-config"}
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def fail(msg, code=1):
    sys.stderr.write(msg + "\n"); sys.exit(code)


args, a = {}, sys.argv[1:]
i = 0
while i < len(a):
    f = a[i]
    if f in VALUE:
        if i + 1 >= len(a): fail("error: option '%s' argument missing" % f)
        args[f] = a[i + 1]; i += 2
    elif f in BOOL: args[f] = True; i += 1
    else: fail("error: unknown option '%s'" % f)
if args.get("--output-format") != "json": fail("error: the runner must ask for --output-format json")
if "--resume" in args and "--session-id" in args: fail("Error: --session-id cannot be used with --resume")
sid = args.get("--resume") or args.get("--session-id")
if sid is None: sid = str(uuid.uuid4())
elif not UUID.match(sid): fail("Error: Invalid session ID. Must be a valid UUID.")

sdir = os.path.join(D, "sessions"); os.makedirs(sdir, exist_ok=True)
spath = os.path.join(sdir, sid + ".json")
history = json.load(open(spath)) if os.path.exists(spath) else []
with open(os.path.join(D, "calls.jsonl"), "a") as log:
    log.write(json.dumps({"msg": args.get("-p"), "resume": args.get("--resume"), "session_id": args.get("--session-id"),
                          "tools": args.get("--tools"), "allowed": args.get("--allowedTools"), "model": args.get("--model"),
                          "mcp": args.get("--mcp-config"), "permission": args.get("--permission-mode"),
                          "schema": json.loads(args.get("--json-schema") or "null"), "cwd": os.getcwd(),
                          "history": len(history) if os.path.exists(spath) else None}) + "\n")


if "--resume" in args and not os.path.exists(spath): fail("No conversation found with session ID: %s" % sid)
if "--session-id" in args and os.path.exists(spath): fail("Error: Session ID %s is already in use." % sid)

p = os.path.join(D, "script.json")
with open(os.path.join(D, ".lock"), "w") as lk:          # fakes run in parallel: pop the next answer atomically
    fcntl.flock(lk, fcntl.LOCK_EX)
    script = json.load(open(p))
    ans = script.pop(0) if script else {"__error": "fake script ran out"}
    json.dump(script, open(p, "w"))

if "__crash" in ans: fail(ans["__crash"], ans.get("code", 1))
if "__sleep" in ans: time.sleep(ans["__sleep"]); ans = {}


def reply(**kw):   # the real reply's top-level fields (see fixtures/real_not_logged_in.json)
    base = {"type": "result", "subtype": "success", "is_error": False, "duration_ms": 5, "duration_api_ms": 4,
            "num_turns": 1, "result": "", "session_id": sid, "total_cost_usd": 0.01, "usage": {}, "modelUsage": {},
            "permission_denials": [], "uuid": str(uuid.uuid4())}
    base.update(kw); print(json.dumps(base))


if "__error" in ans: reply(is_error=True, result=ans["__error"], total_cost_usd=0); sys.exit(0)
if "__subtype" in ans: reply(is_error=True, subtype=ans["__subtype"], result=""); sys.exit(0)
history.append({"user": args.get("-p"), "assistant": ans})
json.dump(history, open(spath, "w"))
if "__raw" in ans: reply(result=ans["__raw"])
else: reply(structured_output=ans)
