#!/usr/bin/env python3
"""Stands in for `claude -p` in tests. Answers come from $FAKE_DIR/script.json, a list consumed in order; each entry is
the structured output to return, or {"__error": "..."} / {"__raw": "text"}. Every call is logged to calls.jsonl."""
import json, os, sys

d = os.environ["FAKE_DIR"]
a = sys.argv[1:]
def opt(k): return a[a.index(k) + 1] if k in a else None
sid = opt("--resume") or opt("--session-id")
with open(os.path.join(d, "calls.jsonl"), "a") as f:
    f.write(json.dumps({"msg": opt("-p"), "resume": opt("--resume"), "session_id": opt("--session-id"),
                        "tools": opt("--tools"), "allowed": opt("--allowedTools"), "schema": json.loads(opt("--json-schema")),
                        "cwd": os.getcwd()}) + "\n")
p = os.path.join(d, "script.json")
script = json.load(open(p))
ans = script.pop(0) if script else {"__error": "fake script ran out"}
json.dump(script, open(p, "w"))
if "__error" in ans: print(json.dumps({"is_error": True, "result": ans["__error"], "session_id": sid}))
elif "__raw" in ans: print(json.dumps({"is_error": False, "result": ans["__raw"], "session_id": sid, "total_cost_usd": 0.01}))
else: print(json.dumps({"is_error": False, "result": "", "structured_output": ans, "session_id": sid, "total_cost_usd": 0.01}))
