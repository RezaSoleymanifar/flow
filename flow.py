#!/usr/bin/env python3
"""flow: a tiny local runner for workflows of headless Claude steps.

A BLOCK is a reusable step: blocks/<name>/block.yaml (+ prompt.md, + optional check script).
A WORKFLOW wires blocks into a graph: workflows/<name>.yaml. An ITEM is one unit of work moving through it.
Every item-step pair keeps its own Claude session (its own conversation history); the item's state file holds the
pointers. Retry, loop back and jump are all the same move: point the item at a step, resume that step's session.

  flow add WORKFLOW [JSON] [--key K]   new item at the start step; JSON becomes data.input; --key dedupes
  flow tick [WORKFLOW]                 run every ready item until it ends, waits at a gate or gets stuck
  flow status [WORKFLOW]               one JSON line per item
  flow show ITEM                       one item's whole state
  flow approve ITEM                    pass a gate
  flow reject ITEM NOTE                send a gated item back to its step with your note
  flow jump ITEM STEP [NOTE]           move an item to any step (resumes that step's session if it has one)
  flow validate [WORKFLOW]             check workflow files and the blocks they use

Env: FLOW_HOME (default: this folder), FLOW_RUNS (item state, default FLOW_HOME/runs), FLOW_CLAUDE (command that runs claude, default "claude";
the sandbox setup points it at the sandboxed wrapper).
"""
import ast, fcntl, json, os, shlex, subprocess, sys, time, uuid

import yaml
import jsonschema

HOME = os.environ.get("FLOW_HOME") or os.path.dirname(os.path.realpath(__file__))
RUNS = os.environ.get("FLOW_RUNS") or os.path.join(HOME, "runs")
CLAUDE = shlex.split(os.environ.get("FLOW_CLAUDE", "claude"))
TIMEOUT = int(os.environ.get("FLOW_TIMEOUT", "1800"))
REVIEW_SCHEMA = {"type": "object", "required": ["pass", "notes"],
                 "properties": {"pass": {"type": "boolean"}, "notes": {"type": "string"}}}


# ---------- files ----------
def now(): return time.strftime("%Y-%m-%dT%H:%M:%S%z")

def read_yaml(p):
    with open(p) as f: return yaml.safe_load(f) or {}

def write_json(p, obj):   # atomic: a crash never leaves half a state file
    tmp = p + ".tmp"
    with open(tmp, "w") as f: json.dump(obj, f, indent=1)
    os.replace(tmp, p)

def event(item, ev, **detail):
    os.makedirs(RUNS, exist_ok=True)
    with open(os.path.join(RUNS, "events.jsonl"), "a") as f:
        f.write(json.dumps({"ts": now(), "item": item.get("id"), "workflow": item.get("workflow"),
                            "step": item.get("step"), "event": ev, **detail}) + "\n")

def workflow(name):
    wf = read_yaml(os.path.join(HOME, "workflows", name + ".yaml"))
    wf.setdefault("name", name); wf.setdefault("max_attempts", 3)
    return wf

def block(name):
    d = os.path.join(HOME, "blocks", name)
    b = read_yaml(os.path.join(d, "block.yaml"))
    b["dir"] = d
    with open(os.path.join(d, b.get("prompt", "prompt.md"))) as f: b["prompt_text"] = f.read()
    b.setdefault("output", {"type": "object"}); b.setdefault("tools", [])
    return b

def item_dir(wf, iid): return os.path.join(RUNS, wf, iid)

os.umask(0o027)   # state is readable by the sandbox group, writable only by the runner

def load(iid):
    for wf in (os.listdir(RUNS) if os.path.isdir(RUNS) else []):
        p = os.path.join(RUNS, wf, iid, "state.json")
        if os.path.exists(p):
            with open(p) as f: return json.load(f)
    die("no item %s" % iid)

def save(st): write_json(os.path.join(item_dir(st["workflow"], st["id"]), "state.json"), st)

def all_items(wfname=None):
    out = []
    for wf in sorted(os.listdir(RUNS)) if os.path.isdir(RUNS) else []:
        if wfname and wf != wfname: continue
        d = os.path.join(RUNS, wf)
        if not os.path.isdir(d): continue
        for iid in sorted(os.listdir(d)):
            p = os.path.join(d, iid, "state.json")
            if os.path.exists(p):
                with open(p) as f: out.append(json.load(f))
    return out

def die(msg, code=2):
    print(json.dumps({"error": msg})); sys.exit(code)


# ---------- conditions: a safe little expression language over the data ----------
class Obj(dict):
    """dict with dot access; a missing field reads as None so `out.x == "y"` is just False."""
    def __getattr__(self, k): return wrap(self.get(k))

def wrap(v):
    if isinstance(v, dict): return Obj({k: wrap(x) for k, x in v.items()})
    if isinstance(v, list): return [wrap(x) for x in v]
    return v

ALLOWED = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.Compare, ast.Eq, ast.NotEq,
           ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.In, ast.NotIn, ast.Is, ast.IsNot, ast.Name, ast.Load,
           ast.Attribute, ast.Subscript, ast.Constant, ast.List, ast.Tuple, ast.Call)

def parse_cond(expr):
    tree = ast.parse(str(expr), mode="eval")
    for n in ast.walk(tree):
        if not isinstance(n, ALLOWED): raise ValueError("not allowed in a condition: %s" % type(n).__name__)
        if isinstance(n, ast.Call) and not (isinstance(n.func, ast.Name) and n.func.id == "len"):
            raise ValueError("only len() may be called in a condition")
        if isinstance(n, ast.Attribute) and n.attr.startswith("_"): raise ValueError("no private fields")
        if isinstance(n, ast.Name) and n.id not in ("out", "data", "len", "True", "False", "None"):
            raise ValueError("unknown name %s (use out or data)" % n.id)
    return compile(tree, "<when>", "eval")

def evaluate(expr, out, data):
    return eval(parse_cond(expr), {"__builtins__": {}, "len": len}, {"out": wrap(out), "data": wrap(data)})


# ---------- claude ----------
def claude(msg, schema, session, resume, tools=(), mcp=None, model=None, cwd=None):
    """One headless turn. Returns (output_or_None, reason_or_None, session_id, cost)."""
    cmd = CLAUDE + ["-p", msg, "--output-format", "json", "--json-schema", json.dumps(schema),
                    "--permission-mode", "dontAsk", "--strict-mcp-config", "--tools", ",".join(tools)]
    allowed = list(tools) + ["mcp__%s" % s for s in (mcp or {})]
    if allowed: cmd += ["--allowedTools", ",".join(allowed)]
    if mcp: cmd += ["--mcp-config", json.dumps({"mcpServers": mcp})]
    if model: cmd += ["--model", model]
    cmd += ["--resume", session] if resume else ["--session-id", session]
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        return None, "the session timed out", None, None
    try: d = json.loads(p.stdout)
    except ValueError: return None, "claude gave no JSON (exit %s): %s" % (p.returncode, (p.stderr or p.stdout)[-400:]), None, None
    sid, cost = d.get("session_id"), d.get("total_cost_usd")
    if d.get("is_error"): return None, "claude error: %s" % d.get("result"), sid, cost
    out = d.get("structured_output")
    if out is None:
        txt = (d.get("result") or "").strip()
        if txt.startswith("```"): txt = txt.strip("`").split("\n", 1)[-1]
        try: out = json.loads(txt)
        except ValueError: return None, "the answer was not JSON in the required format", sid, cost
    try: jsonschema.validate(out, schema)
    except jsonschema.ValidationError as e: return None, "the answer broke the required format: %s" % e.message, sid, cost
    return out, None, sid, cost


# ---------- one step ----------
def run_check(b, out, st, cwd):
    if not b.get("check"): return None
    path = os.path.join(b["dir"], b["check"])
    argv = [sys.executable, path] if path.endswith(".py") else [path]
    p = subprocess.run(argv, input=json.dumps({"out": out, "data": st["data"]}), capture_output=True,
                       text=True, cwd=cwd, timeout=300)
    return None if p.returncode == 0 else ("check failed: " + (p.stdout.strip() or p.stderr.strip() or "no reason given"))

def review(b, out, st, cwd):
    if not b.get("review"): return None, None
    msg = ("You are reviewing another agent's work. You did not do it. Judge it only against the rubric.\n\n"
           "## Rubric\n%s\n\n## The item\n%s\n\n## The answer to review\n%s\n\n"
           "Return pass (true or false) and notes. If it fails, the notes must say exactly what to fix."
           % (b["review"], json.dumps(st["data"], indent=1), json.dumps(out, indent=1)))
    sid = str(uuid.uuid4())    # always a fresh session: the reviewer never shares history with the worker
    verdict, err, _, cost = claude(msg, REVIEW_SCHEMA, sid, False, model=b.get("review_model"), cwd=cwd)
    event(st, "review", session=sid, passed=bool(verdict and verdict["pass"]), cost=cost)
    if err: return "the reviewer could not answer: " + err, cost
    return (None if verdict["pass"] else "reviewer: " + verdict["notes"]), cost

def worker_msg(b, st, resuming):
    data = json.dumps(st["data"], indent=1)
    fb = st.get("feedback")
    if not resuming:
        m = "%s\n\n## The item\n%s\n" % (b["prompt_text"], data)
        if fb: m += "\n## Note\n%s\n" % fb
        return m + "\nAnswer only with the structured output."
    return ("%s\n\n## The item now\n%s\n\nAnswer again with the structured output."
            % (fb or "This item is back at your step.", data))

def run_step(wf, st):
    step = wf["steps"][st["step"]]
    b = block(step["block"])
    cwd = os.path.join(item_dir(st["workflow"], st["id"]), "work")
    os.makedirs(cwd, exist_ok=True)
    try: os.chmod(cwd, 0o2770)
    except OSError: pass
    st["status"] = "running"; save(st)
    sid = st["sessions"].get(st["step"])
    resuming = sid is not None
    sid = sid or str(uuid.uuid4())
    out, reason, got_sid, cost = claude(worker_msg(b, st, resuming), b["output"], sid, resuming,
                                        b["tools"], b.get("mcp"), b.get("model"), cwd)
    if got_sid: st["sessions"][st["step"]] = got_sid
    event(st, "work", session=got_sid, resumed=resuming, attempt=st["attempt"], ok=reason is None, cost=cost)
    if reason is None: reason = run_check(b, out, st, cwd)
    if reason is None: reason, _ = review(b, out, st, cwd)
    if reason:
        st["attempt"] += 1; st["feedback"] = "Your last answer failed. %s. Fix that." % reason
        st["reason"] = reason
        if st["attempt"] >= wf["max_attempts"]:
            st["status"] = "stuck"; event(st, "stuck", reason=reason)
        else:
            st["status"] = "ready"; event(st, "retry", reason=reason)
        save(st); return
    st["data"][st["step"]] = out; st["feedback"] = None; st["reason"] = None
    if step.get("gate"):
        st["status"] = "waiting"; event(st, "gate"); save(st); return
    route(wf, st, out)

def route(wf, st, out):
    for r in wf["steps"][st["step"]].get("routes", [{"to": "end"}]):
        if "when" in r and not evaluate(r["when"], out, st["data"]): continue
        if "each" in r:   # fan out: one new item per element, then this item is done
            spawned = []
            for i, el in enumerate(evaluate(r["each"], out, st["data"]) or []):
                el = json.loads(json.dumps(el))
                key = str(el.get("key")) if isinstance(el, dict) and el.get("key") else "%s-%d" % (st["id"], i)
                if new_item(st["workflow"], el, key, r["to"]): spawned.append(key)
            event(st, "fan_out", to=r["to"], items=spawned)
            return finish(st, "done")
        if r["to"] == "end": return finish(st, "done")
        event(st, "route", to=r["to"])
        st.update(step=r["to"], status="ready", attempt=0, feedback=None, reason=None); save(st); return
    st["status"] = "stuck"; st["reason"] = "no route matched"; event(st, "stuck", reason=st["reason"]); save(st)

def finish(st, status):
    st["status"] = status; event(st, status); save(st)


# ---------- items ----------
def safe_id(k): return "".join(c if c.isalnum() or c in "-_." else "_" for c in k)[:80]

def new_item(wfname, inp, key=None, step=None):
    wf = workflow(wfname)
    iid = safe_id(key) if key else time.strftime("%Y%m%d%H%M%S-") + uuid.uuid4().hex[:4]
    d = item_dir(wfname, iid)
    if os.path.exists(os.path.join(d, "state.json")): return None   # dedupe: the same key never runs twice
    os.makedirs(d, exist_ok=True)
    st = {"id": iid, "workflow": wfname, "step": step or wf["start"], "status": "ready", "attempt": 0,
          "feedback": None, "reason": None, "data": {"input": inp}, "sessions": {}, "created": now()}
    save(st); event(st, "added"); return st

def recover(wf, st):
    """A 'running' item found by a tick that holds the lock was cut off mid-step (crash, reboot)."""
    b = block(wf["steps"][st["step"]]["block"])
    if b.get("once"):   # a step with real-world effects may have happened: never rerun it blind
        st["status"] = "stuck"; st["reason"] = "cut off mid-step; check whether it already happened, then jump"
    else:
        st["status"] = "ready"
    event(st, "recovered", status=st["status"]); save(st)

def tick(wfname=None):
    os.makedirs(RUNS, exist_ok=True)
    lock = open(os.path.join(RUNS, ".lock"), "w")
    try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError: return {"skipped": "another tick is running"}
    wfs, moved = {}, 0
    for st in all_items(wfname):
        if st["status"] == "running":
            recover(wfs.setdefault(st["workflow"], workflow(st["workflow"])), st)
    for _ in range(1000):   # keep going until nothing is ready: a chain runs end to end in one tick
        ready = [s for s in all_items(wfname) if s["status"] == "ready"]
        if not ready: break
        for st in ready:
            run_step(wfs.setdefault(st["workflow"], workflow(st["workflow"])), st); moved += 1
    return {"steps_run": moved}


# ---------- validate ----------
def validate(name):
    errs = []
    try: wf = workflow(name)
    except Exception as e: return ["%s: cannot read: %s" % (name, e)]
    steps = wf.get("steps") or {}
    if wf.get("start") not in steps: errs.append("start step %r is not a step" % wf.get("start"))
    for s, cfg in steps.items():
        try: block(cfg.get("block", ""))
        except Exception as e: errs.append("%s: block %r: %s" % (s, cfg.get("block"), e))
        for r in cfg.get("routes", []):
            if r.get("to") != "end" and r.get("to") not in steps: errs.append("%s: routes to unknown step %r" % (s, r.get("to")))
            for k in ("when", "each"):
                if k in r:
                    try: parse_cond(r[k])
                    except Exception as e: errs.append("%s: bad %s %r: %s" % (s, k, r[k], e))
        if cfg.get("routes") and "when" in cfg["routes"][-1] and "each" not in cfg["routes"][-1]:
            errs.append("%s: last route has a condition; add a default route so nothing gets stuck" % s)
    return errs


# ---------- cli ----------
def main(a):
    if not a or a[0] in ("-h", "--help", "help"): print(__doc__); return 0
    cmd, rest = a[0], a[1:]
    if cmd == "add":
        key = None
        if "--key" in rest: i = rest.index("--key"); key = rest[i + 1]; rest = rest[:i] + rest[i + 2:]
        if not rest: die("usage: flow add WORKFLOW [JSON] [--key K]")
        st = new_item(rest[0], json.loads(rest[1]) if len(rest) > 1 else {}, key)
        print(json.dumps({"added": st["id"]} if st else {"exists": safe_id(key)})); return 0
    if cmd == "tick": print(json.dumps(tick(rest[0] if rest else None))); return 0
    if cmd == "status":
        for s in all_items(rest[0] if rest else None):
            print(json.dumps({k: s.get(k) for k in ("id", "workflow", "step", "status", "attempt", "reason")}))
        return 0
    if cmd == "show": print(json.dumps(load(rest[0]), indent=1)); return 0
    if cmd in ("approve", "reject", "jump"):
        st = load(rest[0]); wf = workflow(st["workflow"])
        if cmd == "approve":
            if st["status"] != "waiting": die("%s is %s, not waiting at a gate" % (st["id"], st["status"]))
            event(st, "approved"); route(wf, st, st["data"][st["step"]])
        elif cmd == "reject":
            if st["status"] != "waiting": die("%s is %s, not waiting at a gate" % (st["id"], st["status"]))
            note = " ".join(rest[1:]) or "rejected"
            st.update(status="ready", attempt=0, reason=None, feedback="Reza rejected your answer: %s. Redo it." % note)
            event(st, "rejected", note=note); save(st)
        else:
            if len(rest) < 2 or rest[1] not in wf["steps"]: die("usage: flow jump ITEM STEP [NOTE]; steps: %s" % list(wf["steps"]))
            note = " ".join(rest[2:]) or None
            event(st, "jump", to=rest[1]); st.update(step=rest[1], status="ready", attempt=0, reason=None, feedback=note); save(st)
        print(json.dumps({k: st.get(k) for k in ("id", "step", "status")})); return 0
    if cmd == "validate":
        names = rest or [f[:-5] for f in sorted(os.listdir(os.path.join(HOME, "workflows"))) if f.endswith(".yaml")]
        res = {n: validate(n) for n in names}
        print(json.dumps(res, indent=1)); return 1 if any(res.values()) else 0
    die("unknown command %s; see flow --help" % cmd)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
