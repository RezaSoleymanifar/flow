#!/usr/bin/env python3
"""flow: a tiny local runner for workflows of headless Claude steps.

A BLOCK is a reusable step: blocks/<name>/block.yaml (+ prompt.md, + optional check script).
A WORKFLOW wires blocks into a graph: workflows/<name>.yaml. An ITEM is one unit of work moving through it.
Every item-step pair keeps its own Claude session (its own conversation history); the item's state file holds the
pointers. Retry, loop back and jump are all the same move: point the item at a step, resume that step's session.

  flow add WORKFLOW [JSON|@FLAVOR] [--key K] [--step S]   new item (at the start step, or S); JSON, or a preset from
                                       FLOW_HOME/flavors.yaml ({workflow: {flavor: input}}), becomes data.input; --key dedupes
  flow tick [WORKFLOW]                 run every ready item until it ends, waits at a gate or gets stuck
  flow report [WORKFLOW] [--since DATE]   items, Claude calls, cost, tokens (in, cached, out), retries, wait per step
  flow status [WORKFLOW]               one JSON line per item
  flow show ITEM                       one item's whole state (ITEM may be WORKFLOW/ID when ids collide)
  flow approve ITEM                    pass a gate
  flow reject ITEM NOTE                send a gated item back to its step with your note
  flow jump ITEM STEP [NOTE]           move an item to any step (resumes that step's session if it has one)
  flow done ITEM JSON                  hand an external step its result (checked like a worker's answer)
  flow fail ITEM REASON                report that an external step could not be done
  flow validate [WORKFLOW]             check workflow files and the blocks they use
  flow new block NAME [--external WHO] [--check] [--record]   scaffold blocks/NAME (block.yaml, prompt.md, stubs)
  flow new workflow NAME BLOCK [BLOCK...]                      scaffold workflows/NAME.yaml as a straight chain

A block with no tools (a pure judge) runs with a bare system prompt, no settings or project memory, in a scratch
folder: nothing but prompt.md and the item reaches the model.
A block with `driver: <script>` is done by that script for zero tokens: item data in on stdin, the answer out as
JSON, then the same checks and record as any answer. Exit 3 = "I can't handle this one": the item falls back to
the block's `external` owner. One script per block; the block boundary is the script boundary.
A block with `external: <who>` (e.g. chrome, human) is not run by a Claude session: the item waits in status
"external" until `flow done` or `flow fail` brings the result. The result goes through the block's format,
check and reviewer like any other answer.

Env: FLOW_HOME (default: ./flows or . of the folder you run it from, else this folder), FLOW_RUNS (item state, default FLOW_HOME/runs), FLOW_CLAUDE (command that
runs claude, default "claude"; setup.sh points it at the sandboxed wrapper), FLOW_TIMEOUT (seconds per turn),
FLOW_RECORD (folder: save every raw claude reply, to build test fixtures from real runs), FLOW_PARALLEL (items at once; default 2 per CPU core, max 32; 0 = no cap)
a block with `lane: <name>` runs one item at a time in that lane, oldest first).

A step's `on_stuck: STEP` sends the item to STEP once its retries run out, instead of stopping it.
A route's `to` may be a list: the item forks, one child per branch, each with its own sessions.
`each:` children get the element as data.input and the batch's input as data.batch.
"""
import ast, json, os, re, shlex, subprocess, sys, tempfile, threading, time, uuid
try: import fcntl
except ImportError: fcntl = None   # Windows: msvcrt byte-range locks stand in (see _lock)

def _lock(f, shared=False, slot=0):
    """Non-blocking file lock. POSIX: flock. Windows: a shared lock holds one byte in 1..999 (slot), an exclusive lock
    holds bytes 0..999, so an exclusive lock waits for every shared holder and vice versa. Raises BlockingIOError if busy."""
    if fcntl: fcntl.flock(f, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB); return
    import msvcrt
    f.seek(1 + slot % 999 if shared else 0)
    try: msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1 if shared else 1000)
    except OSError as e: raise BlockingIOError(str(e))


import yaml
import jsonschema

def _home():
    """Where the blocks and workflows live: FLOW_HOME if set, else ./flows or . when run from a project, else here."""
    if os.environ.get("FLOW_HOME"): return os.environ["FLOW_HOME"]
    for cand in (os.path.join(os.getcwd(), "flows"), os.getcwd()):
        if os.path.isdir(os.path.join(cand, "workflows")): return cand
    return os.path.dirname(os.path.realpath(__file__))
HOME = _home()
RUNS = os.environ.get("FLOW_RUNS") or os.path.join(HOME, "runs")
CLAUDE = shlex.split(os.environ.get("FLOW_CLAUDE", "claude"))
TIMEOUT = int(os.environ.get("FLOW_TIMEOUT", "1800"))
RECORD = os.environ.get("FLOW_RECORD")
# Items run in parallel. Default: two per CPU core, at most 32: each Claude session is a Node process, and an
# unlimited fan-out (577 items at once, 2026-10-19) drove a WSL box to load 180. FLOW_PARALLEL overrides; 0 = unlimited.
_env = os.environ.get("FLOW_PARALLEL")
PARALLEL = (int(_env) or 10**6) if _env else max(4, min(32, 2 * (os.cpu_count() or 2)))
REVIEW_SCHEMA = {"type": "object", "required": ["pass", "notes"],
                 "properties": {"pass": {"type": "boolean"}, "notes": {"type": "string"}}}
os.umask(0o027)   # state is readable by the sandbox group, writable only by the runner


class FinalFail(str):
    """A check verdict that no retry can change (a hard rule). The item stops at once."""


class ConfigError(Exception):
    """A workflow or block is broken. The item stops with the reason; the runner keeps going."""


# ---------- files ----------
def now(): return time.strftime("%Y-%m-%dT%H:%M:%S%z")

def read_yaml(p):
    with open(p, encoding="utf-8") as f: return yaml.safe_load(f) or {}

def write_json(p, obj):   # atomic: a crash never leaves half a state file
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(obj, f, indent=1)
    os.replace(tmp, p)

def event(item, ev, **detail):
    os.makedirs(RUNS, exist_ok=True)
    with open(os.path.join(RUNS, "events.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": now(), "item": item.get("id"), "workflow": item.get("workflow"),
                            "step": item.get("step"), "event": ev, **detail}) + "\n")

def flavor(wfname, name):
    """A named preset of item input, from FLOW_HOME/flavors.yaml: {workflow: {flavor: {...input}}}."""
    p = os.path.join(HOME, "flavors.yaml")
    if not os.path.exists(p): die("no flavors.yaml in %s" % HOME)
    with open(p, encoding="utf-8") as f: fl = yaml.safe_load(f) or {}
    if name not in (fl.get(wfname) or {}): die("no flavor %r for workflow %s (have: %s)" % (name, wfname, ", ".join(fl.get(wfname) or [])))
    inp = fl[wfname][name]
    if not isinstance(inp, dict): die("flavor %s.%s must be a mapping" % (wfname, name))
    return dict(inp, flavor=name)


def workflow(name):
    p = os.path.join(HOME, "workflows", name + ".yaml")
    if not os.path.exists(p): raise ConfigError("no workflow %r" % name)
    wf = read_yaml(p)
    wf.setdefault("name", name); wf.setdefault("max_attempts", 3); wf.setdefault("max_steps", 100)
    if not isinstance(wf.get("steps"), dict) or wf.get("start") not in wf["steps"]:
        raise ConfigError("workflow %r needs steps and a start step" % name)
    return wf

def block(name):
    d = os.path.join(HOME, "blocks", str(name))
    if not os.path.exists(os.path.join(d, "block.yaml")): raise ConfigError("no block %r" % name)
    b = read_yaml(os.path.join(d, "block.yaml"))
    b["dir"] = d
    p = os.path.join(d, b.get("prompt", "prompt.md"))
    if not os.path.exists(p): raise ConfigError("block %r has no prompt file" % name)
    with open(p, encoding="utf-8") as f: b["prompt_text"] = f.read()
    def _inc(m):                       # {{include path}}: splice a file in, path relative to the flows home
        q = os.path.join(HOME, m.group(1).strip())
        if not os.path.exists(q): raise ConfigError("block %r includes a missing file %s" % (name, m.group(1)))
        with open(q, encoding="utf-8") as f: return f.read()
    b["prompt_text"] = re.sub(r"\{\{\s*include\s+([^}]+)\}\}", _inc, b["prompt_text"])
    if b.get("review"): b["review"] = re.sub(r"\{\{\s*include\s+([^}]+)\}\}", _inc, str(b["review"]))
    b.setdefault("output", {"type": "object"}); b.setdefault("tools", []); b.setdefault("external", None)
    try: jsonschema.Draft202012Validator.check_schema(b["output"])
    except jsonschema.SchemaError as e: raise ConfigError("block %r output is not a valid schema: %s" % (name, e.message))
    return b

def item_dir(wf, iid): return os.path.join(RUNS, wf, iid)

def all_items(wfname=None, bad=None):
    """Every readable item. Unreadable state files are listed in `bad`, never crash the runner."""
    out = []
    for wf in sorted(os.listdir(RUNS)) if os.path.isdir(RUNS) else []:
        if wfname and wf != wfname: continue
        d = os.path.join(RUNS, wf)
        if not os.path.isdir(d): continue
        for iid in sorted(os.listdir(d)):
            p = os.path.join(d, iid, "state.json")
            if not os.path.exists(p): continue
            try:
                with open(p, encoding="utf-8") as f: out.append(json.load(f))
            except (ValueError, OSError):
                if bad is not None: bad.append("%s/%s" % (wf, iid))
    return out

def load(iid):
    """An item by id, or by WORKFLOW/ID when the same id exists in more than one workflow."""
    wf = None
    if "/" in iid: wf, iid = iid.split("/", 1)
    hits = [st for st in all_items(wf) if st["id"] == iid]
    if not hits: die("no item %s" % (wf + "/" + iid if wf else iid))
    if len(hits) > 1: die("item %s exists in %s; say which: %s" % (iid, ", ".join(h["workflow"] for h in hits),
                                                                  " or ".join("%s/%s" % (h["workflow"], iid) for h in hits)))
    return hits[0]

def save(st): write_json(os.path.join(item_dir(st["workflow"], st["id"]), "state.json"), st)

def die(msg, code=2):
    print(json.dumps({"error": msg})); sys.exit(code)


# ---------- conditions: a safe little expression language over the data ----------
class Missing:
    """What a missing field reads as, however deep: equal to None, false, empty. So `out.a.b == "y"` is just False."""
    def __getattr__(self, k): return self
    def __getitem__(self, k): return self
    def __eq__(self, o): return o is None or isinstance(o, Missing)
    def __ne__(self, o): return not self.__eq__(o)
    def __hash__(self): return 0
    def __bool__(self): return False
    def __len__(self): return 0
    def __iter__(self): return iter(())
    def __contains__(self, x): return False
MISSING = Missing()

class Obj(dict):
    """dict with dot access; a missing field reads as MISSING."""
    def __getattr__(self, k): return wrap(self[k]) if k in self else MISSING
    def __getitem__(self, k): return dict.__getitem__(self, k) if k in self else MISSING

def wrap(v):
    if v is None: return MISSING
    if isinstance(v, dict): return Obj({k: wrap(x) for k, x in v.items()})
    if isinstance(v, list): return [wrap(x) for x in v]
    return v

ALLOWED = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.Compare, ast.Eq, ast.NotEq,
           ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.In, ast.NotIn, ast.Is, ast.IsNot, ast.Name, ast.Load,
           ast.Attribute, ast.Subscript, ast.Constant, ast.List, ast.Tuple, ast.Call)

def parse_cond(expr):
    try: tree = ast.parse(str(expr), mode="eval")
    except SyntaxError as e: raise ValueError("not a valid expression: %s" % e.msg)
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
def record(cmd, p):
    if not RECORD: return
    os.makedirs(RECORD, exist_ok=True)
    with open(os.path.join(RECORD, "%d-%s.json" % (time.time() * 1000, uuid.uuid4().hex[:6])), "w", encoding="utf-8") as f:
        json.dump({"argv": cmd[len(CLAUDE):], "returncode": p.returncode, "stdout": p.stdout, "stderr": p.stderr}, f, indent=1)

def parse_reply(stdout, returncode, stderr, schema):
    """Turn claude's raw JSON reply into (output, reason, session_id, cost). Pure, so fixtures can test it."""
    try: d = json.loads(stdout)
    except ValueError: return None, "claude gave no JSON (exit %s): %s" % (returncode, (stderr or stdout or "")[-400:]), None, None
    if not isinstance(d, dict): return None, "claude gave an unexpected reply", None, None
    sid, cost = d.get("session_id"), d.get("total_cost_usd")
    if d.get("is_error") or (d.get("subtype") or "success") != "success":
        return None, "claude error: %s" % (d.get("result") or d.get("subtype")), sid, cost
    out = d.get("structured_output")
    if out is None:
        txt = (d.get("result") or "").strip()
        if txt.startswith("```"): txt = txt.strip("`").split("\n", 1)[-1].strip()
        try: out = json.loads(txt)
        except ValueError: return None, "the answer was not JSON in the required format", sid, cost
    try: jsonschema.validate(out, schema)
    except jsonschema.ValidationError as e: return None, "the answer broke the required format: %s" % e.message, sid, cost
    return out, None, sid, cost

BARE_SYSTEM = "You are a careful judge. Read the instructions and the item, then answer only with JSON that matches the required format."
LAST_SECONDS = 0.0
LAST_TOKENS = {}                      # the last call's usage: in (fresh input), cached (read from cache), out

def usage_of(stdout):
    """Token counts from claude's JSON reply; {} when there are none."""
    try: u = json.loads(stdout).get("usage") or {}
    except (ValueError, AttributeError): return {}
    return {"in": (u.get("input_tokens") or 0) + (u.get("cache_creation_input_tokens") or 0),
            "cached": u.get("cache_read_input_tokens") or 0, "out": u.get("output_tokens") or 0}

def claude(msg, schema, session, resume, tools=(), mcp=None, model=None, cwd=None):
    """One headless turn. Returns (output_or_None, reason_or_None, session_id, cost)."""
    big = len(msg.encode()) > (20000 if os.name == "nt" else 100000)   # one argument caps near 128 KB on Linux, the whole command line at 32 K on Windows: a long prompt goes in on stdin instead
    cmd = CLAUDE + ["-p"] + ([] if big else [msg]) + ["--output-format", "json", "--json-schema", json.dumps(schema),
                    "--permission-mode", "dontAsk", "--strict-mcp-config", "--tools", ",".join(tools)]
    allowed = list(tools) + ["mcp__%s" % s for s in (mcp or {})]
    if allowed: cmd += ["--allowedTools", ",".join(allowed)]
    if mcp: cmd += ["--mcp-config", json.dumps({"mcpServers": mcp})]
    if model: cmd += ["--model", model]
    if not tools and not mcp:        # a pure judge: no settings, hooks, project memory or tool preamble, and a bare
        cmd += ["--setting-sources", "", "--system-prompt", BARE_SYSTEM]; cwd = tempfile.gettempdir()   # system prompt
    cmd += ["--resume", session] if resume else ["--session-id", session]
    global LAST_SECONDS, LAST_TOKENS; t0 = time.time(); LAST_TOKENS = {}
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=TIMEOUT, input=msg if big else None)
    except subprocess.TimeoutExpired:
        LAST_SECONDS = round(time.time() - t0, 2); return None, "the session timed out after %ss" % TIMEOUT, None, None
    except OSError as e:
        LAST_SECONDS = round(time.time() - t0, 2); return None, "could not start claude: %s" % e, None, None
    LAST_SECONDS = round(time.time() - t0, 2)   # wall time of this call, logged on the work/review event
    LAST_TOKENS = usage_of(p.stdout)
    record(cmd, p)
    return parse_reply(p.stdout, p.returncode, p.stderr, schema)

def session_lost(reason):
    r = (reason or "").lower()
    return "no conversation found" in r or "session not found" in r


# ---------- one step ----------
def run_check(b, out, st, cwd):
    if not b.get("check"): return None
    path = os.path.join(b["dir"], b["check"])
    if not os.path.exists(path): raise ConfigError("check script %s is missing" % b["check"])
    argv = [sys.executable, path] if path.endswith(".py") else [path]
    try:
        p = subprocess.run(argv, input=json.dumps({"out": out, "data": st["data"]}), capture_output=True,
                           text=True, encoding="utf-8", errors="replace", cwd=cwd, timeout=300)
    except subprocess.TimeoutExpired: return "check failed: the check timed out"
    except OSError as e: raise ConfigError("check script could not run: %s" % e)
    if p.returncode == 0: return None
    reason = "check failed: " + (p.stdout.strip() or p.stderr.strip()[-400:] or "no reason given")
    return FinalFail(reason) if p.returncode == 2 else reason   # exit 2: nothing a retry could fix

def run_record(b, out, st, cwd):
    """A block's record script runs after a step passes, before routing. It is how a workflow writes facts
    (a sent message, a decision) to its own store without relying on an agent to remember. A failure stops the item."""
    if not b.get("record"): return None
    path = os.path.join(b["dir"], b["record"])
    if not os.path.exists(path): raise ConfigError("record script %s is missing" % b["record"])
    argv = [sys.executable, path] if path.endswith(".py") else [path]
    try:
        p = subprocess.run(argv, input=json.dumps({"out": out, "data": st["data"], "item": st["id"],
                           "workflow": st["workflow"], "step": st["step"]}), capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=cwd, timeout=300)
    except (subprocess.TimeoutExpired, OSError) as e: return "record failed: %s" % e
    event(st, "record", ok=p.returncode == 0, output=p.stdout.strip()[-400:])
    return None if p.returncode == 0 else "record failed: " + (p.stdout.strip() or p.stderr.strip()[-400:] or "no reason given")

def review(b, out, st, cwd):
    if not b.get("review"): return None
    if b.get("review_when") and not evaluate(b["review_when"], out, st["data"]): return None   # not worth a reviewer
    msg = ("You are reviewing another agent's work. You did not do it. Judge it only against the rubric.\n\n"
           "## Rubric\n%s\n\n## The item\n%s\n\n## The answer to review\n%s\n\n"
           "Return pass (true or false) and notes. If it fails, the notes must say exactly what to fix."
           % (b["review"], json.dumps(st["data"], indent=1, ensure_ascii=False), json.dumps(out, indent=1, ensure_ascii=False)))
    sid = str(uuid.uuid4())    # always a fresh session: the reviewer never shares history with the worker
    verdict, err, _, cost = claude(msg, REVIEW_SCHEMA, sid, False, model=b.get("review_model"), cwd=cwd)
    event(st, "review", session=sid, passed=bool(verdict and verdict["pass"]), cost=cost, seconds=LAST_SECONDS, tokens=LAST_TOKENS, error=err)
    if err: return "the reviewer could not answer: " + err
    return None if verdict["pass"] else "reviewer: " + verdict["notes"]

def worker_msg(b, st, resuming):
    data = json.dumps(st["data"], indent=1, ensure_ascii=False)
    fb = st.get("feedback")
    if not resuming:
        m = "%s\n\n## The item\n%s\n" % (b["prompt_text"], data)
        if fb: m += "\n## Note\n%s\n" % fb
        return m + "\nAnswer only with the structured output."
    return ("%s\n\n## The item now\n%s\n\nAnswer again with the structured output."
            % (fb or "This item is back at your step.", data))

def stop(st, reason):
    st.update(status="stuck", reason=reason); event(st, "stuck", reason=reason); save(st)

def step_of(wf, st):
    step = wf["steps"].get(st["step"])
    if step is None: raise ConfigError("step %r is not in the workflow" % st["step"])
    b = block(step.get("block"))
    if step.get("model"): b["model"] = step["model"]     # a step may pin the model (the same block as haiku and as opus)
    cwd = os.path.join(item_dir(st["workflow"], st["id"]), "work")
    os.makedirs(cwd, exist_ok=True)
    try: os.chmod(cwd, 0o2770)
    except OSError: pass
    return step, b, cwd

def judge(wf, st, step, b, cwd, out, reason):
    """Shared tail of every step: check, review, then retry / stuck / gate / route."""
    if reason is None: reason = run_check(b, out, st, cwd)
    if reason is None: reason = review(b, out, st, cwd)
    if reason:
        st["attempt"] += 1; st["reason"] = reason
        st["feedback"] = "Your last answer failed. %s. Fix that." % reason
        if isinstance(reason, FinalFail): return stop(st, "stopped by a hard rule: " + reason)
        if b.get("once"): return stop(st, "a run-once step failed, so it is not retried on its own: " + reason)
        if st["attempt"] >= wf["max_attempts"]:
            fb = step.get("on_stuck")              # a fallback step takes over instead of stopping (e.g. a model fills what a script could not)
            if fb and fb in wf["steps"] and fb != st["step"]:
                event(st, "fallback", to=fb, reason=reason)
                st.update(step=fb, status="ready", attempt=0, feedback="The %s step gave up after %d tries: %s" % (st["step"], wf["max_attempts"], reason))
                save(st); return
            return stop(st, reason)
        st["status"] = "ready"; event(st, "retry", reason=reason); save(st); return
    st["data"][st["step"]] = out; st["feedback"] = None; st["reason"] = None
    rec = run_record(b, out, st, cwd)
    if rec: save(st); return stop(st, rec)   # the fact was not written down: never route on as if it were
    if step.get("gate"):
        st["status"] = "waiting"; event(st, "gate"); save(st); return
    route(wf, st, out)

def run_driver(b, st, cwd):
    """A script that does the step for zero tokens. Gets the item's data on stdin, answers JSON on stdout.
    Returns (out, reason, handled): exit 0 = answer; exit 3 = can't handle this one, fall back to external;
    anything else = a failure counted like a worker's."""
    path = os.path.join(b["dir"], b["driver"])
    if not os.path.exists(path): raise ConfigError("driver script %s is missing" % b["driver"])
    argv = [sys.executable, path] if path.endswith(".py") else [path]
    global LAST_SECONDS; t0 = time.time()
    try:
        p = subprocess.run(argv, input=json.dumps({"data": st["data"], "item": st["id"], "workflow": st["workflow"], "step": st["step"],
                                                   "feedback": st.get("feedback")}), capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=cwd, timeout=TIMEOUT)
    except subprocess.TimeoutExpired: LAST_SECONDS = round(time.time() - t0, 2); return None, "the driver timed out after %ss" % TIMEOUT, True
    except OSError as e: raise ConfigError("driver script could not run: %s" % e)
    LAST_SECONDS = round(time.time() - t0, 2)
    if p.returncode == 3: return None, (p.stdout.strip() or p.stderr.strip()[-400:] or "driver declined"), False
    if p.returncode != 0: return None, "driver failed (exit %d): %s" % (p.returncode, (p.stderr.strip() or p.stdout.strip())[-400:]), True
    try: out = json.loads(p.stdout)
    except ValueError: return None, "driver gave no JSON: %s" % p.stdout[-300:], True
    try: jsonschema.validate(out, b["output"])
    except jsonschema.ValidationError as e: return None, "the driver's answer broke the required format: %s" % e.message, True
    return out, None, True

def run_step(wf, st):
    step, b, cwd = step_of(wf, st)
    if b.get("driver"):   # a script does it; if it declines, the step falls through to its external owner
        st["status"] = "running"; save(st)
        out, reason, handled = run_driver(b, st, cwd)
        if handled:
            event(st, "driver", ok=reason is None, seconds=LAST_SECONDS, reason=reason)
            return judge(wf, st, step, b, cwd, out, reason)
        event(st, "driver_declined", reason=reason)
        if not b["external"]: return judge(wf, st, step, b, cwd, None, "driver declined and the block has no external owner: " + reason)
    if b["external"]:   # someone outside the engine does this step; flow done / flow fail bring the result
        st["status"] = "external"; event(st, "external", who=b["external"], feedback=st.get("feedback")); save(st); return
    st["status"] = "running"; save(st)
    sid = st["sessions"].get(st["step"])
    resuming = sid is not None
    run = lambda s, r: claude(worker_msg(b, st, r), b["output"], s, r, b["tools"], b.get("mcp"), b.get("model"), cwd)
    out, reason, got, cost = run(sid or str(uuid.uuid4()), resuming)
    if resuming and session_lost(reason):   # the session file is gone: start this step over in a fresh one
        event(st, "session_lost", session=sid); del st["sessions"][st["step"]]; resuming = False
        out, reason, got, cost = run(str(uuid.uuid4()), False)
    if got: st["sessions"][st["step"]] = got   # a lost one is caught by session_lost on the next resume
    event(st, "work", session=got, resumed=resuming, attempt=st["attempt"], ok=reason is None, cost=cost, seconds=LAST_SECONDS, tokens=LAST_TOKENS, model=b.get("model"), reason=reason)
    judge(wf, st, step, b, cwd, out, reason)

def external_result(wf, st, out):
    step, b, cwd = step_of(wf, st)
    try: jsonschema.validate(out, b["output"]); reason = None
    except jsonschema.ValidationError as e: reason = "the result broke the required format: %s" % e.message
    event(st, "done", who=b["external"], ok=reason is None, reason=reason)
    judge(wf, st, step, b, cwd, out, reason)

def route(wf, st, out):
    st["steps_taken"] = st.get("steps_taken", 0) + 1
    if st["steps_taken"] >= wf["max_steps"]:
        return stop(st, "reached the %d step limit; check the workflow for a loop" % wf["max_steps"])
    for r in wf["steps"][st["step"]].get("routes", [{"to": "end"}]):
        try:
            if "when" in r and not evaluate(r["when"], out, st["data"]): continue
            if "each" in r:   # fan out: one new item per element, then this item is done
                elems = evaluate(r["each"], out, st["data"])
                if elems is None or isinstance(elems, Missing): elems = []
                if not isinstance(elems, list): return stop(st, "each: %s is not a list" % r["each"])
                spawned = []
                for i, el in enumerate(elems):
                    el = json.loads(json.dumps(el))
                    key = str(el.get("key")) if isinstance(el, dict) and el.get("key") not in (None, "") else "%s-%d" % (st["id"], i)
                    child = {"input": el, "batch": st["data"].get("input")}   # the batch's own input (flavor etc.) rides along
                    if new_item(st["workflow"], el, key, r["to"], data=child): spawned.append(safe_id(key))
                event(st, "fan_out", to=r["to"], items=spawned)
                return finish(st, "done")
        except Exception as e:
            return stop(st, "route condition failed: %s" % e)
        if isinstance(r.get("to"), list):   # fork: one child per branch, this item is done
            kids = []
            for branch in r["to"]:
                if branch not in wf["steps"]: return stop(st, "routes to unknown step %r" % branch)
                kid = new_item(st["workflow"], None, "%s.%s" % (st["id"], branch), branch, data=st["data"])
                if kid: kids.append(kid["id"])
            event(st, "fork", to=r["to"], items=kids)
            return finish(st, "done")
        if r.get("to") == "end": return finish(st, "done")
        if r.get("to") not in wf["steps"]: return stop(st, "routes to unknown step %r" % r.get("to"))
        event(st, "route", to=r["to"])
        st.update(step=r["to"], status="ready", attempt=0, feedback=None, reason=None); save(st); return
    stop(st, "no route matched")

def finish(st, status):
    st["status"] = status; event(st, status); save(st)


# ---------- items ----------
def safe_id(k): return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(k))[:80].lstrip(".") or "item"

def new_item(wfname, inp, key=None, step=None, data=None):
    wf = workflow(wfname)
    iid = safe_id(key) if key else time.strftime("%Y%m%d%H%M%S-") + uuid.uuid4().hex[:6]
    d = item_dir(wfname, iid)
    if os.path.exists(os.path.join(d, "state.json")): return None   # dedupe: the same key never runs twice
    os.makedirs(d, exist_ok=True)
    st = {"id": iid, "workflow": wfname, "step": step or wf["start"], "status": "ready", "attempt": 0,
          "steps_taken": 0, "feedback": None, "reason": None, "data": json.loads(json.dumps(data)) if data else {"input": inp},
          "sessions": {}, "created": now(), "seq": time.time()}
    save(st); event(st, "added"); return st

def recover(wf, st):
    """A 'running' item found by a tick that holds the lock was cut off mid-step (crash, reboot)."""
    try: once = block(wf["steps"][st["step"]]["block"]).get("once")
    except Exception: once = True   # unknown: be safe
    if once:   # a step with real-world effects may have happened: never rerun it blind
        st.update(status="stuck", reason="cut off mid-step; check whether it already happened, then jump")
    else:
        st["status"] = "ready"
    event(st, "recovered", status=st["status"]); save(st)

def tick(wfname=None):
    os.makedirs(RUNS, exist_ok=True)
    # A named tick touches only its own workflow's items: it shares the global lock and holds its workflow's lock, so
    # `tick find` and `tick apply` run side by side. An unnamed tick touches everything and holds the global lock alone.
    lock = open(os.path.join(RUNS, ".lock"), "w")
    try: _lock(lock, shared=bool(wfname), slot=sum(map(ord, wfname or "")))
    except BlockingIOError: return {"skipped": "another tick is running"}
    if wfname:
        wlock = open(os.path.join(RUNS, ".lock-%s" % wfname), "w")
        try: _lock(wlock)
        except BlockingIOError: return {"skipped": "another %s tick is running" % wfname}
    wfs, moved, bad = {}, 0, []
    def wf_of(st):
        if st["workflow"] not in wfs: wfs[st["workflow"]] = workflow(st["workflow"])
        return wfs[st["workflow"]]
    for st in all_items(wfname, bad):
        if st["status"] == "running":
            try: recover(wf_of(st), st)
            except ConfigError as e: stop(st, str(e))
    def lane_of(st):
        try: return block(wf_of(st)["steps"][st["step"]]["block"]).get("lane")
        except Exception: return None
    def work(st):
        try: run_step(wf_of(st), st)
        except ConfigError as e: stop(st, "config: %s" % e)
        except Exception as e: stop(st, "runner error: %s: %s" % (type(e).__name__, e))
    for _ in range(10000):   # keep going until nothing is ready: a chain runs end to end in one tick
        ready = sorted((s for s in all_items(wfname) if s["status"] == "ready"), key=lambda s: (s.get("seq", 0), s["id"]))
        if not ready: break
        batch, lanes = [], set()   # one item per lane, up to PARALLEL items in all; oldest first
        for st in ready:
            lane = lane_of(st)
            if lane and lane in lanes: continue
            if lane: lanes.add(lane)
            batch.append(st)
            if len(batch) >= PARALLEL: break
        moved += len(batch)
        threads = [threading.Thread(target=work, args=(st,)) for st in batch]
        for t in threads: t.start()
        for t in threads: t.join()
    res = {"steps_run": moved}
    if bad: res["unreadable"] = bad
    return res


# ---------- validate ----------
def validate(name):
    try: wf = workflow(name)
    except Exception as e: return ["%s: %s" % (name, e)]
    errs, steps = [], wf["steps"]
    for s, cfg in steps.items():
        cfg = cfg or {}
        try:
            b = block(cfg.get("block"))
            for k in ("check", "record", "driver"):
                if b.get(k) and not os.path.exists(os.path.join(b["dir"], b[k])): errs.append("%s: %s script %s is missing" % (s, k, b[k]))
        except Exception as e: errs.append("%s: %s" % (s, e))
        routes = cfg.get("routes", [])
        if not isinstance(routes, list): errs.append("%s: routes must be a list" % s); continue
        for r in routes:
            targets = r.get("to") if isinstance(r.get("to"), list) else [r.get("to")]
            for t in targets:
                if t != "end" and t not in steps: errs.append("%s: routes to unknown step %r" % (s, t))
            for k in ("when", "each"):
                if k in r:
                    try: parse_cond(r[k])
                    except Exception as e: errs.append("%s: bad %s %r: %s" % (s, k, r[k], e))
        if routes and "when" in routes[-1] and "each" not in routes[-1]:
            errs.append("%s: last route has a condition; add a default route so nothing gets stuck" % s)
    return errs


# ---------- report ----------
def report(wfname=None, since=None):
    """Cost, time and steps per workflow and per step, from the events log. `since` is an ISO date prefix."""
    from datetime import datetime
    p = os.path.join(RUNS, "events.jsonl")
    if not os.path.exists(p): return {}
    def ts(e): return datetime.strptime(e["ts"][:19], "%Y-%m-%dT%H:%M:%S")
    rep = {}
    with open(p, encoding="utf-8") as f:
        for l in f:
            try: e = json.loads(l)
            except ValueError: continue
            if wfname and e.get("workflow") != wfname: continue
            if since and e["ts"] < since: continue
            w = rep.setdefault(e["workflow"], {"items": set(), "done": set(), "stuck": set(), "claude_calls": 0, "cost_usd": 0.0,
                                               "claude_seconds": 0.0, "tokens": {"in": 0, "cached": 0, "out": 0}, "retries": 0, "first": e["ts"], "last": e["ts"], "steps": {}})
            w["items"].add(e["item"]); w["last"] = max(w["last"], e["ts"]); w["first"] = min(w["first"], e["ts"])
            st = w["steps"].setdefault(e.get("step") or "?", {"work": 0, "external": 0, "review": 0, "retries": 0, "cost_usd": 0.0, "claude_seconds": 0.0, "tokens": {"in": 0, "cached": 0, "out": 0}, "seconds": 0.0, "_open": {}})
            ev = e["event"]
            if ev == "driver": st["driver"] = st.get("driver", 0) + 1; st["claude_seconds"] += 0
            if ev in ("work", "review"):
                w["claude_calls"] += 1; w["cost_usd"] += e.get("cost") or 0; st["cost_usd"] += e.get("cost") or 0; st[ev] += 1
                w["claude_seconds"] += e.get("seconds") or 0; st["claude_seconds"] += e.get("seconds") or 0
                for k, v in (e.get("tokens") or {}).items(): w["tokens"][k] += v; st["tokens"][k] += v
            if ev == "external": st["external"] += 1; st["_open"][e["item"]] = ts(e)
            if ev == "done" and e["item"] in st["_open"]: st["seconds"] += (ts(e) - st["_open"].pop(e["item"])).total_seconds()
            if ev == "retry": w["retries"] += 1; st["retries"] += 1
            if ev == "done" and e.get("step") is None or ev == "done" and e.get("who") is None: w["done"].add(e["item"])
            if ev == "stuck": w["stuck"].add(e["item"])
    for w in rep.values():
        w["items"] = len(w["items"]); w["done"] = len(w["done"]); w["stuck"] = len(w["stuck"])
        w["cost_usd"] = round(w["cost_usd"], 4); w["claude_seconds"] = round(w["claude_seconds"], 1)
        w["wall_minutes"] = round((datetime.strptime(w["last"][:19], "%Y-%m-%dT%H:%M:%S") - datetime.strptime(w["first"][:19], "%Y-%m-%dT%H:%M:%S")).total_seconds() / 60, 1)
        for st in w["steps"].values():
            st.pop("_open"); st["cost_usd"] = round(st["cost_usd"], 4); st["claude_seconds"] = round(st["claude_seconds"], 1)
            st["external_wait_minutes"] = round(st.pop("seconds") / 60, 1)
    return rep


# ---------- scaffolding ----------
BLOCK_YAML = """# %(name)s: <one line: what this step does and what it must never do>
%(external)soutput:                         # JSON Schema the answer must match; the engine rejects anything else
  type: object
  required: [result]
  properties:
    result: {type: string}
%(tools)s%(check)s%(record)s"""
PROMPT_MD = "Instructions for this step. Say what to do, what to report, and what to stop and `flow fail` on.\n"
CHECK_PY = ('"""Hard rules on this step\'s answer. Exit 0 = pass. Exit 1 + a line on stdout = fail, the worker retries with\n'
            'that line. Exit 2 = fail for good, no retry (e.g. a do-not-contact rule)."""\n'
            'import json, sys\n'
            'd = json.load(sys.stdin)        # {"out": the answer, "data": the item\'s data}\n')
RECORD_PY = ('"""Runs after the step passes, before it routes on. Write what the workflow must never forget to its own store.\n'
             'A non-zero exit stops the item: nothing moves on unrecorded."""\n'
             'import json, sys\n'
             'd = json.load(sys.stdin)        # {"out", "data", "item", "workflow", "step"}\n')
WORKFLOW_YAML = """# %(name)s: <one line: what goes in, what comes out>
start: %(first)s
steps:
%(steps)s"""

def new(kind, name, rest):
    safe = safe_id(name)
    if kind == "block":
        d = os.path.join(HOME, "blocks", safe)
        if os.path.exists(d): die("blocks/%s already exists" % safe)
        os.makedirs(d)
        ext = ""
        if "--external" in rest:
            i = rest.index("--external")
            if i + 1 >= len(rest): die("--external needs a value (who does it: chrome, human, ...)")
            ext = "external: %s                 # done outside the engine: the item waits for `flow done`\n" % rest[i + 1]
        tools = "" if ext else "tools: []                       # e.g. [Read, Bash, WebSearch]; nothing = judge from the item alone\n"
        files = {"block.yaml": BLOCK_YAML % {"name": safe, "external": ext, "tools": tools,
                                             "check": "check: check.py\n" if "--check" in rest else "",
                                             "record": "record: record.py\n" if "--record" in rest else ""},
                 "prompt.md": PROMPT_MD}
        if "--check" in rest: files["check.py"] = CHECK_PY
        if "--record" in rest: files["record.py"] = RECORD_PY
        for fn, body in files.items():
            with open(os.path.join(d, fn), "w", encoding="utf-8") as f: f.write(body)
        return {"created": sorted(os.path.join("blocks", safe, fn) for fn in files)}
    if kind == "workflow":
        if not rest: die("usage: flow new workflow NAME BLOCK [BLOCK...]")
        p = os.path.join(HOME, "workflows", safe + ".yaml")
        if os.path.exists(p): die("workflows/%s.yaml already exists" % safe)
        for b in rest:
            if not os.path.isdir(os.path.join(HOME, "blocks", b)): die("no block %r (flow new block %s)" % (b, b))
        steps = []
        for i, b in enumerate(rest):
            nxt = rest[i + 1] if i + 1 < len(rest) else "end"
            steps.append("  %s:\n    block: %s\n    routes: [{to: %s}]" % (b, b, nxt))
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f: f.write(WORKFLOW_YAML % {"name": safe, "first": rest[0], "steps": "\n".join(steps) + "\n"})
        return {"created": os.path.join("workflows", safe + ".yaml"), "problems": validate(safe)}
    die("usage: flow new block|workflow NAME ...")


# ---------- cli ----------
def main(a):
    if not a or a[0] in ("-h", "--help", "help"): print(__doc__); return 0
    cmd, rest = a[0], a[1:]
    try:
        if cmd == "add":
            opts = {}
            for o in ("--key", "--step"):
                if o in rest:
                    i = rest.index(o)
                    if i + 1 >= len(rest): die("%s needs a value" % o)
                    opts[o] = rest[i + 1]; rest = rest[:i] + rest[i + 2:]
            if not rest: die("usage: flow add WORKFLOW [JSON] [--key K] [--step S]")
            if len(rest) > 1 and rest[1].startswith("@"): inp = flavor(rest[0], rest[1][1:])   # named preset
            else:
                try: inp = json.loads(rest[1]) if len(rest) > 1 else {}
                except ValueError: die("input is not valid JSON")
            if "--step" in opts and opts["--step"] not in workflow(rest[0])["steps"]: die("no step %r in %s" % (opts["--step"], rest[0]))
            key = opts.get("--key"); st = new_item(rest[0], inp, key, opts.get("--step"))
            print(json.dumps({"added": st["id"]} if st else {"exists": safe_id(key)})); return 0
        if cmd == "tick": print(json.dumps(tick(rest[0] if rest else None))); return 0
        if cmd == "report":
            since = None
            if "--since" in rest:
                i = rest.index("--since"); since = rest[i + 1]; rest = rest[:i] + rest[i + 2:]
            print(json.dumps(report(rest[0] if rest else None, since), indent=1)); return 0
        if cmd == "status":
            for s in all_items(rest[0] if rest else None):
                print(json.dumps({k: s.get(k) for k in ("id", "workflow", "step", "status", "attempt", "reason")}))
            return 0
        if cmd == "show":
            if not rest: die("usage: flow show ITEM")
            print(json.dumps(load(rest[0]), indent=1)); return 0
        if cmd in ("approve", "reject", "jump"):
            if not rest: die("usage: flow %s ITEM ..." % cmd)
            st = load(rest[0]); wf = workflow(st["workflow"])
            if cmd in ("approve", "reject") and st["status"] != "waiting":
                die("%s is %s, not waiting at a gate" % (st["id"], st["status"]))
            if cmd == "approve":
                event(st, "approved"); route(wf, st, st["data"][st["step"]])
            elif cmd == "reject":
                note = " ".join(rest[1:]) or "rejected"
                st.update(status="ready", attempt=0, reason=None, feedback="Reza rejected your answer: %s. Redo it." % note)
                event(st, "rejected", note=note); save(st)
            else:
                if len(rest) < 2 or rest[1] not in wf["steps"]: die("usage: flow jump ITEM STEP [NOTE]; steps: %s" % list(wf["steps"]))
                if st["status"] == "running": die("%s is running right now" % st["id"])
                note = " ".join(rest[2:]) or None
                event(st, "jump", to=rest[1]); st.update(step=rest[1], status="ready", attempt=0, reason=None, feedback=note); save(st)
            print(json.dumps({k: st.get(k) for k in ("id", "step", "status")})); return 0
        if cmd in ("done", "fail"):
            if len(rest) < 2: die("usage: flow %s ITEM %s" % (cmd, "JSON" if cmd == "done" else "REASON"))
            st = load(rest[0]); wf = workflow(st["workflow"])
            if st["status"] != "external": die("%s is %s, not waiting on an external step" % (st["id"], st["status"]))
            if cmd == "done":
                try: out = json.loads(rest[1])
                except ValueError: die("result is not valid JSON")
                external_result(wf, st, out)
            else:
                step, b, cwd = step_of(wf, st)
                event(st, "failed", who=b["external"], reason=" ".join(rest[1:]))
                judge(wf, st, step, b, cwd, None, " ".join(rest[1:]))
            print(json.dumps({k: st.get(k) for k in ("id", "step", "status", "reason")})); return 0
        if cmd == "new":
            if len(rest) < 2: die("usage: flow new block NAME [--external WHO] [--check] [--record] | flow new workflow NAME BLOCK...")
            print(json.dumps(new(rest[0], rest[1], rest[2:]))); return 0
        if cmd == "validate":
            wd = os.path.join(HOME, "workflows")
            names = rest or ([f[:-5] for f in sorted(os.listdir(wd)) if f.endswith(".yaml")] if os.path.isdir(wd) else [])
            res = {n: validate(n) for n in names}
            print(json.dumps(res, indent=1)); return 1 if any(res.values()) else 0
    except ConfigError as e:
        die(str(e))
    die("unknown command %s; see flow --help" % cmd)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
