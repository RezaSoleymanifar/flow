#!/usr/bin/env python3
"""flow: a tiny local runner for workflows of headless Claude steps.

A BLOCK is a reusable step: blocks/<name>/block.yaml (+ prompt.md, + optional check script).
A WORKFLOW wires blocks into a graph: workflows/<name>.yaml. An ITEM is one unit of work moving through it.
Every item-step pair keeps its own Claude session (its own conversation history); the item's state file holds the
pointers. Retry, loop back and jump are all the same move: point the item at a step, resume that step's session.

  flow add WORKFLOW [JSON|@FLAVOR] [--key K] [--step S]   new item (at the start step, or S); JSON, or a preset from
                                       FLOW_HOME/flavors.yaml ({workflow: {flavor: input}}), becomes data.input; --key dedupes
  flow tick [WORKFLOW]                 run every ready item until it ends, waits at a gate or gets stuck
  flow run WORKFLOW [JSON]             add a run keyed by the minute, then tick that workflow (what a schedule calls)
  flow schedule                        write each workflow's `schedule:` (5-field cron) into the crontab, one line per
                                       workflow, in a block owned by this FLOW_HOME; other lines and homes are untouched
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

A block with `review_view: <script>` decides what its reviewer sees ({"out","data"} in, {"item","answer"} out) and
can skip the review when nothing in the answer needs one (empty output): review only the risky part of a batch.
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
FLOW_RECORD (folder: save every raw claude reply, to build test fixtures from real runs), FLOW_PARALLEL (items
run at once, default 4; a block with `lane: <name>` runs one item at a time in that lane, oldest first).

A route's `to` may be a list: the item forks, one child per branch, each with its own sessions.
`each:` children get the element as data.input and the batch's input as data.batch.
A step with `join: true` (no block) is the fan-in for the children of one fan-out or fork. Each child's result is
written into ONE merged item the moment it arrives: data.<join step> = {parts: [arrived children's data], items:
[their ids], missing: [siblings that got stuck]}. With `gate: true` the merge waits at the gate from the first
arrival and can be approved any time, with whatever has arrived; without a gate it moves on once no sibling can
still arrive. Nobody is too late: a child arriving after its merge was approved or moved on opens the next round
(ID.STEP-2, -3, ...), so you can stop whenever and continue from what is already done.
"""
import ast, fcntl, json, os, re, shlex, subprocess, sys, tempfile, threading, time, uuid

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
PARALLEL = int(os.environ.get("FLOW_PARALLEL", "0")) or 10**6   # items run at once; 0 = no cap. A block's `lane` is still one at a time
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

def flavor(wfname, name):
    """A named preset of item input, from FLOW_HOME/flavors.yaml: {workflow: {flavor: {...input}}}."""
    p = os.path.join(HOME, "flavors.yaml")
    if not os.path.exists(p): die("no flavors.yaml in %s" % HOME)
    with open(p) as f: fl = yaml.safe_load(f) or {}
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
    with open(p) as f: b["prompt_text"] = f.read()
    def _inc(m):                       # {{include path}}: splice a file in, path relative to the flows home
        q = os.path.join(HOME, m.group(1).strip())
        if not os.path.exists(q): raise ConfigError("block %r includes a missing file %s" % (name, m.group(1)))
        with open(q) as f: return f.read()
    b["prompt_text"] = re.sub(r"\{\{\s*include\s+([^}]+)\}\}", _inc, b["prompt_text"])
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
                with open(p) as f: out.append(json.load(f))
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
    with open(os.path.join(RECORD, "%d-%s.json" % (time.time() * 1000, uuid.uuid4().hex[:6])), "w") as f:
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
    cmd = CLAUDE + ["-p", msg, "--output-format", "json", "--json-schema", json.dumps(schema),
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
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=TIMEOUT)
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
                           text=True, cwd=cwd, timeout=300)
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
                           "workflow": st["workflow"], "step": st["step"]}), capture_output=True, text=True, cwd=cwd, timeout=300)
    except (subprocess.TimeoutExpired, OSError) as e: return "record failed: %s" % e
    event(st, "record", ok=p.returncode == 0, output=p.stdout.strip()[-400:])
    return None if p.returncode == 0 else "record failed: " + (p.stdout.strip() or p.stderr.strip()[-400:] or "no reason given")

def review_view(b, out, st, cwd):
    """A block's review_view script picks what the reviewer sees: {"out", "data"} in, {"item", "answer"} out.
    An empty answer ({} or null) means nothing here needs a review, and the review is skipped. Returns (view, reason)."""
    path = os.path.join(b["dir"], b["review_view"])
    if not os.path.exists(path): raise ConfigError("review_view script %s is missing" % b["review_view"])
    argv = [sys.executable, path] if path.endswith(".py") else [path]
    try:
        p = subprocess.run(argv, input=json.dumps({"out": out, "data": st["data"]}), capture_output=True, text=True, cwd=cwd, timeout=300)
    except (subprocess.TimeoutExpired, OSError) as e: return None, "review_view failed: %s" % e
    if p.returncode != 0: return None, "review_view failed: " + (p.stderr.strip()[-400:] or p.stdout.strip()[-400:] or "no reason given")
    try: return json.loads(p.stdout or "null") or None, None
    except ValueError: return None, "review_view gave no JSON: %s" % p.stdout[-300:]

def compact(obj): return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))   # prompts: no whitespace tokens

def review(b, out, st, cwd):
    if not b.get("review"): return None
    item, answer = st["data"], out
    if b.get("review_view"):   # the block narrows the review to what matters, or skips it when nothing does
        view, err = review_view(b, out, st, cwd)
        if err: return err
        if not view: event(st, "review_skipped"); return None
        item, answer = view.get("item", item), view.get("answer", answer)
    msg = ("You are reviewing another agent's work. You did not do it. Judge it only against the rubric.\n\n"
           "## Rubric\n%s\n\n## The item\n%s\n\n## The answer to review\n%s\n\n"
           "Return pass (true or false) and notes. If it fails, the notes must say exactly what to fix."
           % (b["review"], compact(item), compact(answer)))
    sid = str(uuid.uuid4())    # always a fresh session: the reviewer never shares history with the worker
    verdict, err, _, cost = claude(msg, REVIEW_SCHEMA, sid, False, model=b.get("review_model"), cwd=cwd)
    event(st, "review", session=sid, passed=bool(verdict and verdict["pass"]), cost=cost, seconds=LAST_SECONDS, tokens=LAST_TOKENS, error=err)
    if err: return "the reviewer could not answer: " + err
    return None if verdict["pass"] else "reviewer: " + verdict["notes"]

def worker_msg(b, st, resuming):
    data = compact(st["data"])
    fb = st.get("feedback")
    if not resuming:
        m = "%s\n\n## The item\n%s\n" % (b["prompt_text"], data)
        if fb: m += "\n## Note\n%s\n" % fb
        return m + "\nAnswer only with the structured output."
    return ("%s\n\n## The item now\n%s\n\nAnswer again with the structured output."
            % (fb or "This item is back at your step.", data))

def stop(st, reason):
    st.update(status="stuck", reason=reason); event(st, "stuck", reason=reason); save(st)
    nudge_join(st)   # a stuck sibling must not leave the rest of its group waiting at a join

def step_of(wf, st):
    step = wf["steps"].get(st["step"])
    if step is None: raise ConfigError("step %r is not in the workflow" % st["step"])
    b = block(step.get("block"))
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
        if st["attempt"] >= wf["max_attempts"]: return stop(st, reason)
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
                                                   "feedback": st.get("feedback")}), capture_output=True, text=True, cwd=cwd, timeout=TIMEOUT)
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
    step = wf["steps"].get(st["step"]) or {}
    if step.get("join") and st.get("group"):   # a child put here by jump arrives like any other
        return try_join(wf, st)
    if step.get("join"):   # the merged item of a fan-in: its answer is the merge itself
        out = st["data"].get(st["step"], {})
        if step.get("gate"):
            st["status"] = "waiting"; event(st, "gate"); save(st); return
        return route(wf, st, out)
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
    event(st, "work", session=got, resumed=resuming, attempt=st["attempt"], ok=reason is None, cost=cost, seconds=LAST_SECONDS, tokens=LAST_TOKENS, reason=reason)
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
                    el = json.loads(json.dumps(el, default=lambda o: None))   # a null inside an element reads as Missing; it stays null
                    key = str(el.get("key")) if isinstance(el, dict) and el.get("key") not in (None, "") else "%s-%d" % (st["id"], i)
                    child = {"input": el, "batch": st["data"].get("input")}   # the batch's own input (flavor etc.) rides along
                    grp = {"id": st["id"], "index": i}
                    if new_item(st["workflow"], el, key, r["to"], data=child, group=grp): spawned.append(safe_id(key))
                event(st, "fan_out", to=r["to"], items=spawned)
                return finish(st, "done")
        except Exception as e:
            return stop(st, "route condition failed: %s" % e)
        if isinstance(r.get("to"), list):   # fork: one child per branch, this item is done
            kids = []
            for branch in r["to"]:
                if branch not in wf["steps"]: return stop(st, "routes to unknown step %r" % branch)
                kid = new_item(st["workflow"], None, "%s.%s" % (st["id"], branch), branch, data=st["data"],
                               group={"id": st["id"], "index": r["to"].index(branch)})
                if kid: kids.append(kid["id"])
            event(st, "fork", to=r["to"], items=kids)
            return finish(st, "done")
        if r.get("to") == "end": return finish(st, "done")
        if r.get("to") not in wf["steps"]: return stop(st, "routes to unknown step %r" % r.get("to"))
        if (wf["steps"][r["to"]] or {}).get("join"):   # fan-in: park here, merge when the whole group has settled
            if not st.get("group"): return stop(st, "reached join step %r but was not made by a fan-out or fork" % r["to"])
            event(st, "route", to=r["to"])
            st.update(step=r["to"], attempt=0, feedback=None, reason=None)
            return try_join(wf, st)
        event(st, "route", to=r["to"])
        st.update(step=r["to"], status="ready", attempt=0, feedback=None, reason=None); save(st); return
    stop(st, "no route matched")

def finish(st, status):
    st["status"] = status; event(st, status); save(st)
    nudge_join(st)

JOIN_LOCK = threading.RLock()
_JL = threading.local()
UNSETTLED = ("ready", "running", "waiting", "external")   # a sibling in one of these can still arrive
OPEN = ("collecting", "waiting")                            # a merged item still taking arrivals

class join_lock:
    """Tick threads and a separate `flow approve` never touch a merge at the same time. Reentrant."""
    def __enter__(self):
        JOIN_LOCK.acquire(); n = getattr(_JL, "n", 0); _JL.n = n + 1
        if n == 0:
            os.makedirs(RUNS, exist_ok=True)
            _JL.f = open(os.path.join(RUNS, ".joinlock"), "w"); fcntl.flock(_JL.f, fcntl.LOCK_EX)
    def __exit__(self, *a):
        _JL.n -= 1
        if _JL.n == 0: fcntl.flock(_JL.f, fcntl.LOCK_UN); _JL.f.close()
        JOIN_LOCK.release()

def open_merge(wfname, gid, j):
    """The newest merged item of this group at join j, if it is still taking arrivals; and how many rounds exist."""
    ms = sorted((s for s in all_items(wfname) if s.get("join_of") == gid and s["step"] == j), key=lambda s: s.get("seq", 0))
    return (ms[-1] if ms and ms[-1]["status"] in OPEN else None), len(ms)

def settle(m):
    """Refresh who is missing; once no sibling can still arrive, an ungated merge moves on."""
    sibs = [s for s in all_items(m["workflow"]) if (s.get("group") or {}).get("id") == m["join_of"]]
    m["data"][m["step"]]["missing"] = [s["id"] for s in sibs if s["status"] == "stuck"]
    if m["status"] == "collecting" and not any(s["status"] in UNSETTLED for s in sibs):
        m["status"] = "ready"; event(m, "merge_complete")
    save(m)

def try_join(wf, st):
    """A child arrived at a join: publish its result into the open merged item at once, opening a new round
    when none is open (the last one was approved or already moved on). Nobody is ever too late."""
    with join_lock():
        gid, j = st["group"]["id"], st["step"]
        m, rounds = open_merge(st["workflow"], gid, j)
        if not m:
            mid = safe_id("%s.%s" % (gid, j)) + ("-%d" % (rounds + 1) if rounds else "")
            data = {"input": st["data"].get("batch", st["data"].get("input")), j: {"parts": [], "items": [], "missing": []}}
            m = new_item(st["workflow"], None, mid, j, data=data)
            if m is None: return stop(st, "merged item %s already exists but is not open" % mid)
            m["join_of"] = gid; m["status"] = "waiting" if (wf["steps"][j] or {}).get("gate") else "collecting"
        m["data"][j]["parts"].append(st["data"]); m["data"][j]["items"].append(st["id"])
        event(m, "merged", child=st["id"])
        event(st, "joined", into=m["id"]); st["status"] = "done"; save(st)
        settle(m)

def nudge_join(st):
    """An item of a group just stopped or ended: refresh its group's open merge (missing list, ready to move on)."""
    g = st.get("group")
    if not g: return
    try:
        with join_lock():
            for m in all_items(st["workflow"]):
                if m.get("join_of") == g["id"] and m["status"] in OPEN: settle(m)
    except Exception as e:
        event(st, "join_error", reason=str(e))


# ---------- items ----------
def safe_id(k): return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(k))[:80].lstrip(".") or "item"

def new_item(wfname, inp, key=None, step=None, data=None, group=None):
    wf = workflow(wfname)
    iid = safe_id(key) if key else time.strftime("%Y%m%d%H%M%S-") + uuid.uuid4().hex[:6]
    d = item_dir(wfname, iid)
    if os.path.exists(os.path.join(d, "state.json")): return None   # dedupe: the same key never runs twice
    os.makedirs(d, exist_ok=True)
    st = {"id": iid, "workflow": wfname, "step": step or wf["start"], "status": "ready", "attempt": 0,
          "steps_taken": 0, "feedback": None, "reason": None, "data": json.loads(json.dumps(data)) if data else {"input": inp},
          "sessions": {}, "created": now(), "seq": time.time()}
    if group: st["group"] = group   # which fan-out or fork made it, for a later join
    save(st); event(st, "added"); return st

def recover(wf, st):
    """A 'running' item found by a tick that holds the lock was cut off mid-step (crash, reboot)."""
    if (wf["steps"].get(st["step"]) or {}).get("join"): once = False   # a merge has no outside effect
    else:
        try: once = block(wf["steps"][st["step"]]["block"]).get("once")
        except Exception: once = True   # unknown: be safe
    if once:   # a step with real-world effects may have happened: never rerun it blind
        st.update(status="stuck", reason="cut off mid-step; check whether it already happened, then jump")
    else:
        st["status"] = "ready"
    event(st, "recovered", status=st["status"]); save(st)

def tick(wfname=None):
    os.makedirs(RUNS, exist_ok=True)
    lock = open(os.path.join(RUNS, ".lock"), "w")
    try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError: return {"skipped": "another tick is running"}
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


# ---------- schedule ----------
CRONTAB = shlex.split(os.environ.get("FLOW_CRONTAB") or "crontab")   # tests point this at a fake

def schedule_lines():
    """One crontab line per workflow that declares `schedule:`, carrying this home's FLOW_* settings."""
    out, wd = [], os.path.join(HOME, "workflows")
    env = [("FLOW_HOME", HOME)] + [(k, os.environ[k]) for k in ("FLOW_RUNS", "FLOW_CLAUDE", "FLOW_PARALLEL", "FLOW_TIMEOUT") if os.environ.get(k)]
    env = " ".join("%s=%s" % (k, shlex.quote(v)) for k, v in env)
    for f in sorted(os.listdir(wd)) if os.path.isdir(wd) else []:
        if not f.endswith(".yaml"): continue
        sch = workflow(f[:-5]).get("schedule")
        if sch: out.append("%s %s %s %s run %s >> %s 2>&1" % (sch, env, shlex.quote(sys.executable), shlex.quote(os.path.realpath(__file__)),
                                                             f[:-5], shlex.quote(os.path.join(RUNS, "cron.log"))))
    return out

def schedule_sync():
    begin, end = "# flow: begin " + HOME, "# flow: end " + HOME
    p = subprocess.run(CRONTAB + ["-l"], capture_output=True, text=True)
    keep, inside = [], False
    for l in (p.stdout if p.returncode == 0 else "").splitlines():
        if l == begin: inside = True; continue
        if l == end: inside = False; continue
        if not inside: keep.append(l)
    mine = schedule_lines()
    if mine: keep += [begin, "# managed by `flow schedule`: edit schedule: in %s/workflows, then rerun it" % HOME] + mine + [end]
    q = subprocess.run(CRONTAB + ["-"], input="\n".join(keep).strip("\n") + "\n", capture_output=True, text=True)
    if q.returncode: die("crontab refused the update: " + (q.stderr.strip() or "no reason given"))
    return mine


# ---------- validate ----------
def validate(name):
    try: wf = workflow(name)
    except Exception as e: return ["%s: %s" % (name, e)]
    errs, steps = [], wf["steps"]
    sch = wf.get("schedule")
    if sch is not None and (not isinstance(sch, str) or len(sch.split()) != 5 or "%" in sch):
        errs.append("schedule must be a 5-field cron expression like \"52 6 * * *\", got %r" % (sch,))
    for s, cfg in steps.items():
        cfg = cfg or {}
        if cfg.get("join"):
            if cfg.get("block"): errs.append("%s: a join step merges items and runs no block" % s)
        else:
          try:
            b = block(cfg.get("block"))
            for k in ("check", "record", "driver", "review_view"):
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
    with open(p) as f:
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
            with open(os.path.join(d, fn), "w") as f: f.write(body)
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
        with open(p, "w") as f: f.write(WORKFLOW_YAML % {"name": safe, "first": rest[0], "steps": "\n".join(steps) + "\n"})
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
        if cmd == "run":
            if not rest: die("usage: flow run WORKFLOW [JSON]")
            try: inp = json.loads(rest[1]) if len(rest) > 1 else {}
            except ValueError: die("input is not valid JSON")
            st = new_item(rest[0], inp, "%s-%s" % (rest[0], time.strftime("%Y%m%d-%H%M")))
            print(json.dumps({"added": st["id"] if st else None, "tick": tick(rest[0])})); return 0
        if cmd == "schedule":
            print(json.dumps({"scheduled": schedule_sync()}, indent=1)); return 0
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
            if cmd == "approve" and (wf["steps"].get(st["step"]) or {}).get("join"):
                with join_lock():   # approve exactly what has arrived; later arrivals open the next round
                    st = load("%s/%s" % (st["workflow"], st["id"]))
                    event(st, "approved", items=st["data"][st["step"]]["items"]); route(wf, st, st["data"][st["step"]])
            elif cmd == "approve":
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
