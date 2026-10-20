"""Shared test harness: a throwaway FLOW_HOME with blocks and workflows, and the realistic fake claude."""
import json, os, shutil, subprocess, sys, tempfile, textwrap, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FLOW = os.path.join(ROOT, "flow.py")
FAKE = os.path.join(HERE, "fake_claude.py")

DRAFT = {"type": "object", "required": ["intent", "text"],
         "properties": {"intent": {"type": "string"}, "text": {"type": "string"}}}
GOOD = {"intent": "reply", "text": "Thanks Sam, happy to talk."}
PASS = {"pass": True, "notes": ""}
SENT = {"sent": True}

REPLY = """
start: draft
steps:
  draft:
    block: draft
    routes:
      - when: out.intent == "decline"
        to: archive
      - to: send
  send:
    block: send
    routes: [{to: end}]
  archive:
    block: archive
    routes: [{to: end}]
"""


class Base(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(); self.fake = tempfile.mkdtemp()
        self.script()
        self.env = dict(os.environ, FLOW_HOME=self.home, FLOW_SYNC="0", FAKE_DIR=self.fake, FLOW_PARALLEL="1",   # scripted answers are ordered
                        FLOW_CLAUDE="%s %s" % (sys.executable, FAKE))
        self.env.pop("FLOW_RUNS", None); self.env.pop("FLOW_RECORD", None)
        self.block("draft", {"output": DRAFT, "tools": ["Read"], "check": "check.py",
                             "review": "The text must be polite."},
                   check="import json,sys\nd=json.load(sys.stdin)\n"
                         "if '-' in d['out']['text']: print('no dashes allowed'); sys.exit(1)\n")
        self.block("send", {"output": {"type": "object", "required": ["sent"],
                                       "properties": {"sent": {"type": "boolean"}}}, "once": True})
        self.block("archive", {"output": {"type": "object"}})
        self.workflow("reply", REPLY)

    def tearDown(self): shutil.rmtree(self.home); shutil.rmtree(self.fake)

    # --- setup helpers ---
    def block(self, name, cfg, check=None, prompt=True):
        d = os.path.join(self.home, "blocks", name); os.makedirs(d, exist_ok=True)
        if prompt:
            with open(os.path.join(d, "prompt.md"), "w") as f: f.write("Do the %s step." % name)
        with open(os.path.join(d, "block.yaml"), "w") as f: f.write(json.dumps(cfg))
        if check:
            with open(os.path.join(d, "check.py"), "w") as f: f.write(check)

    def workflow(self, name, text):
        os.makedirs(os.path.join(self.home, "workflows"), exist_ok=True)
        with open(os.path.join(self.home, "workflows", name + ".yaml"), "w") as f: f.write(textwrap.dedent(text))

    def gate_draft(self):
        self.workflow("reply", REPLY.replace("block: draft\n", "block: draft\n    gate: true\n"))

    def script(self, *answers):
        with open(os.path.join(self.fake, "script.json"), "w") as f: json.dump(list(answers), f)

    def left(self):
        with open(os.path.join(self.fake, "script.json")) as f: return json.load(f)

    # --- running ---
    def run_flow(self, *args, env=None):
        return subprocess.run([sys.executable, FLOW] + list(args), env=env or self.env, capture_output=True, text=True)

    def flow(self, *args, ok=True):
        p = self.run_flow(*args)
        if ok: self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return p.stdout

    def tick(self): return json.loads(self.flow("tick"))

    def add(self, inp=None, key="i1", wf="reply"):
        self.flow("add", wf, json.dumps(inp if inp is not None else {"name": "Sam"}), "--key", key); return key

    # --- looking ---
    def calls(self):
        p = os.path.join(self.fake, "calls.jsonl")
        if not os.path.exists(p): return []
        with open(p) as f: return [json.loads(l) for l in f]

    def state(self, iid): return json.loads(self.flow("show", iid))

    def statuses(self, wf=None):
        return {json.loads(l)["id"]: json.loads(l)["status"] for l in self.flow("status", *( [wf] if wf else [])).splitlines()}

    def events(self, kind=None):
        p = os.path.join(self.home, "runs", "events.jsonl")
        with open(p) as f: ev = [json.loads(l) for l in f]
        return [e for e in ev if kind is None or e["event"] == kind]

    def state_path(self, iid, wf="reply"): return os.path.join(self.home, "runs", wf, iid, "state.json")

    def edit_state(self, iid, wf="reply", **kw):
        p = self.state_path(iid, wf)
        with open(p) as f: st = json.load(f)
        st.update(kw)
        with open(p, "w") as f: json.dump(st, f)
