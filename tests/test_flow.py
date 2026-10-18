"""Behavior tests for flow, run against a fake claude. python3 -m unittest discover -s tests"""
import json, os, shutil, subprocess, sys, tempfile, textwrap, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
FLOW = os.path.join(HERE, "..", "flow.py")
FAKE = os.path.join(HERE, "fake_claude.py")

DRAFT = {"type": "object", "required": ["intent", "text"],
         "properties": {"intent": {"type": "string"}, "text": {"type": "string"}}}


class Base(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(); self.fake = tempfile.mkdtemp()
        open(os.path.join(self.fake, "script.json"), "w").write("[]")
        self.env = dict(os.environ, FLOW_HOME=self.home, FAKE_DIR=self.fake,
                        FLOW_CLAUDE="%s %s" % (sys.executable, FAKE))
        self.block("draft", {"output": DRAFT, "tools": ["Read"],
                             "check": "check.py", "review": "The text must be polite."},
                   check="import json,sys; d=json.load(sys.stdin)\n"
                         "if '-' in d['out']['text']: print('no dashes allowed'); sys.exit(1)\n")
        self.block("send", {"output": {"type": "object", "required": ["sent"],
                                       "properties": {"sent": {"type": "boolean"}}}, "once": True})
        self.block("archive", {"output": {"type": "object"}})
        self.workflow("reply", """
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
        """)

    def tearDown(self): shutil.rmtree(self.home); shutil.rmtree(self.fake)

    def block(self, name, cfg, check=None):
        d = os.path.join(self.home, "blocks", name); os.makedirs(d)
        open(os.path.join(d, "prompt.md"), "w").write("Do the %s step." % name)
        open(os.path.join(d, "block.yaml"), "w").write(json.dumps(cfg))
        if check: open(os.path.join(d, "check.py"), "w").write(check)

    def workflow(self, name, text):
        os.makedirs(os.path.join(self.home, "workflows"), exist_ok=True)
        open(os.path.join(self.home, "workflows", name + ".yaml"), "w").write(textwrap.dedent(text))

    def script(self, *answers): json.dump(list(answers), open(os.path.join(self.fake, "script.json"), "w"))

    def flow(self, *args, ok=True):
        p = subprocess.run([sys.executable, FLOW] + list(args), env=self.env, capture_output=True, text=True)
        if ok: self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return p.stdout

    def calls(self):
        p = os.path.join(self.fake, "calls.jsonl")
        return [json.loads(l) for l in open(p)] if os.path.exists(p) else []

    def state(self, iid): return json.loads(self.flow("show", iid))

    def add(self, inp=None, key="i1", wf="reply"):
        self.flow("add", wf, json.dumps(inp or {"name": "Sam"}), "--key", key); return key


GOOD = {"intent": "reply", "text": "Thanks Sam, happy to talk."}
PASS = {"pass": True, "notes": ""}


class Behaviors(Base):
    def test_happy_path_routes_by_condition_and_carries_data(self):
        self.add(); self.script(GOOD, PASS, {"sent": True}); self.flow("tick")
        st = self.state("i1")
        self.assertEqual((st["status"], st["step"]), ("done", "send"))
        self.assertEqual(st["data"]["draft"], GOOD)
        self.assertIn('"name": "Sam"', self.calls()[0]["msg"])          # the item's data reaches the worker

    def test_condition_sends_to_another_branch(self):
        self.add(); self.script({"intent": "decline", "text": "no thanks"}, PASS, {}); self.flow("tick")
        self.assertEqual(self.state("i1")["step"], "archive")

    def test_each_item_step_has_its_own_session_and_reviewer_is_fresh(self):
        self.add(); self.script(GOOD, PASS, {"sent": True}); self.flow("tick")
        c = self.calls(); st = self.state("i1")
        self.assertEqual(st["sessions"]["draft"], c[0]["session_id"])
        self.assertEqual(st["sessions"]["send"], c[2]["session_id"])
        self.assertNotIn(c[1]["session_id"], st["sessions"].values())   # reviewer is not a worker session
        self.assertIsNone(c[1]["resume"])

    def test_bad_format_retries_in_the_same_session_with_the_reason(self):
        self.add(); self.script({"intent": "reply"}, GOOD, PASS, {"sent": True}); self.flow("tick")
        c = self.calls()
        self.assertEqual(c[1]["resume"], c[0]["session_id"])            # same conversation, resumed
        self.assertIn("required format", c[1]["msg"])
        self.assertEqual(self.state("i1")["status"], "done")

    def test_check_failure_retries_with_check_reason(self):
        self.add(); self.script({"intent": "reply", "text": "hi - there"}, GOOD, PASS, {"sent": True}); self.flow("tick")
        self.assertIn("no dashes allowed", self.calls()[1]["msg"])
        self.assertEqual(self.state("i1")["status"], "done")

    def test_reviewer_failure_retries_with_its_notes(self):
        self.add(); self.script(GOOD, {"pass": False, "notes": "too blunt"}, GOOD, PASS, {"sent": True}); self.flow("tick")
        self.assertIn("too blunt", self.calls()[2]["msg"])
        self.assertEqual(self.state("i1")["status"], "done")

    def test_three_failures_make_it_stuck(self):
        self.add(); self.script({"x": 1}, {"x": 1}, {"x": 1}); self.flow("tick")
        st = self.state("i1"); self.assertEqual((st["status"], st["attempt"]), ("stuck", 3))
        self.assertEqual(len(self.calls()), 3)

    def test_gate_waits_then_approve_routes_on(self):
        self.workflow("reply", open(os.path.join(self.home, "workflows", "reply.yaml")).read()
                      .replace("block: draft\n", "block: draft\n    gate: true\n"))
        self.add(); self.script(GOOD, PASS); self.flow("tick")
        self.assertEqual(self.state("i1")["status"], "waiting")
        self.flow("approve", "i1"); self.script({"sent": True}); self.flow("tick")
        self.assertEqual(self.state("i1")["status"], "done")

    def test_reject_resumes_the_step_session_with_the_note(self):
        self.workflow("reply", open(os.path.join(self.home, "workflows", "reply.yaml")).read()
                      .replace("block: draft\n", "block: draft\n    gate: true\n"))
        self.add(); self.script(GOOD, PASS); self.flow("tick")
        self.flow("reject", "i1", "mention", "the", "Ford", "work"); self.script(GOOD, PASS); self.flow("tick")
        c = self.calls()
        self.assertEqual(c[2]["resume"], c[0]["session_id"]); self.assertIn("mention the Ford work", c[2]["msg"])

    def test_jump_back_resumes_that_steps_own_session(self):
        self.add(); self.script(GOOD, PASS, {"sent": True}); self.flow("tick")
        self.flow("jump", "i1", "draft", "they", "replied", "again"); self.script(GOOD, PASS, {"sent": True}); self.flow("tick")
        c = self.calls()
        self.assertEqual(c[3]["resume"], c[0]["session_id"]); self.assertIn("they replied again", c[3]["msg"])
        self.assertEqual(c[5]["resume"], c[2]["session_id"])            # send's own session resumed too

    def test_fan_out_spawns_one_item_per_element_and_dedupes(self):
        self.block("find", {"output": {"type": "object"}})
        self.workflow("follow", """
            start: find
            steps:
              find:
                block: find
                routes: [{each: out.replies, to: draft}]
              draft:
                block: archive
                routes: [{to: end}]
        """)
        found = {"replies": [{"key": "t1", "from": "A"}, {"key": "t2", "from": "B"}]}
        self.add(key="night1", wf="follow"); self.script(found, {}, {}); self.flow("tick")
        ids = {json.loads(l)["id"]: json.loads(l)["status"] for l in self.flow("status", "follow").splitlines()}
        self.assertEqual(ids, {"night1": "done", "t1": "done", "t2": "done"})
        n = len(self.calls())
        self.add(key="night2", wf="follow"); self.script(found); self.flow("tick")   # same replies found again
        ids = [json.loads(l)["id"] for l in self.flow("status", "follow").splitlines()]
        self.assertEqual(sorted(ids), ["night1", "night2", "t1", "t2"])            # no duplicate t1/t2
        self.assertEqual(len(self.calls()), n + 1)                                  # only night2's find ran

    def test_tools_are_limited_per_block(self):
        self.add(); self.script(GOOD, PASS, {"sent": True}); self.flow("tick")
        c = self.calls()
        self.assertEqual((c[0]["tools"], c[0]["allowed"]), ("Read", "Read"))
        self.assertEqual(c[1]["tools"], "")                              # reviewer gets no tools
        self.assertEqual(c[2]["tools"], "")

    def test_each_item_works_in_its_own_folder(self):
        self.add(key="a"); self.add(key="b"); self.script(GOOD, PASS, {"sent": True}, GOOD, PASS, {"sent": True})
        self.flow("tick")
        cwds = {c["cwd"] for c in self.calls()}
        self.assertEqual(len(cwds), 2)

    def test_crash_mid_step_reruns_normal_steps_but_never_once_steps(self):
        self.add(key="a"); self.add(key="b")
        for iid, step in (("a", "draft"), ("b", "send")):
            p = os.path.join(self.home, "runs", "reply", iid, "state.json")
            st = json.load(open(p)); st.update(status="running", step=step); json.dump(st, open(p, "w"))
        self.script(GOOD, PASS, {"sent": True}); self.flow("tick")
        self.assertEqual(self.state("a")["status"], "done")
        self.assertEqual(self.state("b")["status"], "stuck")

    def test_validate_rejects_unsafe_conditions_and_bad_routes(self):
        self.workflow("bad", """
            start: a
            steps:
              a:
                block: archive
                routes:
                  - when: __import__("os").system("x")
                    to: nowhere
        """)
        out = json.loads(self.flow("validate", "bad", ok=False))["bad"]
        self.assertTrue(any("only len()" in e or "not allowed" in e for e in out), out)
        self.assertTrue(any("unknown step" in e for e in out), out)
        self.assertEqual(json.loads(self.flow("validate", "reply"))["reply"], [])


if __name__ == "__main__":
    unittest.main()
