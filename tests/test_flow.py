import os
import subprocess
import unittest
"""Behavior and edge-case tests, run against the realistic fake claude. python3 -m unittest discover -s tests"""
import fcntl, json, os, re, sys, time, unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base import Base, GOOD, PASS, SENT, REPLY  # noqa: E402

UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


class Routing(Base):
    def test_happy_path_routes_by_condition_and_carries_data(self):
        self.add(); self.script(GOOD, PASS, SENT); self.tick()
        st = self.state("i1")
        self.assertEqual((st["status"], st["step"]), ("done", "send"))
        self.assertEqual(st["data"]["draft"], GOOD)
        self.assertIn('"name":"Sam"', self.calls()[0]["msg"])
        self.assertIn('"intent":"reply"', self.calls()[2]["msg"])        # the next step sees earlier answers

    def test_condition_sends_to_another_branch(self):
        self.add(); self.script({"intent": "decline", "text": "no thanks"}, PASS, {}); self.tick()
        self.assertEqual((self.state("i1")["step"], self.state("i1")["status"]), ("archive", "done"))

    def test_condition_on_a_missing_field_is_just_false(self):
        self.workflow("reply", """
            start: draft
            steps:
              draft:
                block: draft
                routes:
                  - when: out.nothing.deeper == "x"
                    to: archive
                  - to: end
              archive: {block: archive}
        """)
        self.add(); self.script(GOOD, PASS); self.tick()
        self.assertEqual((self.state("i1")["step"], self.state("i1")["status"]), ("draft", "done"))

    def test_condition_can_read_earlier_steps_through_data(self):
        self.workflow("reply", """
            start: draft
            steps:
              draft:
                block: draft
                routes: [{to: archive}]
              archive:
                block: archive
                routes:
                  - when: data.input.name == "Sam" and len(data.draft.text) > 5
                    to: send
                  - to: end
              send: {block: send, routes: [{to: end}]}
        """)
        self.add(); self.script(GOOD, PASS, {}, SENT); self.tick()
        self.assertEqual(self.state("i1")["step"], "send")

    def test_condition_that_errors_stops_the_item_not_the_runner(self):
        self.workflow("reply", """
            start: draft
            steps:
              draft:
                block: draft
                routes:
                  - when: out.text > 5
                    to: end
                  - to: end
        """)
        self.add(key="a"); self.add(key="b", wf="reply"); self.script(GOOD, PASS, GOOD, PASS); self.tick()
        for k in ("a", "b"):
            st = self.state(k); self.assertEqual(st["status"], "stuck"); self.assertIn("route condition failed", st["reason"])

    def test_no_matching_route_is_stuck(self):
        self.workflow("reply", """
            start: draft
            steps:
              draft:
                block: draft
                routes:
                  - when: out.intent == "never"
                    to: end
        """)
        self.add(); self.script(GOOD, PASS); self.tick()
        self.assertEqual(self.state("i1")["reason"], "no route matched")

    def test_a_step_without_routes_ends(self):
        self.workflow("reply", "start: draft\nsteps:\n  draft: {block: archive}\n")
        self.add(); self.script({}); self.tick()
        self.assertEqual(self.state("i1")["status"], "done")

    def test_loop_without_a_gate_is_stopped(self):
        self.workflow("reply", """
            start: a
            max_steps: 5
            steps:
              a: {block: archive, routes: [{to: a}]}
        """)
        self.add(); self.script(*([{}] * 20)); self.tick()
        st = self.state("i1"); self.assertEqual(st["status"], "stuck"); self.assertIn("5 step limit", st["reason"])
        self.assertEqual(len(self.calls()), 5)

    def test_unicode_passes_through(self):
        self.add({"name": "Zoë 李"}); self.script(GOOD, PASS, SENT); self.tick()
        self.assertIn("Zoë 李", self.calls()[0]["msg"])


class Sessions(Base):
    def test_each_item_step_has_its_own_session_and_reviewer_is_fresh(self):
        self.add(); self.script(GOOD, PASS, SENT); self.tick()
        c = self.calls(); st = self.state("i1")
        self.assertEqual(st["sessions"], {"draft": c[0]["session_id"], "send": c[2]["session_id"]})
        self.assertIsNone(c[1]["resume"]); self.assertNotIn(c[1]["session_id"], st["sessions"].values())

    def test_session_ids_are_uuids_and_never_reused_for_a_new_session(self):
        for k in "abc": self.add(key=k)
        self.script(*([GOOD, PASS, SENT] * 3)); self.tick()
        new = [c["session_id"] for c in self.calls() if c["session_id"]]
        self.assertEqual(len(new), len(set(new)))
        self.assertTrue(all(UUID.match(s) for s in new))

    def test_two_items_never_share_a_session_or_folder(self):
        self.add(key="a"); self.add(key="b"); self.script(GOOD, PASS, SENT, GOOD, PASS, SENT); self.tick()
        a, b = self.state("a"), self.state("b")
        self.assertFalse(set(a["sessions"].values()) & set(b["sessions"].values()))
        work = [c for c in self.calls() if c["tools"]]          # tool-less calls (the reviewer) run in a scratch dir
        self.assertEqual(len({c["cwd"] for c in work}), 2)
        self.assertTrue(all(c["system"] for c in self.calls() if not c["tools"]))

    def test_retry_resumes_the_same_conversation(self):
        self.add(); self.script({"intent": "reply"}, GOOD, PASS, SENT); self.tick()
        c = self.calls()
        self.assertEqual(c[1]["resume"], c[0]["session_id"])
        self.assertEqual(c[1]["history"], 1)                              # the worker saw its first turn

    def test_jump_back_resumes_that_steps_own_session(self):
        self.add(); self.script(GOOD, PASS, SENT); self.tick()
        self.flow("jump", "i1", "draft", "they", "replied", "again"); self.script(GOOD, PASS, SENT); self.tick()
        c = self.calls()
        self.assertEqual((c[3]["resume"], c[3]["history"]), (c[0]["session_id"], 1))
        self.assertIn("they replied again", c[3]["msg"])
        self.assertEqual(c[5]["resume"], c[2]["session_id"])

    def test_a_lost_session_starts_fresh_instead_of_failing(self):
        self.add(); self.script(GOOD, PASS, SENT); self.tick()
        sid = self.state("i1")["sessions"]["draft"]
        os.remove(os.path.join(self.fake, "sessions", sid + ".json"))    # e.g. the sandbox home was wiped
        self.flow("jump", "i1", "draft"); self.script(GOOD, PASS, SENT); self.tick()
        c = self.calls()
        self.assertEqual(c[3]["resume"], sid)                              # tried to resume, got "No conversation found"
        self.assertIsNone(c[4]["resume"]); self.assertIn("Do the draft step", c[4]["msg"])   # full prompt, fresh
        self.assertEqual(self.state("i1")["status"], "done")
        self.assertEqual(len(self.events("session_lost")), 1)

    def test_a_failed_start_does_not_poison_the_next_attempt(self):
        self.add(); self.script({"__error": "Not logged in · Please run /login"}, GOOD, PASS, SENT); self.tick()
        self.assertEqual(self.state("i1")["status"], "done")

    def test_resume_message_carries_feedback_and_current_data(self):
        self.add(); self.script({"intent": "reply", "text": "a - b"}, GOOD, PASS, SENT); self.tick()
        m = self.calls()[1]["msg"]
        self.assertIn("no dashes allowed", m); self.assertIn('"name":"Sam"', m)


class Failures(Base):
    def test_bad_format_retries_with_the_reason(self):
        self.add(); self.script({"intent": "reply"}, GOOD, PASS, SENT); self.tick()
        self.assertIn("required format", self.calls()[1]["msg"]); self.assertEqual(self.state("i1")["status"], "done")

    def test_check_failure_retries_with_its_reason(self):
        self.add(); self.script({"intent": "reply", "text": "hi - there"}, GOOD, PASS, SENT); self.tick()
        self.assertIn("no dashes allowed", self.calls()[1]["msg"])

    def test_review_view_narrows_what_the_reviewer_sees_or_skips_it(self):
        """A batch where only some answers are risky: the reviewer sees just those, and nothing when none are."""
        d = os.path.join(self.home, "blocks", "draft")
        with open(os.path.join(d, "block.yaml")) as f: cfg = json.load(f)
        cfg["review_view"] = "view.py"
        with open(os.path.join(d, "block.yaml"), "w") as f: json.dump(cfg, f)
        with open(os.path.join(d, "view.py"), "w") as f:
            f.write("import json,sys\nd=json.load(sys.stdin)\n"
                    "print(json.dumps({'item': {'risky': d['out']['text']}, 'answer': {'intent': d['out']['intent']}}"
                    " if d['out']['intent'] == 'decline' else {}))\n")
        self.script(GOOD, SENT); self.add(key="safe"); self.tick()
        self.assertEqual(self.state("safe")["status"], "done")
        self.assertEqual(len(self.calls()), 2)                              # worker, send: no reviewer at all
        self.assertEqual(len(self.events("review_skipped")), 1)
        self.script({"intent": "decline", "text": "No thanks."}, PASS, {}); self.add(key="risky"); self.tick()
        msg = self.calls()[3]["msg"]                                        # the reviewer's prompt
        self.assertIn('{"risky":"No thanks."}', msg); self.assertNotIn("Sam", msg)   # the view, not the whole item

    def test_reviewer_failure_retries_with_its_notes(self):
        self.add(); self.script(GOOD, {"pass": False, "notes": "too blunt"}, GOOD, PASS, SENT); self.tick()
        self.assertIn("too blunt", self.calls()[2]["msg"]); self.assertEqual(self.state("i1")["status"], "done")

    def test_reviewer_error_counts_as_a_failed_attempt(self):
        self.add(); self.script(GOOD, {"__error": "overloaded"}, GOOD, PASS, SENT); self.tick()
        self.assertIn("reviewer could not answer", self.calls()[2]["msg"])

    def test_three_failures_make_it_stuck_and_stop_calling(self):
        self.add(); self.script({"x": 1}, {"x": 1}, {"x": 1}, GOOD); self.tick()
        st = self.state("i1"); self.assertEqual((st["status"], st["attempt"]), ("stuck", 3))
        self.assertEqual(len(self.calls()), 3); self.assertEqual(len(self.left()), 1)

    def test_max_attempts_is_configurable(self):
        self.workflow("reply", "start: draft\nmax_attempts: 1\nsteps:\n  draft: {block: draft}\n")
        self.add(); self.script({"x": 1}); self.tick()
        self.assertEqual(self.state("i1")["status"], "stuck")

    def test_claude_crash_without_json_is_retried(self):
        self.add(); self.script({"__crash": "segfault", "code": 139}, GOOD, PASS, SENT); self.tick()
        self.assertIn("no JSON", self.calls()[1]["msg"]); self.assertEqual(self.state("i1")["status"], "done")

    def test_claude_error_subtype_is_retried(self):
        self.add(); self.script({"__subtype": "error_max_structured_output_retries"}, GOOD, PASS, SENT); self.tick()
        self.assertEqual(self.state("i1")["status"], "done")

    def test_timeout_is_retried(self):
        env = dict(self.env, FLOW_TIMEOUT="1")
        self.add(); self.script({"__sleep": 3}, GOOD, PASS, SENT)
        p = self.run_flow("tick", env=env); self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIn("timed out", self.calls()[1]["msg"] if len(self.calls()) > 1 else self.state("i1")["reason"])

    def test_plain_json_text_and_code_fences_are_accepted(self):
        self.add(); self.script({"__raw": "```json\n" + json.dumps(GOOD) + "\n```"}, PASS, SENT); self.tick()
        self.assertEqual(self.state("i1")["data"]["draft"], GOOD)

    def test_plain_prose_is_a_format_failure(self):
        self.add(); self.script({"__raw": "Sure! Here is a reply."}, GOOD, PASS, SENT); self.tick()
        self.assertIn("not JSON", self.calls()[1]["msg"])

    def test_check_exit_2_stops_the_item_without_retrying(self):
        self.block("draft", {"output": {"type": "object"}, "check": "check.py"},
                   check="print('on the do-not-contact list'); raise SystemExit(2)\n")
        self.add(); self.script({}, {}, {}); self.tick()
        st = self.state("i1"); self.assertEqual((st["status"], st["attempt"]), ("stuck", 1))
        self.assertIn("hard rule", st["reason"]); self.assertEqual(len(self.calls()), 1)

    def test_check_that_crashes_counts_as_failure_with_its_error(self):
        self.block("draft", {"output": {"type": "object"}, "check": "check.py"}, check="raise SystemExit('boom')\n")
        self.add(); self.script({}, {}, {}); self.tick()
        st = self.state("i1"); self.assertEqual(st["status"], "stuck"); self.assertIn("boom", st["reason"])

    def test_missing_check_script_stops_only_that_item(self):
        self.block("draft", {"output": {"type": "object"}, "check": "gone.py"})
        self.workflow("other", "start: a\nsteps:\n  a: {block: archive}\n")
        self.add(key="bad"); self.add(key="good", wf="other"); self.script({}, {}); self.tick()
        self.assertIn("missing", self.state("bad")["reason"]); self.assertEqual(self.state("good")["status"], "done")

    def test_missing_block_stops_only_that_item(self):
        self.workflow("broken", "start: a\nsteps:\n  a: {block: nope}\n")
        self.add(key="x", wf="broken"); self.add(key="y"); self.script(GOOD, PASS, SENT); self.tick()
        self.assertIn("no block", self.state("x")["reason"]); self.assertEqual(self.state("y")["status"], "done")


class Records(Base):
    def test_record_runs_after_pass_with_the_answer_and_item(self):
        self.block("draft", {"output": {"type": "object"}, "record": "record.py"},
                   check=None)
        with open(os.path.join(self.home, "blocks", "draft", "record.py"), "w") as f:
            f.write("import json,sys,os\nd=json.load(sys.stdin)\n"
                    "open(os.environ['REC'],'a').write(json.dumps([d['item'],d['step'],d['out']])+'\\n')\n")
        self.workflow("reply", "start: draft\nsteps:\n  draft: {block: draft, routes: [{to: end}]}\n")
        self.env["REC"] = os.path.join(self.fake, "rec.jsonl")
        self.add(); self.script({"x": 1}); self.tick()
        with open(self.env["REC"]) as f: rows = [json.loads(l) for l in f]
        self.assertEqual(rows, [["i1", "draft", {"x": 1}]]); self.assertEqual(self.state("i1")["status"], "done")

    def test_record_failure_stops_the_item_before_routing(self):
        self.block("draft", {"output": {"type": "object"}, "record": "record.py"})
        with open(os.path.join(self.home, "blocks", "draft", "record.py"), "w") as f: f.write("print('db locked'); raise SystemExit(1)\n")
        self.workflow("reply", "start: draft\nsteps:\n  draft: {block: draft, routes: [{to: send}]}\n  send: {block: send}\n")
        self.add(); self.script({}, {"sent": True}); self.tick()
        st = self.state("i1"); self.assertEqual((st["status"], st["step"]), ("stuck", "draft")); self.assertIn("db locked", st["reason"])
        self.assertEqual(len(self.calls()), 1)                          # send never ran

    def test_record_does_not_run_on_a_failed_answer(self):
        self.block("draft", {"output": {"type": "object", "required": ["k"]}, "record": "record.py"})
        with open(os.path.join(self.home, "blocks", "draft", "record.py"), "w") as f: f.write("raise SystemExit('should not run')\n")
        self.workflow("reply", "start: draft\nsteps:\n  draft: {block: draft}\n")
        self.add(); self.script({}, {"k": 1}); self.tick()
        self.assertEqual(self.state("i1")["status"], "stuck")           # record ran on the good answer and failed, as designed
        self.assertEqual(len(self.events("record")), 1)


class OnceSteps(Base):
    def test_a_once_step_that_fails_is_never_retried_on_its_own(self):
        self.add(); self.script(GOOD, PASS, {"sent": "maybe"}, SENT); self.tick()
        st = self.state("i1"); self.assertEqual((st["status"], st["step"]), ("stuck", "send"))
        self.assertIn("run-once", st["reason"]); self.assertEqual(len(self.calls()), 3)

    def test_crash_mid_step_reruns_normal_steps_but_never_once_steps(self):
        self.add(key="a"); self.add(key="b")
        self.edit_state("a", status="running", step="draft"); self.edit_state("b", status="running", step="send")
        self.script(GOOD, PASS, SENT); self.tick()
        self.assertEqual(self.state("a")["status"], "done"); self.assertEqual(self.state("b")["status"], "stuck")

    def test_a_human_can_rerun_a_stuck_once_step_by_jumping(self):
        self.add(); self.edit_state("i1", status="stuck", step="send")
        self.flow("jump", "i1", "send"); self.script(SENT); self.tick()
        self.assertEqual(self.state("i1")["status"], "done")


class Gates(Base):
    def test_gate_waits_then_approve_routes_on(self):
        self.gate_draft(); self.add(); self.script(GOOD, PASS); self.tick()
        self.assertEqual(self.state("i1")["status"], "waiting")
        self.tick(); self.assertEqual(len(self.calls()), 2)              # a waiting item is left alone
        self.flow("approve", "i1"); self.script(SENT); self.tick()
        self.assertEqual(self.state("i1")["status"], "done")

    def test_approve_follows_conditions_on_the_gated_answer(self):
        self.gate_draft(); self.add(); self.script({"intent": "decline", "text": "no"}, PASS); self.tick()
        self.flow("approve", "i1"); self.script({}); self.tick()
        self.assertEqual(self.state("i1")["step"], "archive")

    def test_reject_resumes_the_step_session_with_the_note(self):
        self.gate_draft(); self.add(); self.script(GOOD, PASS); self.tick()
        self.flow("reject", "i1", "mention", "the", "Ford", "work"); self.script(GOOD, PASS); self.tick()
        c = self.calls()
        self.assertEqual(c[2]["resume"], c[0]["session_id"]); self.assertIn("mention the Ford work", c[2]["msg"])
        self.assertEqual(self.state("i1")["status"], "waiting")

    def test_approve_or_reject_needs_a_waiting_item(self):
        self.add()
        for cmd in (["approve", "i1"], ["reject", "i1", "no"]):
            p = self.run_flow(*cmd); self.assertEqual(p.returncode, 2); self.assertIn("not waiting", p.stdout)


class FanOut(Base):
    def setUp(self):
        super().setUp()
        self.block("find", {"output": {"type": "object"}})
        self.workflow("follow", """
            start: find
            steps:
              find:
                block: find
                routes: [{each: out.replies, to: handle}]
              handle:
                block: archive
                routes: [{to: end}]
        """)

    def test_one_item_per_element_and_no_duplicates_on_rerun(self):
        found = {"replies": [{"key": "t1", "from": "A"}, {"key": "t2", "from": "B"}]}
        self.add(key="night1", wf="follow"); self.script(found, {}, {}); self.tick()
        self.assertEqual(self.statuses("follow"), {"night1": "done", "t1": "done", "t2": "done"})
        self.assertEqual(self.state("t1")["data"]["input"], {"key": "t1", "from": "A"})
        self.assertEqual(self.state("t1")["data"]["batch"], {"name": "Sam"})        # the batch's input rides along
        n = len(self.calls())
        self.add(key="night2", wf="follow"); self.script(found); self.tick()
        self.assertEqual(sorted(self.statuses("follow")), ["night1", "night2", "t1", "t2"])
        self.assertEqual(len(self.calls()), n + 1)

    def test_empty_list_finishes_without_children(self):
        self.add(key="n", wf="follow"); self.script({"replies": []}); self.tick()
        self.assertEqual(self.statuses("follow"), {"n": "done"})

    def test_missing_list_counts_as_empty(self):
        self.add(key="n", wf="follow"); self.script({}); self.tick()
        self.assertEqual(self.statuses("follow"), {"n": "done"})

    def test_not_a_list_stops_the_item(self):
        self.add(key="n", wf="follow"); self.script({"replies": "oops"}); self.tick()
        self.assertIn("not a list", self.state("n")["reason"])

    def test_elements_without_keys_get_stable_ids(self):
        self.add(key="n", wf="follow"); self.script({"replies": ["x", 3]}, {}, {}); self.tick()
        self.assertEqual(sorted(self.statuses("follow")), ["n", "n-0", "n-1"])

    def test_hostile_keys_stay_inside_the_runs_folder(self):
        self.add(key="n", wf="follow"); self.script({"replies": [{"key": "../../etc/x"}]}, {}); self.tick()
        ids = [i for i in self.statuses("follow") if i != "n"]
        self.assertEqual(len(ids), 1); self.assertNotIn("/", ids[0]); self.assertFalse(ids[0].startswith("."))


class Safety(Base):
    def test_tools_are_limited_per_block(self):
        self.add(); self.script(GOOD, PASS, SENT); self.tick()
        c = self.calls()
        self.assertEqual((c[0]["tools"], c[0]["allowed"], c[0]["permission"]), ("Read", "Read", "dontAsk"))
        self.assertEqual((c[1]["tools"], c[1]["allowed"]), ("", None))     # reviewer: no tools at all
        self.assertEqual((c[2]["tools"], c[2]["allowed"]), ("", None))

    def test_model_and_mcp_servers_are_passed_and_allowed(self):
        self.block("archive", {"output": {"type": "object"}, "model": "sonnet",
                               "mcp": {"mail": {"command": "mail-mcp"}}})
        self.workflow("reply", "start: a\nsteps:\n  a: {block: archive}\n")
        self.add(); self.script({}); self.tick()
        c = self.calls()[0]
        self.assertEqual(c["model"], "sonnet"); self.assertEqual(c["allowed"], "mcp__mail")
        self.assertEqual(json.loads(c["mcp"]), {"mcpServers": {"mail": {"command": "mail-mcp"}}})

    def test_the_answer_reaches_checks_only_as_data(self):
        self.block("draft", {"output": DRAFT_ANY, "check": "check.py"},
                   check="import json,sys\nd=json.load(sys.stdin)\nopen('seen.json','w').write(json.dumps(d))\n")
        self.workflow("reply", "start: draft\nsteps:\n  draft: {block: draft}\n")
        self.add(); self.script({"intent": "$(rm -rf /)", "text": "`x`"}); self.tick()
        self.assertEqual(self.state("i1")["status"], "done")

    def test_work_folders_are_private_to_the_group(self):
        self.add(); self.script(GOOD, PASS, SENT); self.tick()
        mode = os.stat(os.path.join(self.home, "runs", "reply", "i1", "work")).st_mode & 0o7777
        self.assertEqual(mode, 0o2770)


DRAFT_ANY = {"type": "object"}


class Runner(Base):
    def test_unreadable_state_is_reported_and_skipped(self):
        self.add(key="a"); self.add(key="b")
        with open(self.state_path("a"), "w") as f: f.write("{not json")
        self.script(GOOD, PASS, SENT); res = self.tick()
        self.assertEqual(res["unreadable"], ["reply/a"]); self.assertEqual(self.state("b")["status"], "done")

    def test_a_second_tick_while_one_runs_is_skipped(self):
        os.makedirs(os.path.join(self.home, "runs"), exist_ok=True)
        with open(os.path.join(self.home, "runs", ".lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.assertIn("skipped", self.tick())

    def test_jumping_a_running_item_is_refused(self):
        self.add(); self.edit_state("i1", status="running")
        p = self.run_flow("jump", "i1", "send"); self.assertEqual(p.returncode, 2)

    def test_stuck_item_can_be_jumped_back_to_ready(self):
        self.add(); self.script({"x": 1}, {"x": 1}, {"x": 1}); self.tick()
        self.flow("jump", "i1", "draft", "try", "again"); self.script(GOOD, PASS, SENT); self.tick()
        self.assertEqual(self.state("i1")["status"], "done")

    def test_events_record_every_transition(self):
        self.add(); self.script({"x": 1}, GOOD, PASS, SENT); self.tick()
        kinds = [e["event"] for e in self.events()]
        for k in ("added", "work", "retry", "review", "route", "done"): self.assertIn(k, kinds)

    def test_record_mode_saves_raw_replies(self):
        rec = os.path.join(self.fake, "rec"); env = dict(self.env, FLOW_RECORD=rec)
        self.add(); self.script(GOOD, PASS, SENT); self.run_flow("tick", env=env)
        files = os.listdir(rec); self.assertEqual(len(files), 3)
        with open(os.path.join(rec, sorted(files)[0])) as f: r = json.load(f)
        self.assertIn("stdout", r); self.assertIn("--session-id", r["argv"])


class Cli(Base):
    def err(self, *args):
        p = self.run_flow(*args); self.assertEqual(p.returncode, 2, p.stdout); return json.loads(p.stdout)["error"]

    def test_errors_are_json_and_exit_2(self):
        self.assertIn("no workflow", self.err("add", "nope", "{}"))
        self.assertIn("not valid JSON", self.err("add", "reply", "{bad"))
        self.assertIn("--key needs a value", self.err("add", "reply", "{}", "--key"))
        self.assertIn("no item", self.err("show", "ghost"))
        self.add(); self.assertIn("steps", self.err("jump", "i1", "nowhere"))
        self.assertIn("unknown command", self.err("dance"))

    def test_adding_the_same_key_twice_is_a_no_op(self):
        self.add(); out = json.loads(self.flow("add", "reply", "{}", "--key", "i1"))
        self.assertEqual(out, {"exists": "i1"})

    def test_add_can_start_an_item_at_a_later_step(self):
        self.flow("add", "reply", '{"name": "Sam"}', "--key", "s", "--step", "send")
        self.assertEqual(self.state("s")["step"], "send")
        self.script(SENT); self.tick(); self.assertEqual(self.state("s")["status"], "done")
        p = self.run_flow("add", "reply", "{}", "--step", "nope"); self.assertEqual(p.returncode, 2); self.assertIn("no step", p.stdout)

    def test_add_takes_a_named_flavor_from_flavors_yaml(self):
        with open(os.path.join(self.home, "flavors.yaml"), "w") as f:
            f.write("reply:\n  warm: {name: Sam, tone: warm}\n  cold: {name: Sam, tone: cold}\n")
        self.flow("add", "reply", "@warm", "--key", "w")
        self.assertEqual(self.state("w")["data"]["input"], {"name": "Sam", "tone": "warm", "flavor": "warm"})
        p = self.run_flow("add", "reply", "@nope"); self.assertEqual(p.returncode, 2); self.assertIn("no flavor", p.stdout)
        p = self.run_flow("add", "other", "@warm"); self.assertEqual(p.returncode, 2)

    def test_report_counts_items_calls_cost_retries_per_workflow_and_step(self):
        self.add(key="a"); self.add(key="b")
        self.script({"intent": "reply", "text": "no - dashes"}, GOOD, PASS, SENT, GOOD, PASS, SENT); self.tick(); self.tick()
        rep = json.loads(self.flow("report"))["reply"]
        self.assertEqual((rep["items"], rep["done"], rep["stuck"]), (2, 2, 0))
        self.assertEqual(rep["claude_calls"], 7); self.assertAlmostEqual(rep["cost_usd"], 0.07)
        self.assertGreater(rep["claude_seconds"], 0); self.assertIn("seconds", self.events("work")[0])
        self.assertEqual(rep["retries"], 1); self.assertEqual(rep["steps"]["draft"]["retries"], 1)
        self.assertEqual(rep["steps"]["draft"]["review"], 2); self.assertEqual(rep["steps"]["send"]["work"], 2)
        self.assertEqual(json.loads(self.flow("report", "other")), {})
        self.assertEqual(json.loads(self.flow("report", "--since", "2999-01-01")), {})

    def test_new_scaffolds_blocks_and_a_workflow_that_validate_and_run(self):
        self.flow("new", "block", "triage", "--check", "--record"); self.flow("new", "block", "ship", "--external", "chrome")
        out = json.loads(self.flow("new", "workflow", "handle", "triage", "ship"))
        self.assertEqual(out["problems"], [])
        for fn in ("block.yaml", "prompt.md", "check.py", "record.py"): self.assertTrue(os.path.exists(os.path.join(self.home, "blocks", "triage", fn)))
        p = self.run_flow("new", "block", "triage"); self.assertEqual(p.returncode, 2); self.assertIn("already exists", p.stdout)
        p = self.run_flow("new", "workflow", "x", "nope"); self.assertEqual(p.returncode, 2); self.assertIn("no block", p.stdout)
        self.add({"q": 1}, key="h1", wf="handle"); self.script({"result": "ok"}); self.tick()
        st = json.loads(self.flow("show", "h1"))
        self.assertEqual((st["step"], st["status"]), ("ship", "external"))   # triage ran, check+record stubs passed, waits on chrome

    def test_same_id_in_two_workflows_needs_the_workflow_prefix(self):
        self.workflow("other", "start: a\nsteps:\n  a: {block: archive}\n")
        self.add(key="x"); self.add(key="x", wf="other")
        p = self.run_flow("show", "x"); self.assertEqual(p.returncode, 2); self.assertIn("exists in", p.stdout)
        self.assertEqual(json.loads(self.flow("show", "other/x"))["workflow"], "other")
        self.assertEqual(json.loads(self.flow("show", "reply/x"))["workflow"], "reply")
        p = self.run_flow("show", "other/nope"); self.assertEqual(p.returncode, 2)

    def test_items_without_a_key_get_unique_ids(self):
        ids = {json.loads(self.flow("add", "reply", "{}"))["added"] for _ in range(5)}
        self.assertEqual(len(ids), 5)

    def test_status_filters_by_workflow(self):
        self.workflow("other", "start: a\nsteps:\n  a: {block: archive}\n")
        self.add(key="a"); self.add(key="b", wf="other")
        self.assertEqual(list(self.statuses("other")), ["b"])


class Validate(Base):
    def problems(self, text, name="w"):
        self.workflow(name, text); p = self.run_flow("validate", name)
        return json.loads(p.stdout)[name]

    def test_good_workflow_has_no_problems(self):
        self.assertEqual(json.loads(self.flow("validate", "reply"))["reply"], [])

    def test_catches_every_kind_of_mistake(self):
        self.block("badschema", {"output": {"type": "nonsense"}})
        out = self.problems("""
            start: a
            steps:
              a:
                block: archive
                routes:
                  - when: __import__("os").system("x")
                    to: nowhere
              b: {block: missing}
              c: {block: badschema}
              d:
                block: archive
                routes:
                  - when: out.x ==
                    to: end
        """)
        text = "\n".join(out)
        for needle in ("only len()", "unknown step 'nowhere'", "no block 'missing'", "not a valid schema",
                       "not a valid expression", "add a default route"):
            self.assertIn(needle, text)

    def test_bad_start_is_caught(self):
        self.assertTrue(self.problems("start: x\nsteps:\n  a: {block: archive}\n"))


class Parallel(Base):
    """Items run PARALLEL at once; a block's lane runs one item at a time, oldest first; routes can fork."""
    def slow_block(self, name, lane=None):
        cfg = {"output": {"type": "object"}}
        if lane: cfg["lane"] = lane
        self.block(name, cfg)

    def test_items_run_at_the_same_time_up_to_the_limit(self):
        self.slow_block("slow"); self.workflow("p", "start: a\nsteps:\n  a: {block: slow}\n")
        for k in "abcd": self.add(key=k, wf="p")
        self.script(*([{"__sleep": 2}] * 4))
        t0 = time.time(); self.run_flow("tick", env=dict(self.env, FLOW_PARALLEL="4")); dt = time.time() - t0
        self.assertLess(dt, 6, "4 two-second items took %.1fs: not parallel" % dt)
        self.assertEqual(set(self.statuses("p").values()), {"done"})

    def test_a_lane_runs_one_at_a_time_oldest_first(self):
        self.slow_block("click", lane="chrome"); self.workflow("p", "start: a\nsteps:\n  a: {block: click}\n")
        for k in ("x1", "x2", "x3"): self.add(key=k, wf="p"); time.sleep(0.01)
        self.script({"__sleep": 1}, {"__sleep": 1}, {"__sleep": 1})
        t0 = time.time(); self.run_flow("tick", env=dict(self.env, FLOW_PARALLEL="4")); dt = time.time() - t0
        self.assertGreater(dt, 2.5, "lane items overlapped")
        self.assertEqual([e["item"] for e in self.events("work")], ["x1", "x2", "x3"])   # oldest first

    def test_lane_items_do_not_block_other_lanes(self):
        self.slow_block("click", lane="chrome"); self.slow_block("think")
        self.workflow("p", "start: a\nsteps:\n  a: {block: click}\n")
        self.workflow("q", "start: a\nsteps:\n  a: {block: think}\n")
        self.add(key="c1", wf="p"); self.add(key="c2", wf="p"); self.add(key="t1", wf="q")
        self.script({"__sleep": 1}, {"__sleep": 1}, {"__sleep": 1})
        t0 = time.time(); self.run_flow("tick", env=dict(self.env, FLOW_PARALLEL="4")); dt = time.time() - t0
        self.assertLess(dt, 3.5)                                          # t1 ran alongside c1, c2 queued once

    def test_parallel_items_keep_separate_state_and_sessions(self):
        self.slow_block("plain"); self.workflow("p", "start: a\nsteps:\n  a: {block: plain, routes: [{to: b}]}\n  b: {block: plain}\n")
        for k in "abcd": self.add(key=k, wf="p")
        self.script(*([{"__sleep": 1}] * 8))
        self.run_flow("tick", env=dict(self.env, FLOW_PARALLEL="4"))
        sess = [tuple(sorted(self.state(k)["sessions"].values())) for k in "abcd"]
        self.assertEqual(len({x for s in sess for x in s}), 8)          # 4 items x 2 steps, all distinct sessions
        self.assertEqual(set(self.statuses("p").values()), {"done"})

    def test_fork_makes_one_child_per_branch_with_shared_data(self):
        self.workflow("f", """
            start: vet
            steps:
              vet:
                block: archive
                routes: [{to: [left, right]}]
              left: {block: archive}
              right: {block: archive}
        """)
        self.add(key="p", wf="f", inp={"name": "Sam"}); self.script({"v": 1}, {}, {}); self.tick()
        self.assertEqual(self.statuses("f"), {"p": "done", "p.left": "done", "p.right": "done"})
        kid = self.state("p.left")
        self.assertEqual(kid["data"]["input"], {"name": "Sam"}); self.assertEqual(kid["data"]["vet"], {"v": 1})
        self.assertEqual(kid["step"], "left"); self.assertEqual(len(self.events("fork")), 1)

    def test_fork_to_unknown_step_is_caught_by_validate_and_at_runtime(self):
        self.workflow("f", "start: a\nsteps:\n  a: {block: archive, routes: [{to: [b, nope]}]}\n  b: {block: archive}\n")
        self.assertTrue(any("nope" in e for e in json.loads(self.run_flow("validate", "f").stdout)["f"]))


class External(Base):
    """A block with `external: chrome` is done by someone outside the engine; the result comes back via `flow done`."""
    def setUp(self):
        super().setUp()
        self.block("connect", {"output": {"type": "object", "required": ["sent"],
                                          "properties": {"sent": {"type": "boolean"}, "note": {"type": "string"}}},
                               "external": "chrome", "once": True, "check": "check.py"},
                   check="import json,sys\nd=json.load(sys.stdin)\n"
                         "if not d['out']['sent']: print('nothing was sent'); sys.exit(1)\n")
        self.workflow("connect", """
            start: vet
            steps:
              vet:
                block: archive
                routes: [{to: connect}]
              connect:
                block: connect
                routes: [{to: end}]
        """)

    def test_external_step_waits_and_tick_leaves_it_alone(self):
        self.add(key="r1", wf="connect"); self.script({}); self.tick()
        st = self.state("r1"); self.assertEqual((st["status"], st["step"]), ("external", "connect"))
        self.tick(); self.assertEqual(len(self.calls()), 1)             # no Claude call for an external step
        self.assertEqual(self.events("external")[-1]["who"], "chrome")

    def test_done_validates_checks_and_routes(self):
        self.add(key="r1", wf="connect"); self.script({}); self.tick()
        p = self.run_flow("done", "r1", '{"nope": 1}')                  # wrong format: once step -> stuck
        self.assertEqual(json.loads(p.stdout)["status"], "stuck")
        self.flow("jump", "r1", "connect"); self.tick()
        self.flow("done", "r1", '{"sent": true, "note": "ok"}')
        st = self.state("r1"); self.assertEqual(st["status"], "done"); self.assertEqual(st["data"]["connect"]["sent"], True)

    def test_done_that_fails_the_check_on_a_once_step_is_stuck(self):
        self.add(key="r1", wf="connect"); self.script({}); self.tick()
        self.flow("done", "r1", '{"sent": false}')
        st = self.state("r1"); self.assertEqual(st["status"], "stuck"); self.assertIn("nothing was sent", st["reason"])

    def test_fail_reports_and_a_retryable_external_step_goes_back_to_ready(self):
        self.block("ask", {"output": {"type": "object"}, "external": "human"})
        self.workflow("ask", "start: a\nsteps:\n  a: {block: ask}\n")
        self.add(key="q", wf="ask"); self.tick()
        self.flow("fail", "q", "could", "not", "reach", "the", "site")
        st = self.state("q"); self.assertEqual((st["status"], st["attempt"]), ("ready", 1))
        self.tick(); self.assertEqual(self.state("q")["status"], "external")
        self.assertIn("could not reach the site", self.events("external")[-1]["feedback"])

    def test_done_needs_an_external_item_and_valid_json(self):
        self.add(key="r1", wf="connect")
        p = self.run_flow("done", "r1", "{}"); self.assertEqual(p.returncode, 2); self.assertIn("not waiting", p.stdout)
        self.script({}); self.tick()
        p = self.run_flow("done", "r1", "{bad"); self.assertEqual(p.returncode, 2); self.assertIn("not valid JSON", p.stdout)

    def test_batch_external_step_records_the_whole_list_then_fans_out(self):
        """Batching is the opposite of parallelism: one step handles many things in one go (one search, one
        inbox read) and answers with a list. The record script sees the whole list once; `each:` then makes one
        item per element for the steps that follow."""
        d = os.path.join(self.home, "blocks", "search"); os.makedirs(d)
        with open(os.path.join(d, "block.yaml"), "w") as f:
            json.dump({"output": {"type": "object", "required": ["found"], "properties": {"found": {"type": "array"}}},
                       "external": "chrome", "record": "record.py"}, f)
        with open(os.path.join(d, "prompt.md"), "w") as f: f.write("Search once, report everyone found.")
        with open(os.path.join(d, "record.py"), "w") as f:
            f.write("import json,sys,os\nd=json.load(sys.stdin)\n"
                    "open(os.path.join(os.environ['FAKE_DIR'],'recorded.json'),'w').write(json.dumps(d['out']['found']))\n")
        self.workflow("batch", """
            start: search
            steps:
              search:
                block: search
                routes: [{each: out.found, to: handle}]
              handle:
                block: archive
                routes: [{to: end}]
        """)
        self.add({"query": "ai recruiters"}, key="s1", wf="batch"); self.tick()
        self.assertEqual(self.state("s1")["status"], "external")
        found = [{"key": "ann", "name": "Ann"}, {"key": "bob", "name": "Bob"}]
        self.flow("done", "s1", json.dumps({"found": found}))
        with open(os.path.join(self.fake, "recorded.json")) as f: self.assertEqual(json.load(f), found)   # one record, whole list
        self.script({}, {}); self.tick()
        self.assertEqual(self.statuses("batch"), {"s1": "done", "ann": "done", "bob": "done"})
        self.assertEqual(self.state("bob")["data"]["input"], {"key": "bob", "name": "Bob"})

    def test_fan_out_elements_may_hold_nulls(self):
        """Real batches carry empty fields (no match, no value); they reach the child as null."""
        self.block("src", {"output": {"type": "object"}, "external": "human"})
        self.workflow("nulls", """
            start: src
            steps:
              src:
                block: src
                routes: [{each: out.found, to: handle}]
              handle:
                block: archive
                routes: [{to: end}]
        """)
        self.add({}, key="n1", wf="nulls"); self.tick()
        self.flow("done", "n1", json.dumps({"found": [{"key": "a", "rule": None, "deep": {"x": None}}]}))
        self.assertEqual(self.state("a")["data"]["input"], {"key": "a", "rule": None, "deep": {"x": None}})

    def test_external_step_can_be_gated_and_reviewed(self):
        self.block("draft-ext", {"output": {"type": "object"}, "external": "human", "review": "Must contain hello."})
        self.workflow("g", """
            start: a
            steps:
              a:
                block: draft-ext
                gate: true
                routes: [{to: end}]
        """)
        self.add(key="x", wf="g"); self.tick(); self.script({"pass": True, "notes": ""})
        self.flow("done", "x", '{"text": "hello"}')
        self.assertEqual(self.state("x")["status"], "waiting")
        self.assertEqual(self.calls()[0]["tools"], "")                   # the reviewer ran, no tools
        self.flow("approve", "x"); self.assertEqual(self.state("x")["status"], "done")



class SyncTest(unittest.TestCase):
    """GitHub is the one copy: a run pulls what others pushed, pushes local commits, refuses uncommitted edits."""
    def test_sync(self):
        import shutil, tempfile
        root = tempfile.mkdtemp(); remote, a, b = (os.path.join(root, x) for x in ("remote.git", "a", "b"))
        env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
        git = lambda d, *x: subprocess.run(["git", "-C", d] + list(x), env=env, check=True, capture_output=True, text=True).stdout.strip()
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", remote], check=True)
        subprocess.run(["git", "clone", "-q", remote, a], check=True)
        os.makedirs(os.path.join(a, "workflows")); open(os.path.join(a, "workflows", ".keep"), "w").close()
        shutil.copy(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "flow.py"), a)
        open(os.path.join(a, ".gitignore"), "w").write("runs/\n")
        git(a, "add", "-A"); git(a, "commit", "-qm", "init"); git(a, "push", "-q", "origin", "main")
        subprocess.run(["git", "clone", "-q", remote, b], check=True)
        tick = lambda d: subprocess.run([sys.executable, os.path.join(d, "flow.py"), "tick"], env=dict(env, FLOW_HOME=d, FLOW_SYNC="1"),
                                        capture_output=True, text=True)
        open(os.path.join(a, "note"), "w").write("1"); git(a, "add", "note"); git(a, "commit", "-qm", "n"); git(a, "push", "-q")
        self.assertEqual(tick(b).returncode, 0); self.assertEqual(git(b, "rev-parse", "HEAD"), git(a, "rev-parse", "HEAD"))   # pulled
        open(os.path.join(b, "note"), "w").write("2")
        r = tick(b); self.assertNotEqual(r.returncode, 0); self.assertIn("uncommitted", r.stdout + r.stderr)                 # refused
        git(b, "commit", "-qam", "m"); self.assertEqual(tick(b).returncode, 0)
        self.assertEqual(git(remote, "rev-parse", "main"), git(b, "rev-parse", "HEAD"))                                       # pushed
        shutil.rmtree(root)

if __name__ == "__main__":
    unittest.main()


class Drivers(Base):
    """A script does the step for zero tokens; the agent is only the fallback."""
    def driver(self, body, external="chrome", once=True, check=None):
        cfg = {"output": {"type": "object", "required": ["sent"], "properties": {"sent": {"type": "boolean"}}},
               "external": external, "driver": "drive.py", "once": once}
        if check: cfg["check"] = "check.py"
        self.block("ship", cfg, check=check)
        with open(os.path.join(self.home, "blocks", "ship", "drive.py"), "w") as f: f.write(body)
        self.workflow("ship", "start: a\nsteps:\n  a: {block: ship, routes: [{to: end}]}\n")

    def test_driver_answer_goes_through_checks_and_record_with_no_claude_call(self):
        self.driver("import json,sys\nd=json.load(sys.stdin)\nprint(json.dumps({'sent': True, 'who': d['data']['input']['name']}))")
        self.add(key="d1", wf="ship"); self.tick()
        st = self.state("d1"); self.assertEqual(st["status"], "done"); self.assertEqual(st["data"]["a"]["who"], "Sam")
        self.assertEqual(self.calls(), []); ev = self.events("driver")[0]; self.assertTrue(ev["ok"]); self.assertIn("seconds", ev)

    def test_driver_that_declines_falls_back_to_the_external_owner(self):
        self.driver("import sys\nprint('no Connect button on this page'); sys.exit(3)")
        self.add(key="d1", wf="ship"); self.tick()
        st = self.state("d1"); self.assertEqual(st["status"], "external")
        self.assertIn("no Connect button", self.events("driver_declined")[0]["reason"])
        self.flow("done", "d1", '{"sent": true}'); self.assertEqual(self.state("d1")["status"], "done")

    def test_driver_crash_or_bad_answer_counts_as_a_failed_attempt(self):
        self.driver("import sys\nsys.stderr.write('browser died'); sys.exit(1)", once=False)
        self.add(key="d1", wf="ship"); self.tick()                     # a tick retries a normal step until stuck
        st = self.state("d1"); self.assertEqual((st["status"], st["attempt"]), ("stuck", 3)); self.assertIn("browser died", st["reason"])
        self.assertEqual(len(self.events("driver")), 3); self.assertEqual(self.calls(), [])
        self.driver("print('not json')", once=False); self.add(key="d2", wf="ship"); self.tick()
        self.assertIn("no JSON", self.state("d2")["reason"])

    def test_driver_failure_on_a_once_step_is_stuck_not_retried(self):
        self.driver("import sys\nsys.exit(1)")
        self.add(key="d1", wf="ship"); self.tick(); self.tick()
        st = self.state("d1"); self.assertEqual(st["status"], "stuck"); self.assertEqual(len(self.events("driver")), 1)

    def test_driver_answer_is_checked_like_any_answer(self):
        self.driver("import json\nprint(json.dumps({'sent': False}))", once=False,
                    check="import json,sys\nd=json.load(sys.stdin)\nif not d['out']['sent']: print('nothing was sent'); sys.exit(1)\n")
        self.add(key="d1", wf="ship"); self.tick()
        self.assertIn("nothing was sent", self.state("d1")["reason"])

    def test_declining_driver_without_an_external_owner_fails_the_attempt(self):
        self.driver("import sys\nsys.exit(3)", external=None, once=False)
        self.add(key="d1", wf="ship"); self.tick()
        self.assertIn("no external owner", self.state("d1")["reason"])

    def test_validate_catches_a_missing_driver_script(self):
        self.driver("print('{}')"); os.remove(os.path.join(self.home, "blocks", "ship", "drive.py"))
        self.assertIn("driver script drive.py is missing", json.loads(self.run_flow("validate").stdout)["ship"][0])


class Join(Base):
    """Fan-in: each child's result is published into one merged item the moment it arrives; a gated merge can be
    approved any time with what has arrived; later arrivals open the next round, so nothing is ever too late."""
    FAN = """
        start: search
        steps:
          search:
            block: search
            routes: [{each: out.found, to: handle}]
          handle:
            block: handle
            HANDLE_GATE
            routes: [{to: merge}]
          merge:
            join: true
            MERGE_GATE
            routes: [{to: end}]
    """

    def setUp(self):
        super().setUp()
        self.block("search", {"output": {"type": "object", "required": ["found"], "properties": {"found": {"type": "array"}}},
                              "external": "chrome"})
        self.block("handle", {"output": {"type": "object", "required": ["n"], "properties": {"n": {"type": "integer"}}},
                              "check": "check.py"},
                   check="import json,sys\nd=json.load(sys.stdin)\n"
                         "if d['data']['input']['name'] == 'Bad': print('bad one'); sys.exit(1)\n")

    def fan(self, names, handle_gate=False, merge_gate=True):
        self.workflow("fan", self.FAN.replace("HANDLE_GATE", "gate: true" if handle_gate else "")
                                     .replace("MERGE_GATE", "gate: true" if merge_gate else ""))
        self.add({"q": "x"}, key="s1", wf="fan"); self.tick()
        self.flow("done", "s1", json.dumps({"found": [{"key": n.lower(), "name": n} for n in names]}))

    def merge(self, iid="s1.merge"): return self.state(iid)["data"]["merge"]

    def test_every_arrival_is_published_into_one_merge_approved_once(self):
        self.fan(["Ann", "Bob"]); self.script({"n": 1}, {"n": 2}); self.tick()
        self.assertEqual(self.statuses("fan"), {"s1": "done", "ann": "done", "bob": "done", "s1.merge": "waiting"})
        m = self.merge()
        self.assertEqual(m["items"], ["ann", "bob"]); self.assertEqual(m["missing"], [])
        self.assertEqual([p["handle"]["n"] for p in m["parts"]], [1, 2])
        self.assertEqual(self.state("s1.merge")["data"]["input"], {"q": "x"})   # the batch's own input rides along
        self.assertEqual(len(self.events("merged")), 2)                         # one write per arrival
        self.flow("approve", "s1.merge"); self.assertEqual(self.state("s1.merge")["status"], "done")

    def test_the_first_arrival_is_on_disk_before_its_siblings_finish(self):
        self.fan(["Ann", "Bob"], handle_gate=True); self.script({"n": 1}, {"n": 2}); self.tick()
        self.flow("approve", "ann")
        self.assertEqual(self.state("s1.merge")["status"], "waiting"); self.assertEqual(self.merge()["items"], ["ann"])
        self.flow("approve", "bob"); self.assertEqual(self.merge()["items"], ["ann", "bob"])

    def test_a_partial_merge_can_be_approved_and_a_later_arrival_opens_the_next_round(self):
        self.fan(["Ann", "Bob"], handle_gate=True); self.script({"n": 1}, {"n": 2}); self.tick()
        self.flow("approve", "ann"); self.flow("approve", "s1.merge")
        self.assertEqual(self.state("s1.merge")["status"], "done")
        self.flow("approve", "bob")
        self.assertEqual(self.state("s1.merge-2")["status"], "waiting"); self.assertEqual(self.merge("s1.merge-2")["items"], ["bob"])
        self.assertEqual(self.merge()["items"], ["ann"])                        # round one is untouched

    def test_a_stuck_sibling_never_holds_the_group_and_is_named_missing(self):
        self.fan(["Ann", "Bad"]); self.script({"n": 1}, {"n": 0}, {"n": 0}, {"n": 0}); self.tick()
        self.assertEqual(self.state("bad")["status"], "stuck")
        self.assertEqual(self.merge()["items"], ["ann"]); self.assertEqual(self.merge()["missing"], ["bad"])

    def test_a_fixed_stuck_child_joins_the_open_merge_if_not_yet_approved(self):
        self.fan(["Ann", "Bad"]); self.script({"n": 1}, {"n": 0}, {"n": 0}, {"n": 0}); self.tick()
        self.flow("jump", "bad", "merge"); self.tick()
        self.assertEqual(self.merge()["items"], ["ann", "bad"]); self.assertEqual(self.merge()["missing"], [])
        self.assertNotIn("s1.merge-2", self.statuses("fan"))

    def test_without_a_gate_the_merge_moves_on_once_no_sibling_can_still_arrive(self):
        self.fan(["Ann", "Bob"], handle_gate=True, merge_gate=False); self.script({"n": 1}, {"n": 2}); self.tick()
        self.flow("approve", "ann"); self.assertEqual(self.state("s1.merge")["status"], "collecting")
        self.flow("approve", "bob"); self.tick()
        self.assertEqual(self.state("s1.merge")["status"], "done"); self.assertEqual(self.merge()["items"], ["ann", "bob"])

    def test_an_ungated_merge_does_not_wait_for_a_stuck_sibling(self):
        self.fan(["Ann", "Bad"], merge_gate=False); self.script({"n": 1}, {"n": 0}, {"n": 0}, {"n": 0}); self.tick()
        self.assertEqual(self.state("s1.merge")["status"], "done"); self.assertEqual(self.merge()["missing"], ["bad"])

    def test_an_item_not_made_by_a_fan_out_cannot_reach_a_join(self):
        self.workflow("solo", """
            start: handle
            steps:
              handle:
                block: archive
                routes: [{to: merge}]
              merge:
                join: true
                routes: [{to: end}]
        """)
        self.add({"name": "Ann"}, key="a", wf="solo"); self.script({}); self.tick()
        self.assertEqual(self.state("a")["status"], "stuck"); self.assertIn("not made by a fan-out", self.state("a")["reason"])

    def test_validate_refuses_a_block_on_a_join_step_and_accepts_a_plain_join(self):
        self.fan(["Ann"])
        self.assertEqual(json.loads(self.run_flow("validate", "fan").stdout)["fan"], [])
        self.workflow("fan", self.FAN.replace("HANDLE_GATE", "").replace("join: true", "join: true\n            block: archive")
                                     .replace("MERGE_GATE", ""))
        self.assertIn("runs no block", json.loads(self.run_flow("validate", "fan").stdout)["fan"][0])


class Schedule(Base):
    """A workflow declares `schedule:`; `flow schedule` owns one crontab block per FLOW_HOME; cron calls `flow run`."""
    def setUp(self):
        super().setUp()
        fake = os.path.join(self.fake, "crontab.py")
        with open(fake, "w") as f:
            f.write("import sys,os\np=os.path.join(os.environ['FAKE_DIR'],'crontab.txt')\n"
                    "if sys.argv[1]=='-l':\n    sys.exit(1) if not os.path.exists(p) else sys.stdout.write(open(p).read())\n"
                    "else: open(p,'w').write(sys.stdin.read())\n")
        self.env["FLOW_CRONTAB"] = "%s %s" % (sys.executable, fake)
        with open(os.path.join(self.fake, "crontab.txt"), "w") as f: f.write("PATH=/usr/bin\n15 4 * * * other-job\n")

    def cron(self):
        with open(os.path.join(self.fake, "crontab.txt")) as f: return f.read()

    def test_schedule_writes_one_line_per_scheduled_workflow_and_keeps_other_lines(self):
        self.workflow("reply", REPLY + "schedule: \"52 6 * * *\"\n")
        self.flow("schedule"); c = self.cron()
        self.assertIn("15 4 * * * other-job", c)
        line = [l for l in c.splitlines() if l.startswith("52 6 * * *")][0]
        self.assertIn("run reply", line); self.assertIn("FLOW_HOME=", line)
        self.flow("schedule"); self.assertEqual(self.cron(), c)                       # idempotent
        self.workflow("reply", REPLY); self.flow("schedule")                          # schedule removed: block gone
        self.assertNotIn("run reply", self.cron()); self.assertIn("other-job", self.cron())

    def test_run_adds_a_minute_keyed_item_and_ticks_it(self):
        self.script(GOOD, PASS, SENT); out = json.loads(self.flow("run", "reply", json.dumps({"name": "Sam"})))
        self.assertTrue(out["added"].startswith("reply-")); self.assertEqual(self.state(out["added"])["status"], "done")

    def test_validate_refuses_a_bad_schedule(self):
        self.workflow("reply", REPLY + "schedule: \"every morning\"\n")
        self.assertIn("5-field cron", json.loads(self.run_flow("validate", "reply").stdout)["reply"][0])
