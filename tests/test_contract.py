"""Contract tests: the runner and the fake agree with the real Claude Code CLI.

Fixture tests always run. The CLI-flag test runs when a real `claude` is found (FLOW_REAL_CLAUDE or on PATH); it needs
no login, so it runs on the box and catches a Claude Code update that renames or drops a flag the runner relies on."""
import json, os, re, shutil, subprocess, sys, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE)); sys.path.insert(0, HERE)
os.environ.setdefault("FLOW_HOME", HERE)
import flow  # noqa: E402
from base import Base  # noqa: E402

FIX = os.path.join(HERE, "fixtures")
REAL = os.environ.get("FLOW_REAL_CLAUDE") or shutil.which("claude") or (
    "/home/reza/.npm-global/bin/claude" if os.path.exists("/home/reza/.npm-global/bin/claude") else None)


def fixture(name):
    with open(os.path.join(FIX, name)) as f: return f.read()


class RealReplies(unittest.TestCase):
    def test_real_error_reply_is_an_error_with_its_session(self):
        out, reason, sid, cost = flow.parse_reply(fixture("real_not_logged_in.json"), 0, "", {"type": "object"})
        self.assertIsNone(out); self.assertIn("Not logged in", reason)
        self.assertEqual(sid, "11111111-1111-4111-8111-111111111111"); self.assertEqual(cost, 0)

    def test_real_missing_session_is_recognised_as_lost(self):
        _, reason, sid, _ = flow.parse_reply("", 1, fixture("real_resume_missing.stderr"), {"type": "object"})
        self.assertTrue(flow.session_lost(reason)); self.assertIsNone(sid)

    def test_real_bad_uuid_is_an_error_not_a_lost_session(self):
        _, reason, _, _ = flow.parse_reply("", 1, fixture("real_bad_uuid.stderr"), {"type": "object"})
        self.assertIn("Invalid session ID", reason); self.assertFalse(flow.session_lost(reason))

    def test_recorded_replies_parse(self):
        """Every reply saved with FLOW_RECORD into fixtures/recorded/ must parse without crashing the runner."""
        d = os.path.join(FIX, "recorded")
        for name in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            with open(os.path.join(d, name)) as f: r = json.load(f)
            schema = json.loads(r["argv"][r["argv"].index("--json-schema") + 1])
            res = flow.parse_reply(r["stdout"], r["returncode"], r["stderr"], schema)
            self.assertEqual(len(res), 4, name)


class FakeMatchesReal(Base):
    def run_fake(self, *args):
        env = dict(self.env)
        return subprocess.run([sys.executable, os.path.join(HERE, "fake_claude.py")] + list(args),
                              env=env, capture_output=True, text=True)

    def test_fake_reply_has_the_real_top_level_fields(self):
        real = set(json.loads(fixture("real_not_logged_in.json")))
        self.script({"a": 1})
        fake = set(json.loads(self.run_fake("-p", "x", "--output-format", "json").stdout)) - {"structured_output"}
        self.assertTrue(fake <= real, fake - real)

    def test_fake_fails_exactly_like_the_real_cli(self):
        p = self.run_fake("-p", "x", "--output-format", "json", "--resume", "22222222-2222-4222-8222-222222222222")
        self.assertEqual((p.returncode, p.stdout, p.stderr), (1, "", fixture("real_resume_missing.stderr")))
        p = self.run_fake("-p", "x", "--output-format", "json", "--session-id", "nope")
        self.assertEqual((p.returncode, p.stderr), (1, fixture("real_bad_uuid.stderr")))
        p = self.run_fake("-p", "x", "--output-format", "json", "--bogus")
        self.assertEqual(p.returncode, 1); self.assertIn("unknown option '--bogus'", p.stderr)

    def test_fake_refuses_to_reuse_a_session_id(self):
        self.script({}, {})
        sid = "33333333-3333-4333-8333-333333333333"
        self.assertEqual(self.run_fake("-p", "x", "--output-format", "json", "--session-id", sid).returncode, 0)
        p = self.run_fake("-p", "x", "--output-format", "json", "--session-id", sid)
        self.assertEqual(p.returncode, 1); self.assertIn("already in use", p.stderr)


@unittest.skipUnless(REAL, "no real claude CLI on this machine")
class RealCli(unittest.TestCase):
    def test_every_flag_the_runner_uses_exists(self):
        help_text = subprocess.run([REAL, "--help"], capture_output=True, text=True).stdout
        captured = []
        real_run = subprocess.run
        def spy(cmd, **kw):
            captured.append(cmd); raise OSError("spy")
        flow.subprocess.run = spy
        try: flow.claude("m", {"type": "object"}, "s", False, ["Read"], {"x": {}}, "sonnet")
        finally: flow.subprocess.run = real_run
        flags = {a for a in captured[0] if a.startswith("-")}
        missing = [f for f in flags if not re.search(r"(^|[\s,])%s\b" % re.escape(f), help_text)]
        self.assertEqual(missing, [], "the installed claude no longer has these flags")
        self.assertIn('"dontAsk"', help_text)                                  # the permission mode we pass

    def test_real_cli_rejects_resume_of_a_missing_session_the_way_the_fake_does(self):
        p = subprocess.run([REAL, "-p", "x", "--output-format", "json", "--resume",
                            "22222222-2222-4222-8222-222222222222"], capture_output=True, text=True, cwd="/tmp")
        self.assertEqual(p.returncode, 1); self.assertTrue(flow.session_lost(p.stderr))


if __name__ == "__main__":
    unittest.main()
