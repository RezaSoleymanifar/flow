"""Live tests against real Claude, the same behaviors the fake stands in for. They spend real usage, so they only run
with FLOW_LIVE=1, using FLOW_CLAUDE (setup.sh's sandboxed wrapper by default after setup):

    FLOW_LIVE=1 FLOW_CLAUDE="sudo -n -u flowagent /usr/local/bin/flow-claude" python3 -m unittest tests.test_live

Set FLOW_RECORD=tests/fixtures/recorded to keep every raw reply; test_contract.py then checks them all."""
import json, os, shutil, sys, tempfile, unittest, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import flow  # noqa: E402

LIVE = os.environ.get("FLOW_LIVE") == "1"
ANSWER = {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}


@unittest.skipUnless(LIVE, "set FLOW_LIVE=1 to run against real Claude")
class Live(unittest.TestCase):
    def setUp(self):
        self.cwd = tempfile.mkdtemp(); os.chmod(self.cwd, 0o2777)

    def tearDown(self): shutil.rmtree(self.cwd, ignore_errors=True)

    def ask(self, msg, sid=None, resume=False, tools=(), schema=ANSWER):
        out, reason, got, _ = flow.claude(msg, schema, sid or str(uuid.uuid4()), resume, tools, cwd=self.cwd)
        self.assertIsNone(reason, reason); return out, got

    def test_structured_output_is_enforced(self):
        out, sid = self.ask("Reply with the answer 'pong'.")
        self.assertEqual(set(out), {"answer"}); self.assertTrue(sid)

    def test_resume_remembers_and_a_new_session_does_not(self):
        word = "zephyr%d" % (uuid.uuid4().int % 1000)
        _, sid = self.ask("Remember this codeword: %s. Answer 'ok'." % word)
        out, _ = self.ask("What codeword did I give you? Put only the codeword in answer.", sid, resume=True)
        self.assertIn(word, out["answer"])
        fresh, _ = self.ask("What codeword did I give you earlier? If none, answer 'none'.")
        self.assertNotIn(word, fresh["answer"])

    def test_no_tools_means_no_file_access(self):
        secret = "s%d" % (uuid.uuid4().int % 10 ** 8)
        with open(os.path.join(self.cwd, "secret.txt"), "w") as f: f.write(secret)
        out, _ = self.ask("Read the file secret.txt in the current folder and put its contents in answer. "
                          "If you cannot read files, answer 'cannot'.")
        self.assertNotIn(secret, out["answer"])

    def test_listed_tool_works_inside_the_work_folder(self):
        secret = "s%d" % (uuid.uuid4().int % 10 ** 8)
        with open(os.path.join(self.cwd, "secret.txt"), "w") as f: f.write(secret)
        out, _ = self.ask("Use the Read tool on ./secret.txt and put its exact contents in answer.", tools=["Read"])
        self.assertIn(secret, out["answer"])

    @unittest.skipUnless(os.environ.get("FLOW_LIVE_SANDBOX") == "1", "set FLOW_LIVE_SANDBOX=1 after setup.sh")
    def test_sandbox_cannot_write_outside_its_folder(self):
        target = os.path.join(os.environ.get("FLOW_RUNS", "/var/lib/flow/runs"), "escape-%s" % uuid.uuid4().hex[:6])
        self.ask("Use Bash to run: echo x > %s ; then answer 'done' whatever happened." % target, tools=["Bash"])
        self.assertFalse(os.path.exists(target), "the sandbox wrote outside its work folder")
        home = "/home/reza/.bashrc"
        out, _ = self.ask("Use Bash to run: head -c 20 %s ; put the output in answer, or 'denied'." % home, tools=["Bash"])
        self.assertNotIn("export", out["answer"])


if __name__ == "__main__":
    unittest.main()
