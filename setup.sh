#!/bin/bash
# One-time sandbox setup. Run once from your own terminal, from the repo:  sudo bash setup.sh
# It creates a locked-down user for agent sessions, gives it its own copy of Claude Code and your Claude token,
# lets the runner start sessions as that user and nothing else, and schedules the runner every 15 minutes.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo"; exit 1; }
FLOW="$(cd "$(dirname "$0")" && pwd)"                       # this repo
AGENT=${AGENT:-flowagent}; RUNNER=${RUNNER:-chat}; STATE=${STATE:-/var/lib/flow}
CLAUDE_SRC=${CLAUDE_SRC:-/home/reza/.npm-global/lib/node_modules/@anthropic-ai/claude-code}

# 1. the sandbox user: no shell, no sudo, no access to /home/reza (already closed to others)
id $AGENT >/dev/null 2>&1 || useradd --system --create-home --home-dir /var/lib/$AGENT --shell /usr/sbin/nologin $AGENT
groupadd -f flow
usermod -aG flow $AGENT; usermod -aG flow $RUNNER

# 2. item state lives here: the runner owns it; the sandbox can only write inside each item's work folder
install -d -o $RUNNER -g flow -m 2750 $STATE $STATE/runs

# 3. the sandbox's own copy of Claude Code (it cannot reach the one in /home/reza)
rm -rf /opt/flow-claude; mkdir -p /opt/flow-claude
cp -a "$CLAUDE_SRC" /opt/flow-claude/
chown -R root:root /opt/flow-claude; chmod -R go-w /opt/flow-claude

# 4. your Claude subscription token, readable only by the sandbox user
if [ ! -s /var/lib/$AGENT/.claude-token ]; then
  echo; echo "In another terminal, run:  claude setup-token"
  echo "Log in when it asks, then paste the token it prints here (input is hidden):"
  read -rs TOKEN; echo
  [ -n "$TOKEN" ] || { echo "no token given"; exit 1; }
  umask 077; printf '%s' "$TOKEN" > /var/lib/$AGENT/.claude-token; unset TOKEN
  chown $AGENT:$AGENT /var/lib/$AGENT/.claude-token; chmod 600 /var/lib/$AGENT/.claude-token
fi

# 5. the only door into the sandbox: a root-owned wrapper that runs Claude as the sandbox user
cat > /usr/local/bin/flow-claude <<'EOF'
#!/bin/bash
export HOME=/var/lib/flowagent
export CLAUDE_CODE_OAUTH_TOKEN="$(cat /var/lib/flowagent/.claude-token)"
exec /opt/flow-claude/claude-code/bin/claude.exe "$@"
EOF
chmod 755 /usr/local/bin/flow-claude
echo "$RUNNER ALL=($AGENT) NOPASSWD: /usr/local/bin/flow-claude" > /etc/sudoers.d/flow
chmod 440 /etc/sudoers.d/flow; visudo -cf /etc/sudoers.d/flow

# 6. the flow command, with its settings baked in, so Claude (as chat) and cron call it the same way
cat > /usr/local/bin/flow <<EOF
#!/bin/bash
export FLOW_HOME=$FLOW FLOW_RUNS=$STATE/runs FLOW_CLAUDE="sudo -n -u $AGENT /usr/local/bin/flow-claude"
exec python3 $FLOW/flow.py "\$@"
EOF
chmod 755 /usr/local/bin/flow

# 7. run every 15 minutes
echo "*/15 * * * * $RUNNER /usr/local/bin/flow tick >> $STATE/tick.log 2>&1" > /etc/cron.d/flow
chmod 644 /etc/cron.d/flow

echo "flow is set up. Try: flow status"
