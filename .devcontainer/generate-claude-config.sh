#!/bin/bash
# Generate .claude.json for container from host OAuth config + keychain token

OAUTH_FILE=/workspace/.devcontainer/.claude-oauth.json
TOKEN_FILE=/workspace/.devcontainer/.claude-token
OUTPUT=~/.claude.json

# Extract OAuth account from exported file
OAUTH_ACCOUNT=$(jq '.oauthAccount' "$OAUTH_FILE" 2>/dev/null)
USER_ID=$(jq -r '.userID // "generated-user-id"' "$OAUTH_FILE" 2>/dev/null)
TOKEN=$(cat "$TOKEN_FILE" 2>/dev/null || echo "")
# Get last 20 chars of token (Claude Code uses this as the key suffix)
TOKEN_SUFFIX="${TOKEN: -20}"

# Create config with OAuth
jq -n \
  --argjson oauth "$OAUTH_ACCOUNT" \
  --arg userId "$USER_ID" \
  --arg tokenSuffix "$TOKEN_SUFFIX" \
  '{
    numStartups: 1,
    installMethod: "devcontainer",
    autoUpdates: true,
    cachedStatsigGates: {
      tengu_disable_bypass_permissions_mode: false,
      tengu_tool_pear: false
    },
    sonnet45MigrationComplete: true,
    shiftEnterKeyBindingInstalled: true,
    hasCompletedOnboarding: true,
    hasOpusPlanDefault: false,
    hasAvailableSubscription: false,
    oauthAccount: $oauth,
    userID: $userId,
    customApiKeyResponses: {
      approved: [$tokenSuffix],
      rejected: []
    },
    projects: {
      "/workspace": {
        allowedTools: [],
        history: [],
        mcpContextUris: [],
        mcpServers: {},
        enabledMcpjsonServers: [],
        disabledMcpjsonServers: [],
        hasTrustDialogAccepted: true,
        ignorePatterns: []
      }
    }
  }' > "$OUTPUT"

chmod 600 "$OUTPUT"
echo "Generated $OUTPUT with OAuth config"
