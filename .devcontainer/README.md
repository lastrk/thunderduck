# Secure DevContainer Configuration for cudf-metal

This directory contains a secure, Podman-compatible DevContainer configuration for the cudf-metal project. The configuration follows Microsoft's recommended best practices and implements defense-in-depth security principles.

## Security Model

### Access Controls

| Resource | Access Level | Explanation |
|----------|--------------|-------------|
| Project workspace (`/workspace`) | **Read-Write** | Full access to project files for development (includes `.devcontainer/`) |
| Host network | **Blocked** | Container cannot access host services |
| Internet (HTTP/HTTPS) | **Allowed** | Required for package installation |
| Root privileges | **Restricted** | Non-root user with sudo for toolchain installation |

### Security Features

1. **Non-Root Execution**
   - Container runs as `vscode` user (non-root)
   - Sudo available for installing development tools
   - Prevents privilege escalation attacks

2. **Minimal Capabilities**
   - Drops all Linux capabilities by default
   - Only grants essential capabilities (SETUID, SETGID, AUDIT_WRITE)
   - Reduces attack surface significantly

3. **Network Isolation**
   - Uses `slirp4netns` for user-mode networking
   - Container cannot access host services (no `127.0.0.1` access to host)
   - Internet access via HTTP/HTTPS for package downloads
   - Ideal for development while maintaining security boundaries

4. **Filesystem Isolation**
   - Project workspace has full read-write access
   - Git repository accessible for commits
   - Container isolated from host filesystem outside workspace

5. **Additional Hardening**
   - `no-new-privileges` security option prevents privilege escalation
   - `init` process (tini) for proper zombie process handling
   - Rootless Podman operation (no daemon running as root)

## Prerequisites

### Required Software

1. **Podman** (rootless container runtime)
   ```bash
   # macOS
   brew install podman

   # Ubuntu/Debian
   sudo apt-get install podman

   # Fedora/RHEL
   sudo dnf install podman
   ```

2. **VSCode** with DevContainer extension
   ```bash
   # Install VSCode, then add extension:
   code --install-extension ms-vscode-remote.remote-containers
   ```

3. **Podman Machine** (macOS only)
   ```bash
   # Initialize and start Podman machine
   podman machine init
   podman machine start
   ```

### Configure VSCode to Use Podman

Add to your VSCode settings (`.vscode/settings.json` or user settings):

```json
{
  "dev.containers.dockerPath": "podman",
  "dev.containers.dockerComposePath": "podman-compose"
}
```

Alternatively, set environment variable:
```bash
export DOCKER_HOST="unix:///run/user/$(id -u)/podman/podman.sock"
```

## Usage

### Opening the DevContainer

1. **Open project in VSCode**
   ```bash
   code /path/to/cudf-metal
   ```

2. **Reopen in Container**
   - Press `F1` or `Cmd+Shift+P` (macOS) / `Ctrl+Shift+P` (Linux/Windows)
   - Type: "Dev Containers: Reopen in Container"
   - Select it and wait for container to build and start

3. **First-time setup** (inside container)
   ```bash
   # Claude Code is automatically installed via postCreateCommand
   # Authentication token is extracted from macOS keychain and injected automatically
   claude --version

   # Verify git works
   git status

   # Build project
   mkdir build && cd build
   cmake .. -DBUILD_TESTS=ON
   make -j4
   ```

### Authentication

Claude Code authentication is handled automatically:

1. **On container start**: `initializeCommand` extracts your API/OAuth token from macOS keychain
2. **Token file**: Token is written to `.devcontainer/.claude-token` (gitignored)
3. **Template injection**: `postCreateCommand` uses `claude.json.template` and injects the API key
4. **Configuration created**: API key is injected into `~/.claude.json` from the template
5. **No manual login needed**: Claude Code works immediately without authentication prompts

**How it works**:
1. The template file `.devcontainer/claude.json.template` contains a placeholder:
   ```json
   {
     "apiKey": "{{API_KEY_PLACEHOLDER}}",
     ...
   }
   ```
2. During container creation, the placeholder is replaced with your actual API key
3. The resulting `~/.claude.json` file contains your credentials securely

**Security notes**:
- Token is extracted from keychain on-demand, not stored in git
- `.claude-token` file is gitignored and dockerignored
- Token only exists locally on your machine
- Fresh token extracted on each container rebuild
- Claude Code refreshes credentials automatically (default: every 5 minutes)

**If authentication fails**: You can manually authenticate inside the container:
```bash
# Verify token file exists
cat /workspace/.devcontainer/.claude-token

# Verify .claude.json was created and contains API key
cat ~/.claude.json

# Check the template file
cat /workspace/.devcontainer/claude.json.template

# Manually recreate .claude.json if needed
TOKEN=$(cat /workspace/.devcontainer/.claude-token) && \
  sed "s|{{API_KEY_PLACEHOLDER}}|$TOKEN|g" /workspace/.devcontainer/claude.json.template > ~/.claude.json && \
  chmod 600 ~/.claude.json

# Or authenticate interactively
claude /login
```

### Installing Additional Tools

The container provides sudo access for installing toolchains:

```bash
# Inside the container
sudo apt-get update
sudo apt-get install <package-name>

# Or using pip
pip3 install <package-name>

# Or using npm
npm install -g <package-name>
```

### Committing from Inside Container

Git is pre-configured and has access to the workspace:

```bash
# Inside the container
git add .
git commit -m "Your commit message"
git push
```

**Note**: Git credentials may need to be configured. Use SSH keys or GitHub CLI for authentication.

## Container Architecture

### Base Image

Uses Microsoft's official DevContainer base image:
```
mcr.microsoft.com/devcontainers/base:ubuntu-22.04
```

**Why this image?**
- Officially maintained by Microsoft
- Optimized for DevContainer workflows
- Includes common development tools (git, curl, wget, build-essential)
- Ubuntu 22.04 LTS provides long-term support
- Non-root `vscode` user pre-configured

### Installed Tools

| Tool | Purpose | Installed Via |
|------|---------|---------------|
| git | Version control | Base image |
| build-essential | C++ compilation | apt |
| cmake, ninja | Build system | apt |
| python3, pip | Project tooling | apt |
| node.js, npm | Claude Code, npx | NodeSource |

### Mount Points

| Host Path | Container Path | Access |
|-----------|----------------|--------|
| `${workspaceFolder}` | `/workspace` | Read-Write |

## Network Configuration

### What's Accessible

✅ **Allowed:**
- Package repositories (apt, npm, pip)
- Git remote repositories (GitHub, GitLab, etc.)
- Public internet via HTTP/HTTPS
- DNS resolution

❌ **Blocked:**
- Host machine services (e.g., `localhost:3000` on host)
- Host filesystem outside mounted workspace
- Other containers on host (unless explicitly linked)
- Privileged operations without sudo

### Network Mode: slirp4netns

The container uses `slirp4netns` for user-mode networking:
- No elevated privileges required
- Provides NAT-based internet access
- Isolates container from host network stack
- Compatible with rootless Podman

## Troubleshooting

### Container Won't Start

**Problem**: "Error: container failed to start"

**Solution**:
```bash
# Check Podman is running (macOS)
podman machine list
podman machine start

# Rebuild container
# In VSCode: F1 -> "Dev Containers: Rebuild Container"
```

### Permission Denied Errors

**Problem**: Cannot write to workspace

**Solution**:
```bash
# On host, check ownership
ls -la /path/to/cudf-metal

# Fix if needed (macOS/Linux)
chown -R $(whoami) /path/to/cudf-metal
```

### Git "Dubious Ownership" Error

**Problem**: `fatal: detected dubious ownership in repository`

**Solution**: Already handled by `git config --system --add safe.directory /workspace` in Dockerfile. If still occurs:
```bash
# Inside container
git config --global --add safe.directory /workspace
```

### NPM Global Install Fails

**Problem**: Permission denied when installing global npm packages

**Solution**: Already configured to use `~/.npm-global`. If issues persist:
```bash
# Inside container
mkdir -p ~/.npm-global
npm config set prefix ~/.npm-global
export PATH=~/.npm-global/bin:$PATH
```

### Cannot Access Host Services

**Problem**: Cannot connect to database/service running on host

**This is by design for security.** If you need to access host services:

**Option 1** (Recommended): Run service in separate container and link
```bash
# Use docker-compose or podman-compose
# Add service to docker-compose.yml
```

**Option 2** (Less secure): Modify network mode
```jsonc
// In devcontainer.json, change runArgs:
"--network=host"  // ⚠️ This removes network isolation!
```

### Slow File Operations

**Problem**: File operations feel sluggish

**Solution**: Already using `consistency=cached` for mounts. For better performance:
- Ensure Podman machine has adequate resources (macOS):
  ```bash
  podman machine stop
  podman machine set --cpus 4 --memory 8192
  podman machine start
  ```

## Customization

### Adding VSCode Extensions

Edit `devcontainer.json`:
```jsonc
"customizations": {
  "vscode": {
    "extensions": [
      "ms-vscode.cpptools",
      "your.extension.id"
    ]
  }
}
```

### Adding System Packages

Edit `Dockerfile`:
```dockerfile
RUN apt-get update && apt-get install -y \
    your-package-here \
    && apt-get clean
```

### Relaxing Security (Not Recommended)

If you need to add capabilities:
```jsonc
// devcontainer.json
"runArgs": [
  "--cap-add=CAP_NAME"
]
```

**Warning**: Only add capabilities you understand and need. Each capability increases attack surface.

## Architecture Decisions

### Why Podman Instead of Docker?

1. **Rootless by default**: More secure architecture
2. **No daemon**: Reduces attack surface
3. **OCI-compatible**: Works with standard container images
4. **Drop-in replacement**: Compatible with Docker CLI

### Why slirp4netns Network Mode?

1. **User-mode networking**: No root privileges required
2. **Security**: Isolates container from host network
3. **Compatibility**: Works with rootless Podman
4. **Internet access**: Allows HTTP/HTTPS while blocking host

### Why Not Read-Only DevContainer Config?

The `.devcontainer/` directory is accessible at `/workspace/.devcontainer` with read-write permissions. While making it read-only would provide defense-in-depth, the practical benefits are minimal:
- Config files (Dockerfile, devcontainer.json) are only used at build/startup time
- Malicious code with write access already has access to all source code
- Container already has strong security boundaries (capabilities, network isolation)
- Simplicity and ease of maintenance outweigh marginal security benefit in dev environments

## Performance Considerations

### Build Performance

- Image layers are cached between builds
- `.dockerignore` excludes build artifacts and dependencies
- Using Microsoft's base image reduces build time

### Runtime Performance

- Rootless Podman has minimal overhead vs root mode
- slirp4netns adds ~5-10% latency to network calls
- File I/O performance is near-native with volume mounts

## Security Checklist

Before using this configuration in production or with sensitive data:

- [ ] Review and understand all security settings
- [ ] Verify Podman is running in rootless mode
- [ ] Confirm no additional capabilities beyond defaults
- [ ] Test network isolation (cannot reach host services)
- [ ] Understand that DevContainer config is part of workspace (read-write)
- [ ] Audit any additional packages or extensions added
- [ ] Keep base image updated for security patches

## References

- [DevContainer Specification](https://containers.dev/)
- [Microsoft DevContainer Images](https://github.com/devcontainers/images)
- [Podman Documentation](https://docs.podman.io/)
- [VSCode DevContainers](https://code.visualstudio.com/docs/devcontainers/containers)
- [slirp4netns Documentation](https://github.com/rootless-containers/slirp4netns)

## License

This DevContainer configuration follows the same license as the cudf-metal project.
