# Directive: Specifies Dockerfile syntax parser version
# Purpose: Enables BuildKit features like improved caching and parallel builds
# Why 1.5: Stable version with security improvements and heredoc support
# Security: Ensures consistent build behavior across different Docker/Podman versions
# syntax=docker/dockerfile:1.5

# Base Image Selection (Microsoft's Official DevContainer Image)
# Purpose: Provides Ubuntu 22.04 LTS with DevContainer optimizations
# Why Microsoft's image:
#   - Pre-configured non-root 'vscode' user (UID 1000)
#   - Common dev tools (git, curl, wget, build-essential) already installed
#   - Optimized for VSCode Remote Container extension
#   - Security hardening and regular updates from Microsoft
# Why Ubuntu 22.04: Long-term support (until 2027), wide package availability
# Alternative: Could use 'ubuntu:22.04' but would need manual vscode user setup
FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

# Environment Variable: Disable interactive prompts
# Purpose: Prevents apt-get from waiting for user input during package installation
# Why necessary: Build process is non-interactive; any prompt would hang the build
# Security: Ensures builds are reproducible and don't require human intervention
# Impact: Applied only during image build, not at runtime
ENV DEBIAN_FRONTEND=noninteractive

# Package Installation Layer (Build Tools and Languages)
# Purpose: Install all necessary development tools in a single layer
# Why single RUN: Each RUN creates a new layer; combining reduces image size
# Why '&&': Ensures all commands succeed; failure at any step aborts the build
# Security: Uses official package repositories, validates with apt-get update
RUN apt-get update && apt-get install -y \
    # Build Essential: GCC, G++, make, libc-dev
    # Purpose: Required for compiling C++ code (this project uses C++/Metal)
    # Why necessary: CMake projects need a C++ compiler
    build-essential \
    # CMake: Cross-platform build system generator
    # Purpose: This project uses CMakeLists.txt for building
    # Why necessary: Explicitly required by project's build instructions
    cmake \
    # Ninja: Fast build system (alternative to make)
    # Purpose: Faster parallel builds than traditional make
    # Why included: CMake can use Ninja as backend for improved build speed
    ninja-build \
    # Pkg-config: Helper tool for compiling applications and libraries
    # Purpose: Manages compile/link flags for libraries
    # Why needed: Many CMake scripts use pkg-config to find dependencies
    pkg-config \
    # Python 3: Interpreted language
    # Purpose: Project has Python tools (tools/check_api_coverage.py, etc.)
    # Why necessary: Project documentation references Python scripts
    python3 \
    # Python Pip: Package installer for Python
    # Purpose: Allows installing Python packages needed by project tools
    # Why necessary: May need to install Python dependencies for scripts
    python3-pip \
    # Python Venv: Virtual environment creator
    # Purpose: Isolates Python dependencies from system packages
    # Why useful: Prevents version conflicts between project and system Python packages
    python3-venv \
    # Curl: Command-line tool for transferring data with URLs
    # Purpose: Used below to download Node.js setup script
    # Why necessary: NodeSource setup script must be fetched from web
    curl \
    # Wget: Network downloader
    # Purpose: Alternative to curl for downloading files
    # Why included: Some scripts may prefer wget; provides flexibility
    wget \
    # Node.js Repository Setup (NodeSource)
    # Purpose: Add NodeSource repository for latest LTS version of Node.js
    # Why necessary: Ubuntu 22.04's default Node.js is outdated for Claude Code
    # -f: Fail silently on HTTP errors
    # -s: Silent mode (no progress bar)
    # -S: Show errors even in silent mode
    # -L: Follow redirects
    # Security: HTTPS prevents man-in-the-middle attacks; script is from trusted source
    && curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - \
    # Node.js and NPM Installation
    # Purpose: JavaScript runtime and package manager
    # Why necessary: Claude Code is distributed as npm package (@anthropic-ai/claude-code)
    # Why LTS: Long-term support version is stable and secure
    && apt-get install -y nodejs \
    # Cleanup: Remove package manager caches and temporary files
    # Purpose: Reduce final image size significantly (can save 100+ MB)
    # Why necessary: These files are only needed during installation, not at runtime
    # apt-get clean: Removes downloaded .deb files from /var/cache/apt/archives
    # rm -rf: Removes apt lists, temp files
    # Security: Reduces attack surface by removing unnecessary files
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# NPM Global Directory Configuration (Non-root User)
# Purpose: Configure npm to install global packages in user's home directory
# Why necessary: Default npm global path (/usr/local) requires root privileges
# Why 'su vscode': Runs commands as vscode user (not root)
# Security: Prevents need for sudo when installing npm packages
# mkdir -p ~/.npm-global: Creates directory for global npm packages
# npm config set prefix: Tells npm to install global packages to ~/.npm-global
RUN su vscode -c "mkdir -p ~/.npm-global && npm config set prefix ~/.npm-global"

# PATH Environment Variable: Add NPM global bin directory
# Purpose: Makes globally installed npm packages executable from command line
# Why necessary: Without this, 'claude-code' command wouldn't be found after npm install -g
# ${PATH}: Preserves existing PATH entries (system binaries, etc.)
# Security: Only adds vscode user's directory, not system-wide writable paths
ENV PATH="/home/vscode/.npm-global/bin:${PATH}"

# Git Safe Directory Configuration
# Purpose: Mark /workspace as safe to prevent "dubious ownership" errors
# Why necessary: Mounted volumes may have different UID/GID than container user
# When it occurs: Git sees /workspace owned by different user and refuses operations
# --system: Applies to all users (survives user switching)
# Security: Only marks specific directory, not entire filesystem
# Impact: Allows git commands to work on mounted workspace without warnings
RUN git config --system --add safe.directory /workspace

# VS Code Server Directory
# Purpose: Pre-create directory for VS Code Server installation
# Why necessary: VS Code tries to install server at /vscode/vscode-server
# mkdir -p: Creates /vscode directory (and parents if needed)
# chown: Changes ownership to vscode user and group
# Security: vscode user can write here without sudo, no volume mount needed
# Note: With --userns=keep-id, this directory needs to be created/fixed at runtime
# Trade-off: VS Code Server reinstalled on each container rebuild (acceptable)
RUN mkdir -p /vscode && chmod 777 /vscode

# Working Directory: Set default directory for RUN/CMD/ENTRYPOINT
# Purpose: All commands will execute from /workspace by default
# Why /workspace: Convention for DevContainers; matches mount point in devcontainer.json
# Impact: When opening terminal in container, starts in /workspace
# Benefit: Immediate access to project files without 'cd' command
WORKDIR /workspace

# Default Command: Specify what runs when container starts
# Purpose: Launch interactive bash shell when no other command is provided
# Why bash: Provides full shell environment for development work
# Format: JSON array is exec form (preferred over shell form)
# When used: Only if devcontainer.json doesn't override with different command
# Security: Bash is standard, well-audited shell; no automatic script execution
CMD ["/bin/bash"]
