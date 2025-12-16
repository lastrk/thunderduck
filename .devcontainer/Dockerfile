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
    # Purpose: Required for compiling code
    # Why necessary: Common development tool for compilation tasks
    build-essential \
    # CMake: Cross-platform build system generator
    # Purpose: Flexible build system for various projects
    # Why included: Many projects use CMake for building
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
    # Purpose: General-purpose scripting and tooling
    # Why necessary: Common tool for project automation and scripts
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

# pkgx Installation (User-space Package Manager)
# Purpose: Allow developers to install additional tools without sudo
# Why pkgx: Pure user-space (no root required), fast, intuitive commands, 10K+ packages
# Why useful: Install ad-hoc tools (ripgrep, jq, specific Java versions) on-demand
# Security: No privilege escalation, confined to ~/.pkgx directory
# Alternative to: apt-get (requires sudo), Nix (requires /nix directory setup)
# Installation: Runs as vscode user, installs to ~/.local/bin/pkgx
# Shell integration: Adds eval hook to .bashrc for package activation
RUN su vscode -c 'curl -fsSL https://pkgx.sh | sh' && \
    echo 'eval "$(pkgx --shellcode)"' >> /home/vscode/.bashrc

# ==============================================================================
# OPTIONAL DEVELOPMENT ENVIRONMENTS (Commented Out)
# ==============================================================================
# Uncomment the blocks below to install specific language toolchains.
# Each block is independent and can be enabled separately.
# After uncommenting, rebuild the container: F1 → "Dev Containers: Rebuild Container"
# ==============================================================================
# Podman (Docker)
RUN apt-get update && apt-get install -y podman

# ------------------------------------------------------------------------------
# Java Development Environment
# ------------------------------------------------------------------------------
# Purpose: Install multiple JDK versions, Maven, and Gradle
# Package sources:
#   - OpenJDK 11, 17, 21: Ubuntu 22.04 default repositories
#   - OpenJDK 24: Requires adding Oracle or Adoptium repository (see notes)
# Multi-architecture support: Works on both AMD64 and ARM64
# Note: Use 'update-java-alternatives' to switch between Java versions
# Note: Consider using pkgx for Java (pkgx install openjdk.org@17) as an alternative
# ------------------------------------------------------------------------------
 RUN apt-get update && apt-get install -y \
#     # OpenJDK 11 (LTS, wide enterprise adoption)
#     # Available on both AMD64 and ARM64
#     openjdk-11-jdk \
#     # OpenJDK 17 (LTS, current recommended version)
#     # Available on both AMD64 and ARM64
     openjdk-17-jdk \
#     # OpenJDK 21 (LTS, latest stable LTS release)
#     # Available on both AMD64 and ARM64
#     openjdk-21-jdk \
#     # OpenJDK 24 (Early Access - Not available in Ubuntu 22.04 repos)
#     # To install Java 24, add Adoptium repository:
#     # wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add -
#     # echo "deb https://packages.adoptium.net/artifactory/deb $(lsb_release -sc) main" > /etc/apt/sources.list.d/adoptium.list
#     # apt-get update && apt-get install -y temurin-24-jdk
#     # Maven (latest from Ubuntu repos, typically 3.6.x or 3.8.x)
     maven \
#     # Gradle (version from Ubuntu repos, may not be latest)
#     # For latest Gradle, use: pkgx install gradle.org
     gradle \
#     # Fix ca-certificates-java setup (workaround for cacerts directory issue)
#     # The ca-certificates-java postinst script sometimes fails on first run
#     # This ensures the Java certificate store is properly initialized
     && /var/lib/dpkg/info/ca-certificates-java.postinst configure \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
#
# # Configure default Java version (multi-architecture compatible)
# # Option 1: Use update-java-alternatives (recommended, handles all Java components)
# # RUN update-java-alternatives --set java-1.17.0-openjdk-$(dpkg --print-architecture)
# #
# # Option 2: Use update-alternatives for just the java binary
# # RUN update-alternatives --set java /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)/bin/java
# #
# # Option 3: Let update-alternatives choose automatically (uses highest version)
# # No action needed - already set during installation
#
# # Verify installation
RUN java -version && mvn -version && gradle -version

# ------------------------------------------------------------------------------
# Rust Development Environment
# ------------------------------------------------------------------------------
# Purpose: Install Rust toolchain with cargo and version management via rustup
# Why rustup: Official Rust toolchain installer, allows switching between versions
# Components: rustc (compiler), cargo (package manager), rustup (version manager)
# Installation: Runs as vscode user (non-root installation)
# Rust versions: stable, beta, nightly, or specific versions (e.g., 1.70.0)
# Switching versions: rustup default stable|beta|nightly|1.70.0
# ------------------------------------------------------------------------------
# RUN su vscode -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y' && \
#     echo 'source $HOME/.cargo/env' >> /home/vscode/.bashrc
#
# # Install additional Rust components (optional)
# # RUN su vscode -c 'rustup component add rustfmt clippy rust-analyzer'
#
# # Install additional toolchains (optional)
# # RUN su vscode -c 'rustup toolchain install nightly beta'
#
# # Verify installation
# # RUN su vscode -c 'rustc --version && cargo --version && rustup --version'

# ------------------------------------------------------------------------------
# C++ Development Environment (LLVM/Clang)
# ------------------------------------------------------------------------------
# Purpose: Install recent LLVM/Clang compiler toolchain
# Why LLVM: Modern C++ compiler with excellent diagnostics and tooling
# Version: LLVM 18 (latest stable as of 2024)
# Components: clang, clang++, lldb (debugger), lld (linker), clang-format, clang-tidy
# Alternative: Use system GCC (already installed via build-essential)
# Note: build-essential (GCC/G++) is already installed in base configuration
# ------------------------------------------------------------------------------
# RUN wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc && \
#     echo "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-18 main" > /etc/apt/sources.list.d/llvm.list && \
#     apt-get update && apt-get install -y \
#     # LLVM 18 core package
#     llvm-18 \
#     # Clang C/C++ compiler
#     clang-18 \
#     # Clang C++ standard library
#     libc++-18-dev \
#     libc++abi-18-dev \
#     # LLDB debugger
#     lldb-18 \
#     # LLD linker (faster than GNU ld)
#     lld-18 \
#     # Clang code formatting tool
#     clang-format-18 \
#     # Clang static analyzer
#     clang-tidy-18 \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
#
# # Set LLVM 18 as default (optional)
# # RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100 && \
# #     update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-18 100
#
# # Verify installation
# # RUN clang-18 --version && lldb-18 --version

# ------------------------------------------------------------------------------
# Python Development Environment
# ------------------------------------------------------------------------------
# Purpose: Enhanced Python development with conda, virtualenv, and tools
# Note: python3 and pip are already installed in base configuration
# Conda: Anaconda distribution with package and environment management
# Why conda: Better for data science, manages non-Python dependencies (CUDA, etc.)
# Miniconda vs Anaconda: Miniconda is minimal, Anaconda includes 250+ packages
# Note: Conda installation is large (~500MB-3GB), consider using pkgx or venv instead
# ------------------------------------------------------------------------------
# RUN apt-get update && apt-get install -y \
#     # Python virtual environment (already installed in base, shown for reference)
#     # python3-venv \
#     # Python development headers (for compiling C extensions)
#     python3-dev \
#     # pip wheel support
#     python3-wheel \
#     # setuptools for package installation
#     python3-setuptools \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
#
# # Install Miniconda (conda package manager)
# # Uncomment below to install Miniconda as vscode user
# # RUN su vscode -c 'wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
# #     bash /tmp/miniconda.sh -b -p $HOME/miniconda3 && \
# #     rm /tmp/miniconda.sh' && \
# #     echo 'export PATH="$HOME/miniconda3/bin:$PATH"' >> /home/vscode/.bashrc && \
# #     echo 'eval "$(conda shell.bash hook)"' >> /home/vscode/.bashrc
#
# # Initialize conda (optional, requires Miniconda to be installed above)
# # RUN su vscode -c '$HOME/miniconda3/bin/conda init bash'
#
# # Install common Python development tools via pip (optional)
# # RUN su vscode -c 'pip3 install --user \
# #     virtualenv \
# #     pipenv \
# #     poetry \
# #     black \
# #     flake8 \
# #     pylint \
# #     mypy \
# #     pytest'
#
# # Verify installation
# # RUN python3 --version && pip3 --version
# # RUN su vscode -c 'conda --version'  # If conda installed

# ------------------------------------------------------------------------------
# Clojure Development Environment
# ------------------------------------------------------------------------------
# Purpose: Install Clojure CLI tools (clj and clojure commands)
# Official guide: https://clojure.org/guides/install_clojure#_linux_instructions
# Dependencies: bash, curl, rlwrap (for REPL readline support), Java (OpenJDK)
# Installation: Downloads and runs official installer script from Clojure GitHub
# Components: clj (wrapper with rlwrap), clojure (direct java command)
# Install location: /usr/local/bin/clj, /usr/local/bin/clojure, /usr/local/lib/clojure
# Note: Requires Java to be installed (use Java block above or pkgx install openjdk.org@17)
# ------------------------------------------------------------------------------
# RUN apt-get update && apt-get install -y \
#     # rlwrap: Readline wrapper for REPL (provides command history, editing)
#     rlwrap \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
#
# # Download and install Clojure CLI tools
# # Note: Installs to /usr/local/bin by default, accessible system-wide
# RUN curl -L -O https://github.com/clojure/brew-install/releases/latest/download/linux-install.sh && \
#     chmod +x linux-install.sh && \
#     ./linux-install.sh && \
#     rm linux-install.sh
#
# # Verify installation
# # RUN clojure --version

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
# Purpose: Pre-create directories for VS Code Server installation
# Why necessary: VS Code installs server components to multiple locations
# Locations:
#   - /vscode/vscode-server/extensionsCache: Shared extensions cache
#   - /home/vscode/.vscode-server: Per-user server data
#   - /home/vscode/.vscode-server/extensionsCache: Per-user extensions cache
# mkdir -p: Creates directories (and parents if needed)
# chown: Changes ownership to vscode user and group
# Security: vscode user can write here without sudo, no volume mount needed
# Note: With --userns=keep-id, permissions work correctly with host user mapping
# Why both caches: VSCode syncs extensions between user cache and shared cache
RUN mkdir -p /vscode/vscode-server/extensionsCache \
             /home/vscode/.vscode-server/extensionsCache && \
    chown -R vscode:vscode /home/vscode/.vscode-server && \
    chmod -R 755 /home/vscode/.vscode-server && \
    chmod -R 777 /vscode

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

