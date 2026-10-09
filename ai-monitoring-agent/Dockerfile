FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including curl and zstd for Ollama installation
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    git \
    gnupg \
    ca-certificates \
    zstd \
    && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI (gh)
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y gh && \
    rm -rf /var/lib/apt/lists/*

# Install Ollama
RUN curl -fsSL https://ollama.com/install.sh | sh && chmod +x /usr/local/bin/ollama

# Copy requirements and install Python dependencies (lightweight version first)
COPY requirements-light.txt .
RUN pip install --no-cache-dir -r requirements-light.txt

# Copy application code
COPY . .
RUN find . -name "*.py" -exec chmod +x {} \;

# Create models directory and CodeXA workspace
RUN mkdir -p /app/models /var/otel/codexa-workspace

# Install kubectl for service monitoring
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Configure git for CodeXA
RUN git config --global user.name "CodeXA" && \
    git config --global user.email "codexa@fabhotels.com" && \
    git config --global init.defaultBranch main && \
    git config --global --add safe.directory '*'

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser
RUN chown -R appuser:appuser /app /var/otel/codexa-workspace
USER appuser
ENV PATH="$PATH:/usr/local/bin"
ENV OLLAMA_MODELS="/app/models"

# Expose web dashboard port and Ollama API port
EXPOSE 5000
EXPOSE 11434

# Default command runs Ollama server and the integrated web dashboard
# Model will be downloaded on first use to avoid build time storage issues
CMD ["sh", "-c", "export PATH=$PATH:/usr/local/bin && ollama serve & sleep 5 && python web_dashboard.py"]
