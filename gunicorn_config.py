# Gunicorn configuration for ProjetoAdvocacia
# Increases worker timeout to prevent SIGKILL during heavy initialization
# (database connection pool setup, etc.)

import os

bind = f"0.0.0.0:{os.getenv('PORT', '5000')}"

# Allow up to 120 seconds for a worker to handle a request or initialize.
# The default 30s is too short for this app's DB connection pool warm-up.
timeout = 120

# Keep-alive connections open for 5 seconds between requests.
keepalive = 5

# One worker per CPU core (minimum 2). Adjust via the WEB_CONCURRENCY env var.
workers = int(os.getenv("WEB_CONCURRENCY", 2))

# Use the default sync worker; no async worker needed for this Flask app.
worker_class = "sync"

# Log to stdout so Railway captures all output in the deployment logs.
accesslog = "-"
errorlog = "-"
loglevel = "info"
