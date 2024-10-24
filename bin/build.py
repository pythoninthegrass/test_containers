#!/usr/bin/env python

import logging
import os
import platform
import subprocess
from pathlib import Path
from typing import Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_command(cmd: list[str], env: Optional[Dict[str, str]] = None) -> str:
    """Run a command and return its output, with error handling."""
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, **(env or {})}
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {' '.join(cmd)}")
        logger.error(f"Error output: {e.stderr}")
        raise RuntimeError(f"Command failed with exit code {e.returncode}") from e
    except Exception as e:
        logger.error(f"Unexpected error running command: {' '.join(cmd)}")
        raise RuntimeError(f"Command failed: {str(e)}") from e


def build(setup_kwargs: dict) -> None:
    """Build script for installing custom pymssql on macOS."""
    if platform.system() != "Darwin":
        logger.error("This build script only supports macOS")
        raise SystemError("Unsupported platform")

    try:
        logger.info("Starting custom build process for macOS")

        # Install FreeTDS
        logger.info("Installing FreeTDS via Homebrew")
        run_command(["brew", "install", "FreeTDS"])

        # Get OpenSSL prefix
        logger.info("Getting OpenSSL prefix from Homebrew")
        openssl_prefix = run_command(["brew", "--prefix", "openssl"])

        # Set up environment variables
        build_env = {
            "CFLAGS": f"-I{openssl_prefix}/include",
            "LDFLAGS": f"-L{openssl_prefix}/lib -L/usr/local/opt/openssl/lib",
            "CPPFLAGS": f"-I{openssl_prefix}/include",
            "POETRY_BINARY_ENABLE": "false"
        }

        logger.info("Setting up build environment variables")
        os.environ.update(build_env)

        # Install pymssql
        logger.info("Installing pymssql from source")
        run_command([
            "pip", "install",
            "--pre",
            "--no-binary", ":all:",
            "pymssql",
            "--no-cache"
        ])

        logger.info("Custom build process completed successfully")

    except Exception as e:
        logger.error(f"Build process failed: {str(e)}")
        raise RuntimeError("Failed to complete custom build process") from e


if __name__ == "__main__":
    build({})
