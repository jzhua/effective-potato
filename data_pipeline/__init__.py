"""Data Pipeline Dashboard package."""

__all__ = [
    "__version__",
]

try:
    from importlib.metadata import version
except ImportError:  # pragma: no cover - Python <3.8 fallback
    __version__ = "0.0.0"
else:
    try:
        __version__ = version("data-pipeline-dashboard")
    except Exception:  # pragma: no cover - distribution not installed
        __version__ = "0.0.0"
