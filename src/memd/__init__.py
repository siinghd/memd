"""memd - embedded-first agent memory engine."""
from memd.core.schema import Kind, MemoryRecord, Scope, Source
from memd.engine.memory import Memory

__version__ = "0.5.2"

__all__ = ["Memory", "MemoryRecord", "Scope", "Source", "Kind", "__version__"]
