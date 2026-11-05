"""Deterministic row generator for the simulators.

The implementation now lives in ``rewind.transform`` (the engine seam). This
module re-exports it so the simulator scripts keep their import path.
"""
from __future__ import annotations

from rewind.transform import generate_rows

__all__ = ["generate_rows"]
