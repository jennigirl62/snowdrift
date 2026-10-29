"""snowdrift: catch schema drift across warehouses before it breaks a load."""

from .agent import run
from .detector import DetectSettings, detect
from .impact import ImpactSettings
from .models import Column, Comparison, DriftEvent, DriftType, Severity

__all__ = ["run", "detect", "DetectSettings", "ImpactSettings", "Column", "Comparison", "DriftEvent", "DriftType", "Severity"]
__version__ = "0.1.0"
