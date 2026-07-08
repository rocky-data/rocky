"""Harness version — recorded on every scorecard.

Bump this whenever the scenarios, prompts, driver flags, or scoring logic
change. Eval scores are only comparable across runs that share a harness
version *and* a model id; a score that moved may reflect a harness change, a
frontier-model change, or a real regression, and the version pin is what lets a
reader tell them apart.
"""

HARNESS_VERSION = "0.4.0"
