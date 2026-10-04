"""Suggest personal-data ``[classification]`` tags for a model's columns.

A reviewer aid, never an automatic tagger. :func:`suggest` returns
:class:`Suggestion` objects; a person accepts or rejects each one, and
:func:`apply_accepted` writes only the accepted ones into the model sidecar.

Two classifiers run in order. Pattern rules run first and need no extra
dependencies. When ``use_model=True``, the Laya decision model (optional extra
``rocky-sdk[classify]``) looks again at every column the rules left untagged.
It runs locally; no value leaves the machine.

Measured on a blind set of 160 columns (72 with personal data), this
combination found 69 of 72, but 31 of its 100 flags were wrong. Laya in
particular reads many dates as ``phone`` and short text as ``name``. That error
rate is why every suggestion needs a person to accept it.
"""

from __future__ import annotations

from ._laya import LAYA_CHECKPOINT, LAYA_REVISION, LAYA_VERSION, LayaClassifier
from ._sidecar import apply_accepted, existing_classification
from ._suggest import (
    DEFAULT_KIND_TO_TAG,
    KINDS,
    Classifier,
    ColumnEvidence,
    Suggestion,
    rules_classifier,
    suggest,
)

__all__ = [
    "DEFAULT_KIND_TO_TAG",
    "KINDS",
    "LAYA_CHECKPOINT",
    "LAYA_REVISION",
    "LAYA_VERSION",
    "Classifier",
    "ColumnEvidence",
    "LayaClassifier",
    "Suggestion",
    "apply_accepted",
    "existing_classification",
    "rules_classifier",
    "suggest",
]
