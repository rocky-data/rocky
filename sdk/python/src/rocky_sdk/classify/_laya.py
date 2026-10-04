"""The optional Laya classifier. Imported only when ``use_model=True``."""

from __future__ import annotations

from typing import Any

from ._suggest import KINDS, ColumnEvidence

#: The checkpoint and Hub revision the aid was measured on. A different
#: checkpoint or revision has unmeasured accuracy.
LAYA_CHECKPOINT = "typed-decisions"
LAYA_REVISION = "7b928d828b7b0e022f929d9bd2e44165aa270148"
LAYA_VERSION = "0.3.26"

_MISSING = "the model classifier needs the optional extra: pip install 'rocky-sdk[classify]'"


def render(col: ColumnEvidence) -> str:
    """The text the model reads. Same shape as the measured spike."""
    text = f"Table: {col.table}. Column: {col.column}. Type: {col.type}."
    if col.values:
        text += " Sample values: " + "; ".join(col.values) + "."
    return text


class LayaClassifier:
    """One yes/no question per kind; the most likely ``yes`` wins.

    Below 0.5 for every kind, the answer is ``none``. Extra keyword arguments
    go to ``laya.Router`` (for example ``models={...}`` for a local copy of the
    weights, or ``device="cpu"``).
    """

    def __init__(self, *, revision: str | None = LAYA_REVISION, **router_kwargs: Any) -> None:
        try:
            import laya
        except ImportError as exc:  # pragma: no cover - depends on the extra
            raise ImportError(_MISSING) from exc
        if laya.__version__ != LAYA_VERSION:
            raise ImportError(
                f"laya {laya.__version__} is installed; the aid was measured on "
                f"{LAYA_VERSION}. Install 'rocky-sdk[classify]' to get the pinned version."
            )
        self._router = laya.Router(revision=revision, **router_kwargs)
        self._questions = {
            k: {"type": "noul", "instructions": f"Does this database column hold {v}?"}
            for k, v in KINDS.items()
            if k != "none"
        }

    def __call__(self, col: ColumnEvidence) -> tuple[str, float]:
        result = self._router.predict(render(col), self._questions, model=LAYA_CHECKPOINT)
        probs = {k: float(a["noul"]) for k, a in result["answers"].items()}
        best = max(probs, key=probs.__getitem__)
        if probs[best] < 0.5:
            return "none", 1.0 - probs[best]
        return best, probs[best]
