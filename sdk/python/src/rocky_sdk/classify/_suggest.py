"""Column evidence, the pattern rules, and :func:`suggest`."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from ._sidecar import existing_classification

if TYPE_CHECKING:
    from ..client import RockyClient

#: The kinds of personal data the aid can suggest, with the wording the model
#: is asked about. ``none`` means no personal data of these kinds.
KINDS: dict[str, str] = {
    "email": "an email address",
    "phone": "a telephone number",
    "name": "a person's name",
    "address": "a postal or street address of a person",
    "gov_id": "a government identifier: tax number, social security, passport, national insurance",
    "financial": "a bank account number, IBAN or payment card number",
    "birth_date": "a person's date of birth",
    "network_id": "an IP address or a device MAC address",
    "none": "none of the above: no personal data",
}

#: The tag written for each kind unless the caller passes its own mapping.
#: Rocky has no fixed tag vocabulary; a tag with no ``[mask]`` strategy shows
#: up as a gap in ``rocky compliance``.
DEFAULT_KIND_TO_TAG: dict[str, str] = {
    "email": "pii",
    "phone": "pii",
    "name": "pii",
    "address": "pii",
    "birth_date": "pii",
    "network_id": "pii",
    "gov_id": "confidential",
    "financial": "financial",
}

#: Shown beside any model suggestion for a date or timestamp column, unless it
#: says birth date. The model tags ordinary dates as phones, addresses, and more.
_DATE_WARNING = "The model often mislabels ordinary dates. Check that this is personal data."

#: Patterns the model is known to get wrong, shown beside its suggestions.
_MODEL_WARNINGS: dict[str, str] = {
    "phone": "The model often reads dates and numeric codes as phone numbers.",
    "name": "The model often reads short text (titles, carriers, banks) as names.",
}


@dataclass(frozen=True)
class ColumnEvidence:
    """What a classifier sees for one column."""

    table: str
    column: str
    #: Rocky type name (``String``, ``Int64``, ``Date``, ...).
    type: str
    values: tuple[str, ...] = ()


#: A classifier maps a column to ``(kind, probability)``. ``kind`` is a key
#: of :data:`KINDS`.
Classifier = Callable[[ColumnEvidence], tuple[str, float]]


@dataclass(frozen=True)
class Suggestion:
    """One suggested tag. Nothing is written until a person accepts it."""

    column: str
    kind: str
    tag: str
    #: ``"rules"`` or ``"model"``.
    source: str
    #: The model's probability. Shown, not trusted: it is not well calibrated.
    probability: float
    sample_values: tuple[str, ...] = ()
    warning: str | None = None
    reason: str = field(default="")


# --------------------------------------------------------------------- rules
_NAME_RULES: list[tuple[str, str]] = [
    ("email", r"e-?mail"),
    ("phone", r"phone|mobile|telemovel|telefone|(^|_)tel($|_)"),
    ("birth_date", r"birth|(^|_)dob($|_)|nascimento"),
    ("gov_id", r"(^|_)ssn($|_)|passport|national_insurance|(^|_)nif($|_)|tax_id|(^|_)nino?($|_)"),
    ("financial", r"iban|card_number|account_number|(^|_)pan($|_)"),
    ("network_id", r"(^|_)ip($|_)|ip_address|(^|_)mac($|_)"),
    ("address", r"address|morada|street"),
    ("name", r"first_name|last_name|full_name|(^|_)nome|(^|_)name$"),
]
#: A ``_name`` column that names a thing, not a person.
_NOT_A_PERSON = re.compile(
    r"product|company|table|file|bank|brand|host|domain|event|column|schema|vendor|carrier"
)
_VALUE_RULES: list[tuple[str, str]] = [
    ("email", r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"),
    ("financial", r"^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$"),
    ("financial", r"^\d{13,19}$"),
    ("gov_id", r"^\d{3}-\d{2}-\d{4}$"),
    (
        "network_id",
        r"^(\d{1,3}\.){3}\d{1,3}$|^(?=(?:[0-9a-f]*:){2})[0-9a-f:]+$|^([0-9A-F]{2}:){5}[0-9A-F]{2}$",
    ),
    ("phone", r"^\+?[\d\s()-]{9,17}$"),
]


#: Value rules that never fire on a column of these Rocky types. Dates and
#: times look like phone numbers and IPv6 addresses to the patterns.
_VALUE_RULE_SKIPS: dict[str, tuple[str, ...]] = {
    "phone": ("Date", "Timestamp", "Decimal"),
    "network_id": ("Date", "Timestamp", "Int64", "Decimal"),
    "financial": ("Date", "Timestamp", "Decimal"),
    "gov_id": ("Date", "Timestamp", "Decimal"),
}


def _luhn(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return total % 10 == 0


def rules_classifier(col: ColumnEvidence) -> tuple[str, float]:
    """Pattern rules on the column name, then on the sample values.

    These are the rules measured in the spike. A value rule fires when at
    least 4 of 5 values match (scaled for other sample sizes).
    """
    name = col.column.lower()
    if col.type == "Boolean":
        return "none", 1.0
    for kind, pattern in _NAME_RULES:
        if re.search(pattern, name) and not (kind == "name" and _NOT_A_PERSON.search(name)):
            return kind, 1.0
    if col.values:
        need = max(1, round(len(col.values) * 0.8))
        for kind, pattern in _VALUE_RULES:
            if col.type in _VALUE_RULE_SKIPS.get(kind, ()):
                continue
            hits = sum(bool(re.match(pattern, v, re.I)) for v in col.values)
            if hits >= need:
                if pattern == r"^\d{13,19}$" and not all(_luhn(v) for v in col.values):
                    continue
                return kind, 1.0
    return "none", 1.0


# --------------------------------------------------------------------- types
_TYPE_PATTERNS: list[tuple[str, str]] = [
    ("Boolean", r"^bool"),
    ("Timestamp", r"^timestamp|^datetime"),
    ("Date", r"^date$"),
    ("Int64", r"^(big|small|tiny|hug|u)?int|^integer|^long"),
    ("Decimal", r"^decimal|^numeric|^double|^float|^real"),
    ("String", r"^(var)?char|^text|^string|^uuid"),
]


def rocky_type_name(warehouse_type: str) -> str:
    """Map a warehouse type (``VARCHAR``, ``BIGINT``) to a Rocky type name.

    The aid was measured on Rocky type names. Unknown types pass through.
    """
    t = warehouse_type.strip().lower()
    for rocky, pattern in _TYPE_PATTERNS:
        if re.search(pattern, t):
            return rocky
    return warehouse_type


# --------------------------------------------------------------------- suggest
def _warning(source: str, kind: str, rocky_type: str) -> str | None:
    if source != "model":
        return None
    if rocky_type in ("Date", "Timestamp") and kind != "birth_date":
        return _DATE_WARNING
    return _MODEL_WARNINGS.get(kind)


def evidence_from_profile(profile, *, table: str | None = None) -> list[ColumnEvidence]:
    """Build :class:`ColumnEvidence` from a ``rocky profile`` result."""
    tbl = table or (profile.profiled_table or profile.model).split(".")[-1]
    out = []
    for c in profile.columns:
        values = c.sample_values or c.observed_values or []
        out.append(
            ColumnEvidence(
                table=tbl, column=c.name, type=rocky_type_name(c.type), values=tuple(values)
            )
        )
    return out


def suggest(
    client: RockyClient,
    model: str,
    *,
    sidecar: str | Path | None = None,
    use_model: bool = False,
    sample: int = 5,
    kind_to_tag: Mapping[str, str] | None = None,
    model_classifier: Classifier | None = None,
) -> list[Suggestion]:
    """Suggest tags for ``model``'s columns. Writes nothing.

    Runs ``rocky profile <model> --sample <sample>`` (DuckDB targets only for
    now; other targets return no suggestions because the profile is
    unavailable). Columns already in the sidecar's ``[classification]`` block
    are skipped when ``sidecar`` is given.

    ``use_model=True`` runs the Laya model on every column the rules left
    untagged. It needs ``pip install 'rocky-sdk[classify]'`` and downloads up to
    2.3 GB of weights from huggingface.co on first use. ``model_classifier``
    replaces Laya (for tests, or another local model).
    """
    if sample < 1:
        raise ValueError("sample must be at least 1: the classifiers need sample values")
    tags = dict(DEFAULT_KIND_TO_TAG if kind_to_tag is None else kind_to_tag)
    already = set(existing_classification(sidecar)) if sidecar is not None else set()

    profile = client.profile(model, sample=sample)
    if profile.unavailable:
        return []

    second: Classifier | None = None
    if use_model:
        if model_classifier is not None:
            second = model_classifier
        else:
            from ._laya import LayaClassifier

            second = LayaClassifier()

    out: list[Suggestion] = []
    for col in evidence_from_profile(profile):
        if col.column in already:
            continue
        kind, prob = rules_classifier(col)
        source = "rules"
        if kind == "none" and second is not None:
            kind, prob = second(col)
            source = "model"
        if kind == "none" or kind not in tags:
            continue
        out.append(
            Suggestion(
                column=col.column,
                kind=kind,
                tag=tags[kind],
                source=source,
                probability=round(prob, 4),
                sample_values=col.values,
                warning=_warning(source, kind, col.type),
                reason=f"{source} classified this column as {KINDS[kind]}",
            )
        )
    return out
