---
title: Suggest classification tags
description: An opt-in aid in rocky-sdk that suggests personal-data tags for a model's columns. A person accepts or rejects each suggestion.
sidebar:
  order: 3
---

`rocky_sdk.classify` suggests [`[classification]`](/guides/governance/#tag-columns-in-the-model-sidecar) tags for the columns of a model. It never writes a tag by itself. A person reads each suggestion, then accepts or rejects it. Only accepted suggestions reach the model sidecar.

The aid needs a DuckDB target today, because `rocky profile` is DuckDB only.

## How the aid makes a suggestion

```
  rocky profile <model> --sample 5 ──▶ pattern rules ──┬─ match ─────────────────┐
                                                       └─ no match ──▶ model ──┤  (optional)
                                                                               ▼
  [classification] in models/<model>.toml ◀── apply_accepted ◀── person accepts ◀── suggestions
                    │
                    └──▶ rocky compliance checks each tag against [mask]
```

1. Rocky profiles the model and draws 5 random values from each column.
2. Pattern rules check the column name and the values.
3. With `use_model=True`, a local decision model looks again at every column the rules did not match.
4. You accept or reject each suggestion. `apply_accepted` writes the accepted ones.

## How often the aid is wrong

The aid was measured on a test set of 160 columns. 72 of those columns held personal data.

| Setup | Personal-data columns found | Suggestions that were right |
|---|---|---|
| Rules only | 36 of 72 | 36 of 38 |
| Rules, then the model | 68 of 72 | 68 of 92 |

The rules were changed after the first measurement, so that dates and times no longer match as phone numbers or IP addresses. These numbers are for the changed rules on the same set, so they are likely better than you will see on your own data.

About 1 suggestion in 4 is wrong with the model on. That is why a person must check each one. The model makes two kinds of mistake often:

- It reads many dates and numeric codes as phone numbers.
- It reads short text, such as job titles or carrier names, as person names.

Each suggestion from the model carries a `warning` when it falls into one of these kinds. Its `probability` is shown, but do not rely on it. The model's probabilities are not well calibrated.

## Install

The rules need nothing beyond `rocky-sdk`:

```bash
pip install rocky-sdk
```

The model is an optional extra. It installs PyTorch and the [Laya](https://huggingface.co/convaiinnovations/laya) decision model, version `0.3.26`:

```bash
pip install 'rocky-sdk[classify]'
```

On first use the model downloads up to 2.3 GB of weights from `huggingface.co`. The model runs on your machine. No column value leaves it.

## Suggest, review, and write

```python
from rocky_sdk import RockyClient
from rocky_sdk.classify import apply_accepted, suggest

client = RockyClient(models_dir="models")
sidecar = "models/stg_customers.toml"

suggestions = suggest(client, "stg_customers", sidecar=sidecar, use_model=True)

accepted = []
for s in suggestions:
    print(s.column, s.kind, "->", s.tag, f"({s.source})", s.sample_values)
    if s.warning:
        print("  warning:", s.warning)
    if input("accept? [y/N] ").strip().lower() == "y":
        accepted.append(s)

apply_accepted(sidecar, accepted)
```

`suggest` skips a column that already has a tag in the sidecar. `apply_accepted` only adds keys. It never changes or removes a tag that is already there.

## Choose the tags

By default the aid maps each kind of personal data to a tag:

| Kind | Tag |
|---|---|
| `email`, `phone`, `name`, `address`, `birth_date`, `network_id` | `pii` |
| `gov_id` | `confidential` |
| `financial` | `financial` |

Pass `kind_to_tag` to use your own tags. A kind you leave out is never suggested:

```python
suggest(client, "stg_customers", kind_to_tag={"email": "contact", "gov_id": "restricted"})
```

Every tag needs a strategy in [`[mask]`](/guides/governance/#map-tags-to-masking-strategies), or an entry in `[classifications]`. Otherwise `rocky compliance` reports the tag as a gap.

## Use a local copy of the model

A site without access to `huggingface.co` can copy the weights and point the model at them. `LayaClassifier` passes its keyword arguments to `laya.Router`:

```python
from rocky_sdk.classify import LayaClassifier

model = LayaClassifier(models={"typed-decisions": "/opt/models/laya-typed-decisions"})
suggest(client, "stg_customers", use_model=True, model_classifier=model)
```

The aid pins Hub revision `7b928d828b7b0e022f929d9bd2e44165aa270148`, the one it was measured on. A different revision or checkpoint has unmeasured accuracy.
