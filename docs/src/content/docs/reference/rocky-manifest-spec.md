---
title: "rocky-manifest v0.1"
description: An open, offline-verifiable format for attesting which program, over which inputs, in which environment produced a table version
sidebar:
  order: 11
---

:::caution[Draft / experimental]
`rocky-manifest v0.1` is a draft. The field names and shape may change before a stable release. This is published as an open format with a reference verifier so it can be reviewed and used early, not as a finished standard. Feedback is welcome.
:::

## What this is

A `rocky-manifest` is a small JSON document that attests what produced a table version: which exact program ran, over which inputs, in which environment, and (on a content-addressed write path) the byte-hashes of the output files. It is designed to be checked offline by anyone, using a tiny tool rather than the engine that produced it.

The format is deliberately vendor-neutral. The identity fields are named for what they mean (`program_hash`, `inputs_hash`, `env_hash`) rather than for any one producer, and a `producer` object names the tool and version that wrote the manifest. The goal is an open format with a reference implementation, not a proprietary metadata blob.

The reference verifier is [`rocky-verify`](#the-reference-verifier), a standalone tool that needs no Rocky engine installed.

## The identity it carries

Rocky records a recipe-identity triple on every successful model execution. A manifest is that triple, plus the scheme tag and carrier metadata, serialized into an open shape:

- **`program_hash`** — a BLAKE3 fingerprint of the model's canonical typed representation. It is the identity of the exact program. Two logically distinct programs have different program hashes; the same program has the same hash no matter when or how often it ran.
- **`inputs_hash`** — a BLAKE3 over the inputs the run actually observed. It is present only when the run observed its inputs. On a run that observes nothing, the declared inputs are already folded into `program_hash`, so a bare "no inputs" hash would add nothing and is omitted.
- **`inputs_proof_class`** — the strength of `inputs_hash`, either `strong` or `heuristic`. `strong` means every observed upstream was attested by a content hash, so the input side is byte-verifiable. `heuristic` means at least one upstream was attested by a freshness signal such as a watermark or row count, which attests freshness rather than byte-identity. This label exists so a weak input hash is never read as a content claim. It is present exactly when `inputs_hash` is.
- **`env_hash`** — a BLAKE3 over the environment semantics: the producing tool's version and the adapter or dialect identity. It excludes machine identity by construction, so two runs on different hosts in the same logical environment share an `env_hash`.
- **`hash_scheme`** — the tag naming the canonicalisation-and-hashing algorithm family in force, `v1` at this version. A future canonicalisation change becomes an explicit new scheme value rather than a silent fork of recorded history.

Every hash is lowercase hex over 32 BLAKE3 bytes, so each identity field is exactly 64 hex characters.

## Versioning

`manifest_version` is a constant `"0.1"` at this version. A consumer reads it first and refuses a version it does not understand. `hash_scheme` versions the hashing algorithm family independently of the document shape, so the two can evolve on their own schedules.

## The document shape

A minimal manifest from a general run, where the input side was observed through a freshness signal:

```json
{
  "manifest_version": "0.1",
  "hash_scheme": "v1",
  "producer": { "name": "rocky", "version": "1.56.0" },
  "subject": {
    "model": "orders_clean",
    "run_id": "run-20260708-015241-256",
    "produced_at": "2026-07-08T01:52:41.274162Z",
    "status": "success"
  },
  "program_hash": "2148e619b51421f51cfb3fac423145fe245bbe409b74f31c22d703ab07453036",
  "inputs_hash": "75ee2a328c52b2361aec2c1649cfd774e14ebbeb3f7e0bd99805e8e42c21b9c6",
  "inputs_proof_class": "heuristic",
  "env_hash": "bbf2c2045328f738683a6336df5fa61b5f00076aeaed7c495c04f9d6991d92bd"
}
```

On a content-addressed write path, the manifest also carries the output byte-hashes, which are what make the output half byte-verifiable offline:

```json
{
  "manifest_version": "0.1",
  "hash_scheme": "v1",
  "producer": { "name": "rocky", "version": "1.56.0" },
  "subject": { "model": "fct_revenue", "run_id": "run_2026-05-30T11-04-22Z_8f1a", "status": "success" },
  "program_hash": "2148e619b51421f51cfb3fac423145fe245bbe409b74f31c22d703ab07453036",
  "inputs_hash": "75ee2a328c52b2361aec2c1649cfd774e14ebbeb3f7e0bd99805e8e42c21b9c6",
  "inputs_proof_class": "strong",
  "env_hash": "bbf2c2045328f738683a6336df5fa61b5f00076aeaed7c495c04f9d6991d92bd",
  "output_hashes": [
    { "hash": "736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0", "path": "736713a2611f762af09ee4445c09157bcfdbf6e07145dd8edf2cfd203d8d5bf0.parquet" }
  ]
}
```

### Fields

| Field | Required | Meaning |
|---|---|---|
| `manifest_version` | yes | The spec version this document conforms to. `"0.1"`. |
| `hash_scheme` | yes | The hashing-algorithm-family tag in force. `"v1"`. |
| `producer.name` | yes | The producing tool's name, for example `"rocky"`. |
| `producer.version` | yes | The producing tool's version string. |
| `subject.model` | yes | The model or target whose production this attests. |
| `subject.run_id` | no | The producer's run identifier, for cross-referencing its run ledger. |
| `subject.produced_at` | no | When the production ran (RFC 3339). |
| `subject.status` | no | The recorded execution status, for example `"success"`. |
| `program_hash` | yes | The program-identity key (64-hex BLAKE3). |
| `inputs_hash` | no | The input-match key. Present only when inputs were observed. |
| `inputs_proof_class` | no | Strength of `inputs_hash`: `strong` or `heuristic`. Present exactly when `inputs_hash` is. |
| `env_hash` | yes | The environment key (64-hex BLAKE3). |
| `output_hashes` | no | Byte-hashes of output artifact files, recorded on a content-addressed write path. |

The canonical machine-readable definition is the JSON Schema (draft 2020-12) at `engine/crates/rocky-verify/spec/rocky-manifest-v0.1.schema.json`. It is a standalone schema, deliberately kept out of the CLI's JSON-output schema set: it describes the open format, not a command's wire output. Print it with `rocky-verify schema`.

### The honesty invariant

`inputs_hash` and `inputs_proof_class` travel together. A manifest with one but not the other is invalid, and the schema enforces this in both directions. The point is that no consumer can be handed an input hash whose strength is unstated, and no strength label can float without the hash it describes.

## What each field maps to in a run

Every field corresponds to something the engine records today. Nothing here is aspirational.

| Manifest field | Where it comes from |
|---|---|
| `hash_scheme` | The recorded hash-scheme tag on the execution. |
| `producer.name` / `producer.version` | The producing engine and the version it reports. |
| `subject.model` / `run_id` / `produced_at` / `status` | The model execution's own record within its run. |
| `program_hash` | The recorded recipe-identity program key. |
| `inputs_hash` / `inputs_proof_class` | The recorded input-match key and its proof class, populated when the run observed its inputs. |
| `env_hash` | The recorded environment key. |
| `output_hashes` | The recorded output BLAKE3(s), on the content-addressed write path only. |

The recipe-identity triple is surfaced directly by `rocky history --recipe <program_hash> --output json`, and on every model record of `rocky history` and `rocky trace`. A manifest is that surfaced identity, reshaped into the open format. The content-addressed output hashes come from the content-addressed write path, which names each output file by the BLAKE3 of its bytes; see [Verify a run](/guides/verify-a-run/) for how that path records and exposes them.

## Carriers

The same manifest content can travel in more than one place:

1. **A standalone file.** A `.json` document, suitable for an export bundle handed to an auditor or a downstream consumer. This is the form the reference verifier reads.
2. **Alongside content-addressed artifacts.** On the content-addressed write path, the output bytes are already named by their BLAKE3 hash and recorded in the artifact ledger, which is what populates `output_hashes`.
3. **Table-format metadata.** Embedding the identity in a table's own warehouse-side metadata is a natural carrier, so the attestation travels with the table. Rocky writes it into Delta `TBLPROPERTIES`: each manifest field is carried under a vendor-neutral namespace prefix, `recipe_manifest.`, with the field's dotted path appended, for example `recipe_manifest.program_hash`, `recipe_manifest.env_hash`, `recipe_manifest.hash_scheme`, `recipe_manifest.inputs_hash`, `recipe_manifest.inputs_proof_class`, `recipe_manifest.manifest_version`, `recipe_manifest.producer.name`, `recipe_manifest.producer.version`, `recipe_manifest.subject.model`, `recipe_manifest.subject.run_id`, `recipe_manifest.subject.status`. The prefix is deliberately not a vendor brand such as `rocky.`: vendor identity belongs in the `producer` field, not the key, and a shared neutral namespace lets a reader glob the manifest keys and tell them apart from `delta.*` reserved properties and arbitrary user tags. The write is issued as a post-create `ALTER TABLE ... SET TBLPROPERTIES`, never folded into the CREATE, so it cannot perturb the IR the program hash is computed over.

   A reader reverses that mapping — strip the prefix, split the remaining dotted path — to reconstitute a standalone manifest that `rocky-verify` checks offline. Note the boundary: a managed (non-content-addressed) run carries the identity triple but no `output_hashes`, so offline verification of a table-metadata manifest is schema validation only. It proves the carrier round-tripped and the triple is well-formed, not that the table's bytes are reproducible. Byte-verifiable teeth still live only in `output_hashes` on the content-addressed path.

   The Delta `TBLPROPERTIES` carrier is where this ships today. Managed Iceberg rejects engine-managed property writes (`MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`), so the documented fallback for an Iceberg table is the snapshot-summary carrier, which remains future work.

## The reference verifier

`rocky-verify` is a standalone tool that verifies a manifest with no Rocky engine installed. It does two independent things:

```bash
# Structure only: validate against the v0.1 schema.
rocky-verify verify manifest.json

# Structure plus bytes: also hash any output artifacts and compare.
rocky-verify verify manifest.json --artifacts-dir ./artifacts

# Print the embedded schema.
rocky-verify schema
```

**Schema validation** checks that the document is well-formed: required fields are present, hashes are 64-hex, `inputs_proof_class` is one of the two allowed values, the input-hash-and-proof-class invariant holds, and no unknown fields are present.

**Byte verification** is the offline content check. When the manifest carries `output_hashes` and you point at the directory holding the artifact files, the verifier hashes each file with BLAKE3 and confirms it equals the recorded hash. This is pure byte arithmetic. It catches a manifest whose recorded output hash no longer matches the bytes it claims to describe, without trusting the manifest or any producing tool.

The verifier exits `0` when a manifest verifies, `1` when it fails verification, and `2` when the manifest cannot be read or parsed.

## What a manifest attests, and what it does not

A manifest attests the identity of the program that ran, the inputs it observed (at the stated strength), and the environment it ran in. On a content-addressed path it also attests that the recorded output bytes are exactly those bytes.

It does not attest that re-running the program would reproduce the same output. Non-deterministic SQL, session settings, and user-defined functions can all diverge under an identical `program_hash`. A `heuristic` input proof attests freshness, not byte-identity, and must never be read as a content claim. These boundaries are the same ones the [Verify a run](/guides/verify-a-run/) guide draws for the underlying records.

## Implementation status

| Claim | Status |
|---|---|
| The recipe-identity triple (`program_hash`, `env_hash`, `hash_scheme`) recorded on every successful execution | Shipped |
| `inputs_hash` + `inputs_proof_class` recorded when the run observes its inputs | Shipped |
| Manifest derivable from `rocky history --recipe --output json` | Shipped |
| `rocky-verify` schema validation and offline byte verification | Shipped |
| `output_hashes` populated for a general (non-content-addressed) run | Not yet — output byte-hashes are recorded on the content-addressed write path only |
| Table-format metadata carrier (Delta `TBLPROPERTIES` under the `recipe_manifest.` namespace) | Shipped for Delta — reconstitutes to a manifest `rocky-verify` validates offline (schema-only on a managed run) |
| Table-format metadata carrier for Iceberg (snapshot-summary) | Not yet — managed Iceberg rejects engine-managed property writes; snapshot-summary carrier is future work |
| Signature block for signed custody | Not yet — intentionally unspecified until the signing model lands |
