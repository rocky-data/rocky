# Multi-Layer Pipeline

A medallion architecture (Bronze, Silver, Gold) pipeline with data contracts.

## Architecture

```
Bronze (raw)          Silver (cleaned)           Gold (business)
  |                     |                          |
  raw_events.sql  -->  stg_events.rocky   -+-->  fct_user_activity.rocky
                       stg_users.rocky    -+       |
                                                   contract enforced
```

### Bronze Layer
Raw data ingestion. Plain SQL, full refresh. No transformations -- just a faithful copy of the source.

### Silver Layer
Cleaned and standardized data. Rocky DSL for NULL-safe filtering and column normalization. Removes invalid records, renames columns, adds computed fields.

### Gold Layer
Business-ready aggregations. Joins silver models, applies business logic. Protected by a data contract that enforces required columns and types.

## Project Structure

```
multi-layer/
  rocky.toml
  models/
    bronze/
      raw_events.sql             # Source ingestion
      raw_events.toml
    silver/
      stg_events.rocky           # Clean events
      stg_events.toml
      stg_users.rocky            # Clean users
      stg_users.toml
    gold/
      fct_user_activity.rocky    # Business aggregation
      fct_user_activity.toml
  contracts/
    fct_user_activity.contract.toml   # Data contract
```

## Data Contract

The `fct_user_activity.contract.toml` enforces:
- **Required columns** -- `user_id`, `total_events`, `first_event_date`, `last_event_date` must exist
- **Protected columns** -- `user_id` cannot be removed or renamed
- **Type constraints** -- `user_id` is `Int64` (not nullable), `total_events` is `Int64` (not nullable)

If a model change would violate the contract, `rocky plan` fails with an error before any SQL is executed.

## Running

```bash
# Validate contract compliance
rocky plan --config rocky.toml

# Execute all layers in dependency order
rocky run --config rocky.toml

# Check stored watermarks
rocky state --config rocky.toml
```
