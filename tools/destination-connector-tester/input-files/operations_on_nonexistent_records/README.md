# operations_on_nonexistent_records

This folder contains examples where a source operation refers to a **record or table that does not exist** in the destination at execution time. In all such cases, the destination must **safely ignore the operation** — no data should be changed and no error should be returned.

### Scenarios covered:
- `UPDATE` on a missing record
- `DELETE` on a missing record
- `SOFT_DELETE` on a missing record
- `TRUNCATE` for a table that does not exist
- Operations (update/delete/soft_delete) issued **after a TRUNCATE** removed all rows

These examples demonstrate required no-op behavior for destination connectors.
