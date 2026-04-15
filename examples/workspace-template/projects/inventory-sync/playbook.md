# Inventory Sync Playbook

## Service role

- Accept upstream inventory change events.
- Normalize and route them to downstream systems.

## Investigation order

1. Confirm whether the event was produced upstream.
2. Check the relational record or dedupe marker.
3. Inspect the routing code path.
4. Check queue lag or failed deliveries.
