# Simple Validation Workflow Example

This example demonstrates a minimal four-node workflow that highlights the two-tier queue mechanics in Graft without the complex branching of the document pipeline. The flow is:

```
intake_node → validator_node → [repair_node?] → processor_node
```

Run it with:

```
cd examples/simple-validation
go run ./...
```

You should see two workflows complete:

- One document that requires a repair roundtrip before passing validation.
- One document that passes validation on the first attempt.

Each node appends to the document history so the final completion log shows the exact execution order.
