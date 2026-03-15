# Media Evidence Worker

Generic public extraction worker for async media evidence jobs.

## Strict Boundary

This repository must remain generic.

Allowed here:

- media extraction workflows
- open-source tooling integration
- generic payload schemas
- generic callback logic

This repository is intended to be triggered by a private control plane and return narrow extraction results through a signed callback.
