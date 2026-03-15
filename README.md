# Media Evidence Worker

Generic public extraction worker for async media evidence jobs.

## Strict Boundary

This repository must remain generic.

Allowed here:

- media extraction workflows
- open-source tooling integration
- generic payload schemas
- generic callback logic

Not allowed here:

- private application logic
- newsroom ranking logic
- Telegram logic
- Supabase service-role patterns
- internal prompts or business rules
- real secrets or environment values

This repository is intended to be triggered by a private control plane and return narrow extraction results through a signed callback.
