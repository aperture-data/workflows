# OpenCode Agent Instructions

This repository contains ApertureDB workflows packaged as Docker images. It uses a monorepo-style structure with multiple applications located in the `apps/` directory, sharing common build scripts and base images.

## Architecture & Boundaries
- `apps/`: Contains individual workflow projects (e.g., `embeddings-extraction`, `rag`, `jupyterlab`). Each acts as an independent Docker build but relies on root scripts.
- `base/docker/`: Contains the base Ubuntu+Python Docker image that individual workflows build upon.
- `docker-compose.yml` (at root): Provides shared services for testing, notably the `aperturedb` database, the `lenz` gateway in front of it, and the `ca` cert generator.

## Testing & Execution
**Do not run `pytest` or python execution scripts directly on the host.** Workflows depend on complex interactions with the database and object storage. Execution and testing are entirely orchestrated via Docker Compose.

To test a specific app:
```bash
cd apps/<workflow-name>
./test.sh
```

What `test.sh` does (orchestrated via root `.commonrc`):
1. Builds the `base` image and the app image via `compose.sh`.
2. Starts `aperturedb` and `lenz` (ApertureDB gateway) locally.
3. Frequently runs a `seed` container to populate synthetic data (see `apps/<workflow-name>/test/docker-compose.yml`).
4. Executes the main workflow container.
5. Runs a `tests` container (which executes `pytest` internally) and reports success/failure based on the exit code.

Test logs are captured and written to `apps/<workflow-name>/test.log`. Review this file if a test suite fails.

*Note: If an app lacks a `test.sh`, the CI pipeline falls back to executing `../build.sh` to ensure the Docker image successfully builds.*

## Building Images
If you only need to build the Docker image for an app without running tests:
```bash
apps/build.sh <workflow-name>
# or from the app directory:
cd apps/<workflow-name> && ../build.sh
```

To build the shared base image:
```bash
cd base/docker && bash build.sh
```

## Quirks & Conventions
- **Docker Compose Dependencies:** Test execution relies on strict `depends_on` sequencing (`lenz` -> `seed` -> `workflow-app` -> `tests`). If you modify an app's testing compose file, ensure lifecycle conditions like `service_completed_successfully` are maintained so containers do not execute prematurely.
- **Environment Variables:** Workflows expect ApertureDB connection/auth variables such as `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `CA_CERT`, and optionally `APERTUREDB_KEY`. During local tests, `DB_HOST` resolves to the `lenz` service.
- **Code Standards:** There are no repo-wide linting, formatting, or pre-commit hooks enforced. Conform strictly to the style of the file you are modifying.