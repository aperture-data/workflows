# Workflows base

`Dockerfile` builds `aperturedata/workflows-base`, the image the applications in
this repository build on. It adds the system tools to Ubuntu and installs the
SDK and `aperturedb-workflows` wheels, which the aperturedb-python repository
publishes as `aperturedata/aperturedb-python` and
`aperturedata/aperturedb-workflows`. The image runs `wf-start`.

    bash build.sh

`build.sh` pulls both wheels with ORAS 1.3 and builds the image. `test.sh`
runs the checks in `test` against it.
