# Detector test images

The shared `add-image` Compose service seeds the face and object detector tests
with the workflow base image and its current SDK. It inserts `TOTAL_IMAGES`
images and checks both the count and the SDK schema summary before detectors
start.

`sample.jpg` is the existing 500 KB detector fixture from
`aperturedata/wf-add-image@sha256:44b01c680ec90ceb4aa5786bec0a56c4c6e2be0fb3f54790fc965594c2817c7e`.
Keeping the fixture here removes the dependency on that obsolete executable image.
