# Ingest From Buckets Testing

The testing for ingest from buckets relies on a consistent set of objects being
in buckets in order to facilitate the tests.

The items are automatically generated, when possible.

## Images
Images are created by generateImages.py, the size, text, color and format are
adjustable. We select the defaults and create 5 directories of 1500 images in
order to ensure the loaders can load directories over over 1000, and return
queries over 5000.

## Videos
We generate videos using ffmpeg. The input images are from the images we
generate from the image testing. We create 5 vidoes, 1 which is longer than the
others, to ensure longer videos work fine.

## PDFs
We use the python lorem-text library to generate random paragraphs of text, then
vim to convert the text to a postscript file, and finally ps2pdf to create pdfs.

## Creating Workflow test environment

The data is structured that you should go in each directory and run something
like
```
( ../aws.keys && bash upload.sh -p s3 -b wf-ingest-from-bucket-test-data )
( ../gs.key && bash upload.sh -p gs -b wf-ingest-from-bucket-test-data )
```

where aws.keys looks like:
```
AWS_ACCESS_KEY_ID="BLAH"
AWS_SECRETE_ACCESS_KEY="BLAH"
```
and gs.key:
```
CRED='json_here'
TOKEN_FILE=$(mktemp /tmp/gcp-auth.XXXXXX.json)
exec 3>"$TOKEN_FILE"

echo "$CREDS" >&3
exec 3>&-
GOOGLE_APPLICATION_CREDENTIALS="$TOKEN_FILE"
```

this is done to make it easy to change how the tests work - just change what
bucket you load to and then you can work on it without impariling the github
action that runs when people go to checkin.
