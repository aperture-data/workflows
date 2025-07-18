#! /usr/bin/env python3
# python test.py http://garfield:8000/rag/ask query

# This is an attempt to test the RAG system.
# It is not a good test, but it is a start.
# Specifically, it has enumerated all the questions that we want to ask the RAG system,
# for which we have a list of pages that we expect to be in the response.

# The scoring is based on the overlap of RAG response with the list of pages that we expect to be in the response.

import requests
import json
import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if len(sys.argv) > 1:
    URL = sys.argv[1]
    if len(sys.argv) > 2:
        query_field = sys.argv[2]
    else:
        query_field = "question"
else:
    URL = "http://garfield:81/ask"
    query_field = "question"

auth_header = {
    "Authorization": "Bearer " + os.environ.get("CB_SECRET", "secret")
}

if __name__ == "__main__":
    url = URL
    tests = [
        {
            "question": "in which year was aperturedb created?",
            "hallucination": 1,
            "conciseness": 0,
            "should_have": [
                "Bail out. not defined in the docs"
            ]
        },
        {
            "question": "can it draw keypoint annonation on image?",
            "hallucination": 0,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/python_sdk/object_wrappers/Images#display",
            ]
        },
        {
            "question": "how do i disable DEBUG messages from python sdk?",
            "hallucination": 2,
            "conciseness": 0
        },
        {
            "question": "How can I empty the content in the database?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/python_sdk/helpers/Utils#remove_all_objects",
            ]
        },
        {
            "question": "Does ApertureDB support JSON as a property value?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "A page that talks about types of properties",
            ]
        },
        {
            "question": "can I pass a reference to if_not_found?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "A page saying We cannot pass a reference to if_not_found",
            ]
        },
        {
            "question": "what distance metrics does aperturedb support?",
            "should_have": [
                "https://docs.aperturedata.io/query_language/Reference/descriptor_commands/desc_set_commands/AddDescriptorSet#parameters",
            ],
            "hallucination": 2,
            "conciseness": 2
        },
        {
            "question": "Hi there - I just got charged for ApertureDB - i think i didnt cancel my subscripton. I never used it - would it be possible to refund it and cancel my account? its for the user team@aperturedata.io",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "A page that says we cannot refund the subscription",
            ]
        },
        {
            "question": "Is ApertureDB a vector database?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/concepts/vectordb"
            ]
        },
        {
            "question": "What can I do with ApertureDB?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/Introduction/WhyAperture",
            ]
        },
        {
            "question": "What are the recommended ways to populate the database?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/HowToGuides/Ingestion/Ingestion/Ingestion"
            ]
        },
        {
            "question": "How do I retrieve a part of the video?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/query_language/Reference/video_commands/clip_commands/AddClip#examples",
                "https://docs.aperturedata.io/query_language/Reference/video_commands/clip_commands/FindClip#examples",
                "https://docs.aperturedata.io/query_language/Reference/video_commands/frame_commands/FindFrame#examples",
                "https://docs.aperturedata.io/query_language/Reference/video_commands/frame_commands/AddFrame#examples",
                "https://docs.aperturedata.io/query_language/Reference/video_commands/frame_commands/UpdateFrame#examples",
            ]
        },
        {
            "question": "How do I retrieve a resized image which is 100x100 pixels?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/query_language/Reference/shared_command_parameters/operations",
            ]
        },
        {
            "question": "How do I retrieve an image rotated by 45 degrees?",
            "should_have": [
                "https://docs.aperturedata.io/python_sdk/object_wrappers/Images#rotate",
                "https://docs.aperturedata.io/query_language/Reference/image_commands/image_commands/AddImage#examples",
                "https://docs.aperturedata.io/query_language/Reference/shared_command_parameters/operations"
            ],
            "hallucination": 2,
            "conciseness": 2
        },
        {
            "question": "Show me examples of using ApertureDB to train a model?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/HowToGuides/Advanced/tensorflow_training"
            ]
        },
        {
            "question": "How do I add a read-only user?",
            "hallucination": 2,
            "conciseness": 2,
            "should_have": [
                "https://docs.aperturedata.io/query_language/Reference/acl_commands/CreateUser",
                "https://docs.aperturedata.io/query_language/Overview/Access%20Control#__docusaurus"
        },
        {
            "question": "How can i update metadata associated with a video?",
            "should_have": [
                "https://docs.aperturedata.io/HowToGuides/start/Videos#update-properties-of-the-video-already-in-aperturedb"
            ],
            "hallucination": 2,
            "conciseness": 2
        },
    ]
    responses = []
    for test in tests:
        # response = requests.post(url, json={query_field: test["question"]})
        payload = {query_field: test["question"]}
        logger.info(f"Sending request to {url} with payload {payload}")
        response = requests.post(url, json=payload, headers=auth_header)
        if response.status_code != 200:
            logger.error(f"Request failed with status code {response.status_code}")
            logger.error(f"Response: {response.text}")
            continue
        response = response.json()
        logger.info(f"Response: {response}")
        responses.append(response)
        print(f"Test run for question: {test['question']}")

    with open("responses.json", "w") as f:
        f.write(json.dumps(responses, indent=2))
