import os
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector

def db_connection():
    DB_HOST = os.getenv("DB_HOST", "lenz")
    DB_PORT = int(os.getenv("DB_PORT", "55551"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    CA_CERT = os.getenv("CA_CERT", None)
    return Connector(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASS, ca_cert=CA_CERT)

def test_caption_added():
    client = db_connection()
    query = [{
        "FindImage": {
            "constraints": {
                "filename": ["==", "test_red_square.jpg"]
            },
            "results": {
                "list": ["wf_caption_image_done", "wf_caption_image"]
            }
        }
    }]
    status, response, _ = execute_query(client, query)
    assert status == 0, f"Query failed: {response}"
    
    entities = response[0].get("FindImage", {}).get("entities", [])
    assert len(entities) > 0, "Image not found"
    
    props = entities[0]
    assert props.get("wf_caption_image_done") == True, f"Image not marked as done: {props}"
    assert "wf_caption_image" in props, f"Caption missing: {props}"
    print(f"Caption generated: {props['wf_caption_image']}")
