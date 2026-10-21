"""Seed detector tests with the same SDK as the workflows under test."""
from contextlib import closing
import os
from pathlib import Path

from aperturedb.Connector import Connector
from aperturedb.Utils import Utils


count = int(os.environ["TOTAL_IMAGES"])
image = Path(__file__).with_name("sample.jpg").read_bytes()
with closing(Connector(host=os.environ["DB_HOST"],
                       port=int(os.environ["DB_PORT"]),
                       user="admin", password=os.environ["DB_PASS"],
                       ca_cert=os.environ["CA_CERT"],
                       retry_max_attempts=30)) as db:
    for offset in range(0, count, 10):
        commands = [{"AddImage": {"properties": {"img_id": index}}}
                    for index in range(offset, min(offset + 10, count))]
        response, _ = db.query(commands, [image] * len(commands))
        if not db.last_query_ok():
            raise RuntimeError(f"Image seeding failed: {response}")
    response, _ = db.query([{"FindImage": {"results": {"count": True}}}])
    assert response[0]["FindImage"]["count"] == count, response
    Utils(db).summary()
print(f"Seeded {count} images")
