import os
import sys
import io
from PIL import Image
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector

def db_connection():
    DB_HOST = os.getenv("DB_HOST", "lenz")
    DB_PORT = int(os.getenv("DB_PORT", "55551"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    CA_CERT = os.getenv("CA_CERT", None)
    return Connector(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASS, ca_cert=CA_CERT)

def main():
    print("Starting caption-image test data seeding...")
    client = db_connection()

    try:
        # Create a simple test image
        img = Image.new('RGB', (100, 100), color = 'red')
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format='JPEG')
        img_data = img_byte_arr.getvalue()

        query = [{
            "AddImage": {
                "properties": {
                    "filename": "test_red_square.jpg"
                }
            }
        }]
        
        status, response, _ = execute_query(client, query, [img_data])
        if status != 0:
            print(f"Failed to create test image: {response}")
            sys.exit(1)
            
        print("Created test image successfully.")

    except Exception as e:
        print(f"Error during seeding: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
