import os
import sys
import csv
import json
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector


def load_text_mapping(csv_path: str):
    """Load the mapping of image filenames to expected text from index.csv."""
    mapping = {}

    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping[row['Filename']] = row['Text']
    else:
        print(f"CSV file {csv_path} not found, using empty mapping")

    print(f"Loaded {len(mapping)} image text mappings: {mapping}")
    return mapping


def create_test_images(client, image_dir):
    """Create test images from the directory."""
    print(f"Creating test images from {image_dir}...")

    assert os.path.exists(image_dir), f"Directory {image_dir} does not exist"
    basename = os.path.basename(image_dir)
    image_text_mapping = load_text_mapping(os.path.join(image_dir, "index.csv"))

    query = []
    blobs = []
    image_files = [f for f in os.listdir(
        image_dir) if f.lower().endswith(('.jpeg', '.jpg', '.png'))]

    for image_file in image_files:
        image_path = os.path.join(image_dir, image_file)

        try:
            # Read the image file
            with open(image_path, 'rb') as f:
                image_data = f.read()

            # Get expected text for this image
            assert image_file in image_text_mapping, f"Missing text mapping for {image_file}"
            expected_text = image_text_mapping.get(image_file, "")

            # Add image to database
            query.append({
                "AddImage": {
                    "properties": {
                        "filename": image_file,
                        "expected_text": expected_text,
                        "corpus": basename,
                    }
                }
            })

            blobs.append(image_data)

        except Exception as e:
            print(f"Error processing image {image_file}: {e}")
            continue

    if query:
        status, response, _ = execute_query(client, query, blobs)
        print(f"Created {len(image_files)} test images successfully.")
    else:
        print(
            f"No images were created. Status: {status}, Response: {response}")
        sys.exit(1)


def load_pdf_text_mapping(csv_path: str):
    """Load the mapping of PDF filenames to expected text from index.csv."""
    mapping = {}

    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping[row['Filename']] = row['Text']
    else:
        print(f"CSV file {csv_path} not found, using empty mapping")

    print(f"Loaded {len(mapping)} PDF text mappings: {mapping}")
    return mapping





def create_test_pdfs(client, pdf_dir):
    """Create test PDFs with expected text from the specified directory."""
    print(f"Creating {pdf_type} PDFs from {pdf_dir}...")

    query = []
    blobs = []
    pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith('.pdf')]
    basename = os.path.basename(pdf_dir)
    text_mapping = load_pdf_text_mapping(os.path.join(pdf_dir, "index.csv"))

    for pdf_file in pdf_files:
        pdf_path = os.path.join(pdf_dir, pdf_file)

        try:
            # Read the PDF file
            with open(pdf_path, 'rb') as f:
                pdf_data = f.read()

            # Get expected text for this PDF
            expected_text = text_mapping.get(pdf_file, "")

            # Add PDF blob to database with expected text
            query.append({
                "AddBlob": {
                    "properties": {
                        "document_type": "pdf",
                        "content_type": "application/pdf",
                        "filename": pdf_file,
                        "expected_text": expected_text,
                        "corpus": basename,
                    }
                }
            })
            blobs.append(pdf_data)

        except Exception as e:
            print(f"Error processing PDF {pdf_file}: {e}")
            continue

    if query:
        status, response, _ = execute_query(client, query, blobs)
        print(f"Created {len(pdf_files)} {pdf_type} PDFs successfully.")
    else:
        print(f"No PDFs were created from {pdf_dir}")


def print_schema(client):
    """Print the schema of the database."""
    print("Printing schema of the database...")
    query = [{"GetSchema": {}}]
    status, response, _ = execute_query(client, query)
    assert status == 0, f"Failed to get schema: {response}"
    print(json.dumps(response, indent=2))


def db_connection():
    """Create a database connection."""
    DB_HOST = os.getenv("DB_HOST", "aperturedb")
    DB_PORT = int(os.getenv("DB_PORT", "55555"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    return Connector(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASS)


def main():
    """Main seeding function."""
    print("Starting embeddings-extraction test data seeding...")
    client = db_connection()

    try:
        # Create test images from actual files
        create_test_images(client, "/app/images/signs")
        create_test_images(client, "/app/images/documents")

        # Create test PDFs from actual files
        create_test_pdfs(client, "/app/pdfs/text")
        create_test_pdfs(client, "/app/pdfs/images")

        print("Seeding completed successfully!")

        print_schema(client)

    except Exception as e:
        print(f"Error during seeding: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
