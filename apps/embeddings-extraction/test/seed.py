import os
import sys
import csv
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector

def load_image_text_mapping():
    """Load the mapping of image filenames to expected text from index.csv."""
    csv_path = "/app/photos_with_text/index.csv"
    mapping = {}
    
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping[row['Filename']] = row['Text']
    
    return mapping

def create_test_images(client):
    """Create test images from the photos_with_text directory."""
    print("Creating test images from photos_with_text directory...")
    
    image_dir = "/app/photos_with_text"
    image_text_mapping = load_image_text_mapping()
    
    if not os.path.exists(image_dir):
        print(f"Image directory {image_dir} not found, skipping image creation")
        return
    
    query = []
    blobs = []
    image_files = [f for f in os.listdir(image_dir) if f.lower().endswith(('.jpeg', '.jpg', '.png'))]
    
    for image_file in image_files:
        image_path = os.path.join(image_dir, image_file)
        
        try:
            # Read the image file
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            # Get expected text for this image
            expected_text = image_text_mapping.get(image_file, "")
            
            # Add image to database
            query.append({
                "AddImage": {
                    "properties": {
                        "name": image_file,
                        "expected_text": expected_text,
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
        print(f"No images were created. Status: {status}, Response: {response}")
        sys.exit(1)

def create_test_pdfs(client):
    """Create test PDFs from the pdfs directory."""
    print("Creating test PDFs from pdfs directory...")
    
    pdf_dir = "/app/pdfs"
    
    if not os.path.exists(pdf_dir):
        print(f"PDF directory {pdf_dir} not found, skipping PDF creation")
        return
    
    query = []
    blobs = []
    pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith('.pdf')]
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(pdf_dir, pdf_file)
        
        try:
            # Read the PDF file
            with open(pdf_path, 'rb') as f:
                pdf_data = f.read()
            
            # Add PDF blob to database
            query.append({
                "AddBlob": {
                    "data": base64.b64encode(pdf_data).decode('utf-8'),
                    "properties": {
                        "document_type": "pdf",
                        "content_type": "application/pdf",
                        "filename": pdf_file,
                    }
                }
            })
            blobs.append(pdf_data)
            
        except Exception as e:
            print(f"Error processing PDF {pdf_file}: {e}")
            continue
    
    if query:
        status, response, _ = execute_query(client, query, blobs)
        print(f"Created {len(pdf_files)} test PDFs successfully.")
    else:
        print(f"No PDFs were created. Status: {status}, Response: {response}")
        sys.exit(1)


def main():
    """Main seeding function."""
    print("Starting embeddings-extraction test data seeding...")
    
    DB_HOST = os.getenv("DB_HOST", "aperturedb")
    DB_PORT = int(os.getenv("DB_PORT", "55555"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    print(f"{DB_HOST=}, {DB_PORT=}, {DB_USER=}, {DB_PASS}")
    client = Connector(host=DB_HOST, user=DB_USER,
                       port=DB_PORT, password=DB_PASS)

    try:
        # Create test images from actual files
        create_test_images(db)
        
        # Create test PDFs from actual files
        create_test_pdfs(db)
        
        print("Seeding completed successfully!")
        
    except Exception as e:
        print(f"Error during seeding: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
