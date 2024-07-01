import os
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from roboflow import Roboflow
from fastapi import FastAPI, BackgroundTasks
from dotenv import load_dotenv
import time
import logging

# Load environment variables from .env file
load_dotenv()

# Initialize Firebase with credentials from environment variables
cred = credentials.Certificate({
    "type": os.getenv("FIREBASE_TYPE"),
    "project_id": os.getenv("FIREBASE_PROJECT_ID"),
    "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
    "private_key": os.getenv("FIREBASE_PRIVATE_KEY").replace('\\n', '\n'),
    "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
    "client_id": os.getenv("FIREBASE_CLIENT_ID"),
    "auth_uri": os.getenv("FIREBASE_AUTH_URI"),
    "token_uri": os.getenv("FIREBASE_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("FIREBASE_AUTH_PROVIDER_CERT_URL"),
    "client_x509_cert_url": os.getenv("FIREBASE_CLIENT_CERT_URL")
})
firebase_admin.initialize_app(cred)

# Firestore client
db = firestore.client()

# List of Roboflow API keys
api_keys = [
    "QlP7I2XZtbSOwSXo5wUj",
    "wQLqqPPbH6g3cUXhvpBP",
    "ns98yTFDZdp87luZJ4dX",
    "KI0B8UpPCvzBvAbfcqJr",
    "Mi2Cn0Abmsj4pwsFwkyd",
    "nNwtxUK6clKQyPYC6gla"
]

# Initialize Roboflow objects and create projects
projects = []
for api_key in api_keys:
    rf = Roboflow(api_key=api_key)
    workspace = rf.workspace()
    project = workspace.create_project(
        project_name="Test-3",
        project_license="MIT",
        project_type="instance-segmentation",  # Valid project type
        annotation="instance-segmentation"  # Valid annotation type
    )
    projects.append(project)

# Define valid folders for images
folders = ["Front", "Left", "Rear", "Right"]

# Initialize a counter for circular distribution
project_counter = 0

# FastAPI app
app = FastAPI()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_and_upload_images():
    global project_counter
    
    while True:
        try:
            # Fetch all animal_officer documents
            animal_officer_docs = db.collection('animal_officer').stream()

            for doc in animal_officer_docs:
                officer_id = doc.id
                animals_ref = db.collection('animal_officer').document(officer_id).collection('animals')
                animals = animals_ref.stream()
                
                for animal in animals:
                    animal_data = animal.to_dict()
                    report_id = animal_data.get('reportId', 'unknown')
                    
                    image_paths = animal_data.get('imagePaths', {})
                    
                    for position, url in image_paths.items():
                        if position in folders:
                            response = requests.get(url)
                            if response.status_code == 200:
                                # Create an in-memory image file
                                image_name = f"{report_id}_{position}.jpg"
                                with open(image_name, 'wb') as img_file:
                                    img_file.write(response.content)
                                
                                # Upload the image to the current project in a circular manner
                                current_project = projects[project_counter % len(projects)]
                                current_project.upload(
                                    image_path=image_name,
                                    batch_name=position,  # Batch name corresponds to the folder
                                    split="train",
                                    num_retry_uploads=3,
                                    tag=position  # Tag corresponds to the folder (Front, Left, Rear, Right)
                                )
                                
                                # Remove the in-memory image file after upload
                                os.remove(image_name)
                                
                                logger.info(f"Uploaded {image_name} to Roboflow under batch {position} with tag {position}")
                                
                                # Update the project counter
                                project_counter += 1
                            else:
                                logger.error(f"Failed to download image from {url}")
            # Sleep for 5 minutes before checking again
            time.sleep(300)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            # Sleep for a short while before retrying in case of error
            time.sleep(60)

@app.on_event("startup")
def startup_event():
    background_tasks = BackgroundTasks()
    background_tasks.add_task(check_and_upload_images)
    logger.info("Started background task for checking and uploading images")

@app.get("/")
def read_root():
    return {"message": "Image upload service is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
