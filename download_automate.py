from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import shutil
import getpass
import pandas as pd

df = pd.read_csv('./failed_downloads2.csv')  # Assuming failed_downloads2.csv contains the URLs and paths

# List of URLs to visit
urls = df['link'].tolist()   

# Corresponding paths to save the downloaded files
save_paths = df['attachments_folder'].tolist()

# Get user's default download directory dynamically
username = getpass.getuser()
download_dir = f"C:\\Users\\Vedant.desai\\Downloads"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

driver_path = "C:/Users/Vedant.desai/Downloads/edgedriver_win64/msedgedriver.exe"



# Setup Edge options
edge_options = Options()
edge_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

service = Service(driver_path)

# Start Edge browser
driver = webdriver.Edge(service=service, options=edge_options)

def wait_for_download(directory, timeout=60, poll_frequency=0.5):
    """Wait until a new file is downloaded and fully saved."""
    start_time = time.time()
    initial_files = set(os.listdir(directory))

    while time.time() - start_time < timeout:
        current_files = set(os.listdir(directory))
        new_files = current_files - initial_files

        for file in new_files:
            file_path = os.path.join(directory, file)
            # Check if file is complete (no .crdownload extension and size stable)
            if not file.endswith((".crdownload", ".tmp")):
                # Wait for file size to stabilize
                for _ in range(3):
                    size1 = os.path.getsize(file_path)
                    time.sleep(0.5)
                    size2 = os.path.getsize(file_path)
                    if size1 == size2:
                        return file_path
        time.sleep(poll_frequency)

    raise TimeoutError("Download did not complete within the timeout period.")

def ensure_valid_path(path):
    """Ensure the path is valid and create directories if needed"""
    try:
        # Convert relative path to absolute path
        abs_path = os.path.abspath(path)
        # Create all necessary directories
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        return abs_path
    except Exception as e:
        print(f"Error creating directory structure for {path}: {e}")
        return None

try:
    for url, save_path in zip(urls, save_paths):
        # Skip if the save_path is invalid
        if not save_path or pd.isna(save_path):
            print(f"Skipping {url}: Invalid save path")
            continue

        # Ensure the save path is valid
        valid_save_path = ensure_valid_path(save_path)
        if not valid_save_path:
            print(f"Skipping {url}: Could not create directory structure")
            continue

        driver.get(url)

        try:
            downloaded_file = wait_for_download(download_dir)
            if downloaded_file and os.path.exists(downloaded_file):
                # Get Filename from downloaded_file
                filename = os.path.basename(downloaded_file)
                # Create the full destination path
                destination = os.path.join(valid_save_path, filename)
                
                # Ensure the destination directory exists one final time
                os.makedirs(os.path.dirname(destination), exist_ok=True)
                
                # Move the file
                shutil.move(downloaded_file, destination)
                print(f"✅ Saved {filename} to {valid_save_path}")
            else:
                print(f"❌ Download failed for {url}: File not found")
                
        except TimeoutError as e:
            print(f"❌ Failed to download from {url}: {e}")
        except Exception as e:
            print(f"❌ Error processing file from {url}: {e}")
            print(f"   Save path attempted: {valid_save_path}")

finally:
    driver.quit()