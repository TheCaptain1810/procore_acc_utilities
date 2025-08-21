import fitz  # PyMuPDF
import requests
import os
import mimetypes
import urllib.parse
import shutil
import glob
import string

def extract_hyperlinks_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    links = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        for link in page.get_links():
            uri = link.get("uri")
            if uri:
                links.append(uri)

    doc.close()
    return links

def download_files_from_links(links, download_folder):
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    for i, link in enumerate(links):
        try:
            response = requests.get(link, stream=True, timeout=10)
            if response.status_code == 200:
                # Try to get filename from Content-Disposition header
                content_disposition = response.headers.get('Content-Disposition')
                filename = None
                
                if content_disposition:
                    # Look for filename in content disposition
                    import re
                    if 'filename=' in content_disposition:
                        filename = re.findall("filename=(.+)", content_disposition)[0].strip('"')
                
                if not filename:
                    # Try to get filename and extension from Content-Type
                    content_type = response.headers.get('Content-Type', '').split(';')[0].strip()
                    ext = ''
                    if content_type:
                        # Get extension from mimetypes
                        guessed_ext = mimetypes.guess_extension(content_type)
                        if guessed_ext:
                            ext = guessed_ext
                    
                    # Try to get filename from URL
                    parsed_url = urllib.parse.urlparse(link)
                    url_filename = urllib.parse.unquote(os.path.basename(parsed_url.path))
                    if url_filename and '.' in url_filename:
                        filename = url_filename
                    else:
                        filename = f"file_{i}{ext}"

                # Clean the filename of any invalid characters
                filename = "".join(c for c in filename if c.isalnum() or c in "._- ")
                file_path = os.path.join(download_folder, filename)

                # Ensure we don't overwrite existing files
                base, ext = os.path.splitext(file_path)
                counter = 1
                while os.path.exists(file_path):
                    file_path = f"{base}_{counter}{ext}"
                    counter += 1

                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f"✅ Downloaded: {file_path}")
            else:
                print(f"⚠️ Skipped (status {response.status_code}): {link}")
        except Exception as e:
            print(f"❌ Error downloading {link}: {e}")

def sanitize_filename(filename, max_length=150):
    # Remove or replace invalid characters
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in filename if c in valid_chars)
    
    # Truncate if too long while preserving extension
    name, ext = os.path.splitext(filename)
    if len(filename) > max_length:
        return name[:(max_length-len(ext))] + ext
    return filename

def process_pdf_folder(source_folder, destination_folder):
    # Create destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Get all PDF files in the source folder
    pdf_files = glob.glob(os.path.join(source_folder, "*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {source_folder}")
        return

    print(f"Found {len(pdf_files)} PDF files to process")

    # Process each PDF file
    for pdf_file in pdf_files:
        try:
            print(f"\nProcessing PDF file: {pdf_file}")
            if not os.path.exists(pdf_file):
                print(f"Warning: PDF file no longer exists: {pdf_file}")
                continue
                
            # Get the PDF filename without extension
            pdf_basename = os.path.basename(pdf_file)
            pdf_name_without_ext = os.path.splitext(pdf_basename)[0]
            
            # Sanitize the folder name
            safe_folder_name = sanitize_filename(pdf_name_without_ext)
            
            # Create a folder for this PDF
            pdf_folder = os.path.join(destination_folder, safe_folder_name)
            
            # Handle case where folder might already exist
            counter = 1
            original_folder = pdf_folder
            while os.path.exists(pdf_folder):
                pdf_folder = f"{original_folder}_{counter}"
                counter += 1
            
            os.makedirs(pdf_folder)
            
            # Create attachments subfolder
            attachments_folder = os.path.join(pdf_folder, "attachments")
            os.makedirs(attachments_folder)
            
            # Copy the PDF file to its folder
            safe_pdf_name = sanitize_filename(pdf_basename)
            pdf_copy_path = os.path.join(pdf_folder, safe_pdf_name)
            shutil.copy2(pdf_file, pdf_copy_path)
            print(f"✅ Copied PDF: {pdf_copy_path}")
            
            # Extract and download attachments
            print(f"Processing attachments for: {pdf_basename}")
            hyperlinks = extract_hyperlinks_from_pdf(pdf_file)
            if hyperlinks:
                print(f"Found {len(hyperlinks)} links in {pdf_basename}")
                download_files_from_links(hyperlinks, attachments_folder)
            else:
                print(f"No links found in {pdf_basename}")
            
        except Exception as e:
            print(f"❌ Error processing {pdf_file}: {e}")

# Example usage
if __name__ == "__main__":
    source_folder = "./Transmittals"
    destination_folder = "./Transmittals_w_attachments"
    
    # Ensure paths are absolute
    source_folder = os.path.abspath(source_folder)
    destination_folder = os.path.abspath(destination_folder)
    
    print(f"Current working directory: {os.getcwd()}")
    print(f"Source folder absolute path: {source_folder}")
    print(f"Destination folder absolute path: {destination_folder}")
    
    if not os.path.exists(source_folder):
        print(f"Error: Source folder '{source_folder}' does not exist.")
        print("Available files and folders in current directory:")
        for item in os.listdir('.'):
            print(f"  - {item}")
    else:
        print(f"Source folder exists and contains:")
        try:
            for item in os.listdir(source_folder):
                print(f"  - {item}")
            process_pdf_folder(source_folder, destination_folder)
        except Exception as e:
            print(f"Error accessing source folder: {e}")
            print("Please make sure you're running the script from the correct directory and have access permissions.")


