import fitz  # PyMuPDF
import requests
import os
import mimetypes
import urllib.parse
import glob
import string
import pandas as pd  # For writing failed downloads CSV
 
def extract_hyperlinks_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    links = []
 
    for page_num in range(len(doc)):
        page = doc[page_num]
        for link in page.get_links(): # type: ignore
            uri = link.get("uri")
            if uri:
                links.append(uri)
 
    doc.close()
    return links
 
def download_files_from_links(links, download_folder):
    """Download files from a list of hyperlinks.

    Returns failed_downloads list with dict entries: {link, status_code, error}
    Successful downloads are not logged to CSV per current requirement.
    """
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    failed_downloads = []
    for i, link in enumerate(links):
        lower_link = link.lower()
        if lower_link.startswith('mailto:'):
            print(f"✉️  Skipping email link: {link}")
            continue
        if not lower_link.startswith(('http://', 'https://')):
            print(f"🔌 Skipping non-http(s) link: {link}")
            continue
        try:
            response = requests.get(link, stream=True, timeout=15)
            if response.status_code == 200:
                content_disposition = response.headers.get('Content-Disposition')
                filename = None
                if content_disposition:
                    import re
                    if 'filename=' in content_disposition:
                        filename = re.findall("filename=(.+)", content_disposition)[0].strip('"')
                if not filename:
                    content_type = response.headers.get('Content-Type', '').split(';')[0].strip()
                    ext = ''
                    if content_type:
                        guessed_ext = mimetypes.guess_extension(content_type)
                        if guessed_ext:
                            ext = guessed_ext
                    parsed_url = urllib.parse.urlparse(link)
                    url_filename = urllib.parse.unquote(os.path.basename(parsed_url.path))
                    if url_filename and '.' in url_filename:
                        filename = url_filename
                    else:
                        filename = f"file_{i}{ext}"
                filename = "".join(c for c in filename if c.isalnum() or c in "._- ")
                file_path = os.path.join(download_folder, filename)
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
                # For unauthorized (401) requests, skip without logging as a failure per new requirement
                if response.status_code == 401:
                    print(f"🔒 Skipping unauthorized (401) link (not logged to failures): {link}")
                else:
                    print(f"⚠️ Skipped (status {response.status_code}): {link}")
                    failed_downloads.append({'link': link, 'status_code': response.status_code, 'error': f'Status {response.status_code}'})
        except Exception as e:
            print(f"❌ Error downloading {link}: {e}")
            failed_downloads.append({'link': link, 'status_code': None, 'error': str(e)})
    return failed_downloads
 
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
    """Process all PDFs and download every attachment into a single common folder.

    destination_folder will contain only downloaded attachment files (no PDF copies).
    Failed downloads are logged with pdf_file, attachments_folder (common folder), link.
    """
    # Ensure common destination (attachments) folder exists
    os.makedirs(destination_folder, exist_ok=True)

    pdf_files = glob.glob(os.path.join(source_folder, "*.pdf"))
    if not pdf_files:
        print(f"No PDF files found in {source_folder}")
        return

    print(f"Found {len(pdf_files)} PDF files to process (common attachments folder: {destination_folder})")

    all_failed_downloads = []
    for pdf_file in pdf_files:
        print(f"\nProcessing PDF file: {pdf_file}")
        if not os.path.exists(pdf_file):
            print(f"Warning: PDF file no longer exists: {pdf_file}")
            continue
        pdf_basename = os.path.basename(pdf_file)
        # Extract & download attachments into the common destination folder
        try:
            hyperlinks = extract_hyperlinks_from_pdf(pdf_file)
            if hyperlinks:
                print(f"Found {len(hyperlinks)} links in {pdf_basename}")
                failed = download_files_from_links(hyperlinks, destination_folder)
                for f in failed:
                    f['pdf_file'] = pdf_basename
                    f['attachments_folder'] = destination_folder
                all_failed_downloads.extend(failed)
            else:
                print(f"No links found in {pdf_basename}")
        except Exception as e:
            print(f"❌ Error extracting/downloading links for {pdf_basename}: {e}")
    if all_failed_downloads:
        failed_csv = os.path.join(destination_folder, 'failed_downloads.csv')
        df = pd.DataFrame(all_failed_downloads, columns=['pdf_file', 'attachments_folder', 'link'])
        df.to_csv(failed_csv, index=False)
        print(f"\n❌ Failed downloads saved to: {failed_csv} (columns: pdf_file, attachments_folder, link)")
    else:
        print("\n✅ No failed downloads.")
 
# Example usage
if __name__ == "__main__":
    source_folder = "./Transmittals"
    # Use a single common folder for all PDFs and attachments
    destination_folder = "./Transmittals/attachments"
   
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
 
 
 