import fitz  # PyMuPDF
import requests
import os
import mimetypes
import urllib.parse
import glob
import string
import pandas as pd  # For writing failed downloads CSV
import sys
import atexit

# Mirror all console output to a log file in the current working directory
class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
            except Exception:
                # Never let logging disrupt normal output
                pass

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass

    def isatty(self):
        # Preserve TTY behavior for interactive environments
        try:
            return any(getattr(s, 'isatty', lambda: False)() for s in self._streams)
        except Exception:
            return False


_LOG_FILE = os.path.join(os.getcwd(), 'download.log')
try:
    _log_fp = open(_LOG_FILE, 'a', encoding='utf-8', buffering=1)
    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    sys.stdout = _Tee(_orig_stdout, _log_fp)
    sys.stderr = _Tee(_orig_stderr, _log_fp)

    def _close_logs():
        try:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr
        except Exception:
            pass
        try:
            _log_fp.flush()
            _log_fp.close()
        except Exception:
            pass

    atexit.register(_close_logs)
    print(f"📝 Logging enabled. Writing to: {_LOG_FILE}")
except Exception as _e:
    # If logging setup fails, continue without file logging
    print(f"⚠️ Could not initialize file logging: {_e}")
 
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
                # Log any non-200 status (including 401) as a failed download
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
    A summary CSV is created with file numbers, names, and attachment counts.
    """
    # Ensure common destination (attachments) folder exists
    os.makedirs(destination_folder, exist_ok=True)

    pdf_files = glob.glob(os.path.join(source_folder, "*.pdf"))
    if not pdf_files:
        print(f"No PDF files found in {source_folder}")
        return
        
    # List to store file information and attachment counts
    file_summary = []

    print(f"Found {len(pdf_files)} PDF files to process (common attachments folder: {destination_folder})")

    all_failed_downloads = []
    for i, pdf_file in enumerate(pdf_files):
        print(f"\nProcessing PDF file {i + 1}/{len(pdf_files)}: {pdf_file}")
        if not os.path.exists(pdf_file):
            print(f"Warning: PDF file no longer exists: {pdf_file}")
            continue
        pdf_basename = os.path.basename(pdf_file)
        # Extract & download attachments into the common destination folder
        try:
            hyperlinks = extract_hyperlinks_from_pdf(pdf_file)
            successful_downloads = 0
            if hyperlinks:
                print(f"Found {len(hyperlinks)} links in {pdf_basename}")
                attachments_subfolder = os.path.join(destination_folder, f"pdf_{i + 1}")
                failed = download_files_from_links(hyperlinks, attachments_subfolder)
                successful_downloads = len(hyperlinks) - len(failed)

                # After downloads, rename the attachments folder to the sanitized PDF name
                try:
                    if os.path.isdir(attachments_subfolder):
                        # Build a safe folder name from the PDF base name (without extension)
                        pdf_name_no_ext, _ = os.path.splitext(pdf_basename)
                        safe_name = sanitize_filename(pdf_name_no_ext, max_length=80) or f"pdf_{i + 1}"
                        target_folder = os.path.join(destination_folder, safe_name)
                        # Ensure uniqueness
                        base_target = target_folder
                        suffix = 1
                        while os.path.exists(target_folder):
                            target_folder = f"{base_target}_{suffix}"
                            suffix += 1
                        if attachments_subfolder != target_folder:
                            os.rename(attachments_subfolder, target_folder)
                            print(f"📁 Renamed folder: '{attachments_subfolder}' -> '{target_folder}'")
                        else:
                            print(f"📁 Keeping folder name: '{attachments_subfolder}'")

                        # Update failed records with the final folder path
                        for f in failed:
                            f['pdf_file'] = pdf_basename
                            f['attachments_folder'] = target_folder
                    else:
                        # No folder was created (unlikely when hyperlinks exist), still record failures
                        for f in failed:
                            f['pdf_file'] = pdf_basename
                            f['attachments_folder'] = os.path.join(destination_folder, f"pdf_{i + 1}")
                except Exception as e:
                    print(f"⚠️ Could not rename folder for {pdf_basename}: {e}")
                    # Fall back to original subfolder path in failure records
                    for f in failed:
                        f['pdf_file'] = pdf_basename
                        f['attachments_folder'] = attachments_subfolder

                all_failed_downloads.extend(failed)
            else:
                print(f"No links found in {pdf_basename}")
            
            # Add file information to summary
            file_summary.append({
                'file_number': i + 1,
                'file_name': pdf_basename,
                'attachments_count': successful_downloads
            })
        except Exception as e:
            print(f"❌ Error extracting/downloading links for {pdf_basename}: {e}")
            file_summary.append({
                'file_number': i + 1,
                'file_name': pdf_basename,
                'attachments_count': 0
            })
            
    # Save the file summary CSV
    summary_csv = os.path.join(destination_folder, 'file_summary.csv')
    pd.DataFrame(file_summary).to_csv(summary_csv, index=False)
    print(f"\n📊 File summary saved to: {summary_csv}")
    
    if all_failed_downloads:
        failed_csv = os.path.join(destination_folder, 'failed_downloads.csv')
        # Include link, along with context; keep extra columns if present (status_code, error)
        df = pd.DataFrame(all_failed_downloads)
        # Ensure consistent column order when possible
        preferred_cols = ['pdf_file', 'attachments_folder', 'link', 'status_code', 'error']
        cols = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
        df = df[cols]
        df.to_csv(failed_csv, index=False)
        print(f"\n❌ Failed downloads saved to: {failed_csv}")
    else:
        print("\n✅ No failed downloads.")
 
# Example usage
if __name__ == "__main__":
    source_folder = "./Submittals"
    # Use a single common folder for all PDFs and attachments
    destination_folder = "./attachments"
   
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