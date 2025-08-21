import fitz  # PyMuPDF
import os
import glob

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

def count_downloadable_links(links):
    count = 0
    for link in links:
        lower_link = link.lower()
        if lower_link.startswith('mailto:'):
            continue
        if not lower_link.startswith(('http://', 'https://')):
            continue
        count += 1
    return count

def process_pdf_folder_for_attachment_counts(source_folder):
    pdf_files = glob.glob(os.path.join(source_folder, "*.pdf"))
    if not pdf_files:
        print(f"No PDF files found in {source_folder}")
        return

    print(f"Found {len(pdf_files)} PDF files to process for attachment counts.")
    total_attachments = 0

    for pdf_file in pdf_files:
        print(f"\nProcessing PDF file: {pdf_file}")
        if not os.path.exists(pdf_file):
            print(f"Warning: PDF file no longer exists: {pdf_file}")
            continue
        try:
            hyperlinks = extract_hyperlinks_from_pdf(pdf_file)
            downloadable_count = count_downloadable_links(hyperlinks)
            print(f"Downloadable attachments (excluding mailto and non-http links): {downloadable_count}")
            total_attachments += downloadable_count
        except Exception as e:
            print(f"❌ Error processing {pdf_file}: {e}")

    print(f"\n📊 Total downloadable attachments across all PDFs: {total_attachments}")

# Example usage
if __name__ == "__main__":
    source_folder = "./Transmittals/Transmittals"
    source_folder = os.path.abspath(source_folder)

    print(f"Current working directory: {os.getcwd()}")
    print(f"Source folder absolute path: {source_folder}")

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
            process_pdf_folder_for_attachment_counts(source_folder)
        except Exception as e:
            print(f"Error accessing source folder: {e}")

