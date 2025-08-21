import os
import shutil

def combine_files(source_path):
    # Create combined folder path
    combined_path = os.path.join(source_path, 'combined')
    
    # Create combined folder if it doesn't exist
    if not os.path.exists(combined_path):
        os.makedirs(combined_path)
        print(f"Created combined folder at: {combined_path}")
    
    # Walk through all folders and subfolders
    for root, dirs, files in os.walk(source_path):
        # Skip the combined folder itself
        if root == combined_path:
            continue
            
        for file in files:
            source_file = os.path.join(root, file)
            dest_file = os.path.join(combined_path, file)
            
            # Handle duplicate filenames by adding a counter
            base, ext = os.path.splitext(file)
            counter = 1
            while os.path.exists(dest_file):
                dest_file = os.path.join(combined_path, f"{base}_{counter}{ext}")
                counter += 1
                
            try:
                shutil.copy2(source_file, dest_file)
                print(f"Copied: {file} -> {os.path.basename(dest_file)}")
            except Exception as e:
                print(f"Error copying {file}: {str(e)}")

if __name__ == "__main__":
    folder_path = "./Images"
    combine_files(folder_path)