import os
import csv

def list_images_with_tags(folder_path, output_csv='image_tags.csv'):
    image_data = []

    for root, dirs, files in os.walk(folder_path):
        if root == folder_path:
            continue  # Skip the root folder itself

        tag = os.path.basename(root)

        for file in files:
            # Split filename and extension
            name, ext = os.path.splitext(file)
            # Create new filename with tag as prefix
            new_filename = f"{tag}_{name}{ext}"
            # Get full file paths
            old_filepath = os.path.join(root, file)
            new_filepath = os.path.join(root, new_filename)
            
            # Rename the file
            try:
                os.rename(old_filepath, new_filepath)
                print(f"Renamed: {file} -> {new_filename}")
            except Exception as e:
                print(f"Error renaming {file}: {str(e)}")
            
            image_data.append([new_filename, tag])

    with open(output_csv, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['filename', 'tag'])
        writer.writerows(image_data)

    print(f"CSV file '{output_csv}' created successfully with image filenames and tags.")

# Call the function with your folder path
list_images_with_tags('./Images')