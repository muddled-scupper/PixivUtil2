import os
import re

def rename_folders(base_path):
    """
    Rename folders in the specified directory from "{artist_name} ({artist_id})" to "{artist_id} - {artist_name}".

    :param base_path: Path to the base directory containing the folders.
    """
    for folder_name in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder_name)

        # Check if it's a directory
        if os.path.isdir(folder_path):
            # Match the folder name with the pattern "{artist_name} ({artist_id})"
            match = re.match(r"^(.*?) \((\d+)\)$", folder_name)
            if match:
                artist_name = match.group(1)
                artist_id = match.group(2)

                # Create the new folder name
                new_folder_name = f"{artist_id} - {artist_name}"
                new_folder_path = os.path.join(base_path, new_folder_name)

                # Rename the folder
                os.rename(folder_path, new_folder_path)
                print(f"Renamed: {folder_name} -> {new_folder_name}")

if __name__ == "__main__":
    pass
    # base_path = "./Artists"  # Replace with the actual path to your folder
    # rename_folders(base_path)
