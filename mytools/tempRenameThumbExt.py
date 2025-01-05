import os

def rename_thumbs(root_folder, mid_folder_name='thumbs'):
    mid_folder = os.path.join(root_folder, mid_folder_name)
    for dirpath, dirnames, filenames in os.walk(mid_folder):
        for filename in filenames:
            if filename.endswith('.jpg'):
                    file_base, file_ext = os.path.splitext(filename)
                    new_filename = f"{file_base}.webp"
                    old_path = os.path.join(dirpath, filename)
                    new_path = os.path.join(dirpath, new_filename)
                    os.rename(old_path, new_path)
                    print(f"Renamed: {old_path} -> {new_path}")

if __name__ == "__main__":
     pass
    # root_folder = "../PxArtists"
    # rename_thumbs(root_folder)
