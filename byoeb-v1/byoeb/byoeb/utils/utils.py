import os

def get_git_root_path():
    current_dir = os.path.abspath(__file__)
    try:
        while current_dir != os.path.dirname(current_dir):  # Stop at the filesystem root
            if os.path.isdir(os.path.join(current_dir, ".git")):
                return current_dir
            current_dir = os.path.dirname(current_dir)
        return current_dir
    except Exception as e:
        print(f"Error: {str(e)}")
        return None
    
def log_to_text_file(text):
    git_root = get_git_root_path()
    file_path = os.path.join(git_root, "byoeb-v1/byoeb/log.txt")
    with open(file_path, "a") as file:
        file.write(text + "\n")

def is_idk(
    text: str
):
    idks = [
        "idk",
        "i don't know",
        "i do not know",
        "i don't know the answer",
    ]
    return text.lower() in idks