import os
import subprocess
from datetime import datetime

# ====== CONFIG ======
PROJECT_PATH = r"E:\Docker\airflow"   # <-- CHANGE THIS
# ====================

def run_command(command):
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"Error running: {command}")
        exit(1)

def main():
    # Go to project folder
    os.chdir(PROJECT_PATH)

    # Get current date
    today = datetime.now().strftime("%Y-%m-%d")

    # Create new txt file with date name
    filename = f"{today}.txt"
    with open(filename, "w") as f:
        f.write(f"File created on {today}")

    print(f"Created file: {filename}")

    # Run git commands
    run_command("git add .")
    run_command(f'git commit -m "{today}"')
    run_command("git push")

    print("Git push completed successfully.")

if __name__ == "__main__":
    main()
