import pandas as pd
import requests
import json
import argparse

def main():
    parser = argparse.ArgumentParser(description="Upload users from Excel files.")
    parser.add_argument("--files", nargs="+", required=True, help="List of Excel file paths.")
    parser.add_argument("--locations", nargs="+", required=True, help="List of user locations (same order as files).")
    parser.add_argument("--types", nargs="+", required=True, help="List of user types (same order as files).")
    parser.add_argument("--languages", nargs="+", required=True, help="List of user languages (same order as files).")
    parser.add_argument("--url", default="http://0.0.0.0:8000/register_users", help="API endpoint URL")

    args = parser.parse_args()

    file_paths = args.files
    locations = args.locations
    user_types = args.types
    languages = args.languages
    url = args.url

    if not (len(file_paths) == len(locations) == len(user_types) == len(languages)):
        raise ValueError("Number of files, locations, types, and languages must all be the same.")

    all_responses = []

    for i, file_path in enumerate(file_paths):
        df = pd.read_excel(file_path, header=None)
        sheet_data = df[0].dropna().astype(str).tolist()
        phone_numbers = [int(x) for x in sheet_data if len(x) == 10 and x.isdigit()]

        users_onboarded = []
        for phone in phone_numbers:
            users_onboarded.append({
                "user_location": {"district": locations[i]},
                "user_language": languages[i],
                "user_type": user_types[i],
                "phone_number_id": "91" + str(phone)
            })

        response = requests.post(url, headers={"accept": "application/json"}, data=json.dumps(users_onboarded))
        all_responses.append((file_path, response.status_code, response.text))


    with open("users_response.txt", "w", encoding="utf-8") as f:
        for file_path, status, text in all_responses:
            f.write(f"File: {file_path}\nStatus Code: {status}\nResponse: {text}\n\n")

if __name__ == "__main__":
    main()

