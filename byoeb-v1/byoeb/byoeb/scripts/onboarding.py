import pandas as pd
import requests
import argparse
import ast
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Upload users from Excel files.")
    parser.add_argument("--file", required=True, help="Input Excel file path.")
    parser.add_argument("--url", default="http://0.0.0.0:8000", help="API endpoint URL")
    parser.add_argument("--update", action="store_true", help="If set, update users using the API endpoint")
    parser.add_argument("--sheet", help="output sheet name")

    args = parser.parse_args()

    file_path = args.file
    df = pd.read_excel(file_path, header=0)
    df["phone_number_id"] = df["phone_number_id"].astype(str).apply(lambda x: "91" + x if len(x) == 10 else x)

    users_onboarded = df.to_dict(orient="records") 
    phone_numbers = []  
    for row in users_onboarded:
        if "user_location" in row.keys():
            row["user_location"] = ast.literal_eval(row["user_location"])
        phone_numbers.append(row["phone_number_id"])

    response = requests.post(args.url + "/register_users", headers={"Content-Type": "application/json"}, json=users_onboarded)
    response.raise_for_status()
    print("Successfully registered")

    if args.update:
        update_response = requests.post(args.url + "/update_users", headers={"Content-Type": "application/json"}, json=users_onboarded)
        update_response.raise_for_status()
        print("Successfully updated")

    if args.sheet:
        response = requests.post(args.url + "/get_users", headers={"Accept": "application/json", "Content-Type": "application/json"}, json=phone_numbers)
        response.raise_for_status()
        users = response.json()
        print("Successfully extracted")

        df = pd.DataFrame([{
        	"user_id": user_data.get("user_id"),
        	"user_name": user_data.get("user_name"),
        	"phone": user_data.get("phone_number_id"),
        	"location": user_data.get("user_location"),
        	"user_type": user_data.get("user_type"),
        	"test_user": str(user_data.get("test_user")),
        	"onboarding_date": datetime.fromtimestamp(int(user_data.get("created_timestamp", 0))).date() if user_data.get("created_timestamp") else None,
		    "language":user_data.get("user_language")
        } for user_data in users])
        df.to_excel(args.sheet, index=False)


if __name__ == "__main__":
    main()