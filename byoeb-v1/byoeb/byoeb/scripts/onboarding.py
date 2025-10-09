import pandas as pd
import requests
import json
import argparse
import requests
import pandas as pd
from datetime import datetime
def main():
    parser = argparse.ArgumentParser(description="Upload users from Excel files.")
    parser.add_argument("--file", required=True, help="List of Excel file paths.")
    parser.add_argument("--url", default="http://0.0.0.0:8000/register_users", help="API endpoint URL")
    parser.add_argument(
    "--update",
    action="store_true",
    help="If set, update users using the API endpoint")
    parser.add_argument("--sheet")
    
    args = parser.parse_args()

    file_path = args.file
    url = args.url
    df = pd.read_excel(file_path, header=0)
    users_onboarded = df.to_dict(orient="records") 
    phone_numbers=[]  
    for row in users_onboarded:
	    row["phone_number_id"]=str(row["phone_number_id"])
	    phone_numbers.append(str(row["phone_number_id"]))
    response = requests.post(url, headers={"accept": "application/json"}, data=json.dumps(users_onboarded))
    print(response)
    API_URL = url.replace("register_users","get_users")

    
    response = requests.get(
    API_URL,
    headers={"Content-Type": "application/json"},
    json=phone_numbers
)
	#add items from users to user_onboarded such that we don't overwrite imp exisistin information
    if response.status_code != 200:
	    print(f"Error: {response.status_code} - {response.text}")
	    exit(1)
    else:
    	    print("Successfully Registered")
    	
    users = response.json()
    
    if args.update:
    	update_url=url.replace("register_users","update_users")
    	
    	#print(users_onboarded)
    	update_response = requests.post(
    update_url,
    headers={
        "accept": "application/json",
        "Content-Type": "application/json"
    },
    data=json.dumps(users_onboarded)
)

    	print(update_response)
    if args.sheet:
    	response = requests.get(
    API_URL,
    headers={"Content-Type": "application/json"},
    json=phone_numbers
)
    	if response.status_code != 200:
    		print(f"Error: {response.status_code} - {response.text}")
    		exit(1)
    	else:
    		print("Successfully Updated")

    	users = response.json()
    	records = []
    	for user_data in users:
    		record = {
		"user_id": user_data.get("user_id"),
		"user_name": user_data.get("user_name"),
		"phone": user_data.get("phone_number_id"),
		"location": user_data.get("user_location"),
		"user_type": user_data.get("user_type"),
		"test_user": str(user_data.get("test_user")),
		"onboarding_date": datetime.fromtimestamp(int(user_data.get("created_timestamp", 0))).date()
		if user_data.get("created_timestamp") else None,
		"language":user_data.get("user_language")
	    }
    		records.append(record)
    	df = pd.DataFrame(records)
    	df.to_excel(args.sheet, index=False)
 

if __name__ == "__main__":
    main()
 

