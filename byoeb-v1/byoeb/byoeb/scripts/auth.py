import argparse
import getpass
import uuid

from pymongo import MongoClient
from pydantic import TypeAdapter, ValidationError

from byoeb.chat_app.configuration.config import app_config
from byoeb_core.models.byoeb.user import PhoneNumberId
from byoeb.services.auth.security import PASSWORD_CTX


def main() -> int:
    parser = argparse.ArgumentParser(description="Auth admin CLI.")
    parser.add_argument("--mongo-uri", required=True, help="MongoDB connection string.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    tenant_cmd = subparsers.add_parser("create-tenant", help="Create an auth tenant.")
    tenant_cmd.add_argument("--name", required=True, help="Tenant display name.")

    user_cmd = subparsers.add_parser("create-user", help="Create an auth user.")
    user_cmd.add_argument("--username", required=True, help="Auth username.")
    user_cmd.add_argument("--password", help="Auth password (prompted if omitted).")
    user_cmd.add_argument("--tenant-id", required=True, help="Tenant ID for the auth user.")
    user_cmd.add_argument("--roles", required=True, help="Comma-separated roles.")
    user_cmd.add_argument("--phone", help="Phone number.")

    add_cmd = subparsers.add_parser("add-user-tenant", help="Add a user to another tenant.")
    add_cmd.add_argument("--username", required=True, help="Auth username.")
    add_cmd.add_argument("--tenant-id", required=True, help="Tenant ID to add.")
    add_cmd.add_argument("--roles", required=True, help="Comma-separated roles for the tenant.")

    remove_cmd = subparsers.add_parser("remove-user-tenant", help="Remove a user from a tenant.")
    remove_cmd.add_argument("--username", required=True, help="Auth username.")
    remove_cmd.add_argument("--tenant-id", required=True, help="Tenant ID to remove.")

    int_add_cmd = subparsers.add_parser("add-integration", help="Add a tenant integration (e.g., WhatsApp).")
    int_add_cmd.add_argument("--platform", default="whatsapp", help="Integration platform.")
    int_add_cmd.add_argument("--identifier", required=True, help="Integration identifier (e.g., phone_number_id).")
    int_add_cmd.add_argument("--tenant-id", required=True, help="Tenant ID for the integration.")
    int_add_cmd.add_argument("--app-secret", required=True, help="Application secret.")
    int_add_cmd.add_argument("--bearer-token", required=True, help="Bearer token.")
    int_add_cmd.add_argument("--verify-token", required=True, help="Webhook verification token.")

    int_remove_cmd = subparsers.add_parser("remove-integration", help="Remove a tenant integration.")
    int_remove_cmd.add_argument("--platform", default="whatsapp", help="Integration platform.")
    int_remove_cmd.add_argument("--identifier", required=True, help="Integration identifier.")

    int_list_cmd = subparsers.add_parser("list-integrations", help="List tenant integrations.")
    int_list_cmd.add_argument("--tenant-id", help="Filter by Tenant ID.")

    args = parser.parse_args()

    database_name = app_config["databases"]["mongo_db"]["database_name"]
    user_collection_name = app_config["databases"]["mongo_db"]["auth_user_collection"]
    tenant_collection_name = app_config["databases"]["mongo_db"]["auth_tenant_collection"]
    roles_collection_name = app_config["databases"]["mongo_db"]["auth_tenant_roles_collection"]
    integration_collection_name = app_config["databases"]["mongo_db"]["auth_tenant_integrations_collection"]
    client = MongoClient(args.mongo_uri, uuidRepresentation="standard")
    db = client[database_name]
    user_collection = db[user_collection_name]
    tenant_collection = db[tenant_collection_name]
    roles_collection = db[roles_collection_name]
    integration_collection = db[integration_collection_name]

    if args.command == "create-tenant":
        tenant_id = uuid.uuid4()
        if tenant_collection.find_one({"_id": tenant_id}):
            print("Tenant already exists. No changes made.")
            return 1
        roles = app_config.get("default_tenant_roles", {})
        tenant_collection.insert_one({"_id": tenant_id, "name": args.name})
        roles_collection.insert_one({"_id": tenant_id, "roles": roles})
        print(f"Tenant created: {tenant_id}")
        return 0

    if args.command == "create-user":
        try:
            tenant_id = uuid.UUID(args.tenant_id)
        except ValueError as exc:
            raise SystemExit(f"Invalid tenant-id (not a UUID): {args.tenant_id}") from exc
        phone_number_id = None
        if args.phone:
            try:
                phone_number_id = TypeAdapter(PhoneNumberId).validate_python(args.phone)
            except ValidationError as exc:
                raise SystemExit("Invalid phone number id (expected 11-13 digits).") from exc
        password = args.password or getpass.getpass("Password: ")
        if not password:
            raise SystemExit("Password is required.")
        roles = [role.strip() for role in args.roles.split(",") if role.strip()]
        if not roles:
            raise SystemExit("At least one role is required.")
        if user_collection.find_one({"username": args.username}):
            print("User already exists. No changes made.")
            return 1
        if not tenant_collection.find_one({"_id": tenant_id}):
            print("Tenant not found. No changes made.")
            return 1
        roles_doc = roles_collection.find_one({"_id": tenant_id}) or {}
        tenant_roles = set((roles_doc.get("roles") or {}).keys())
        if not set(roles).issubset(tenant_roles):
            print("One or more roles are not defined for this tenant. No changes made.")
            return 1
        password_hash = PASSWORD_CTX.hash(password)
        user_doc = {
            "_id": uuid.uuid4(),
            "username": args.username,
            "tenants": [{"tenant_id": tenant_id, "roles": roles}],
            "password_hash": password_hash,
        }
        if phone_number_id:
            user_doc["phone_number_id"] = phone_number_id
        user_collection.insert_one(user_doc)
        print("Auth user created.")
        return 0

    if args.command == "add-user-tenant":
        try:
            tenant_id = uuid.UUID(args.tenant_id)
        except ValueError as exc:
            raise SystemExit(f"Invalid tenant-id (not a UUID): {args.tenant_id}") from exc
        roles = [role.strip() for role in args.roles.split(",") if role.strip()]
        if not roles:
            raise SystemExit("At least one role is required.")
        user_doc = user_collection.find_one({"username": args.username})
        if not user_doc:
            print("User not found. No changes made.")
            return 1
        if not tenant_collection.find_one({"_id": tenant_id}):
            print("Tenant not found. No changes made.")
            return 1
        roles_doc = roles_collection.find_one({"_id": tenant_id}) or {}
        tenant_roles = set((roles_doc.get("roles") or {}).keys())
        if not set(roles).issubset(tenant_roles):
            print("One or more roles are not defined for this tenant. No changes made.")
            return 1
        if next((t for t in user_doc.get("tenants", []) if t.get("tenant_id") == tenant_id), None):
            print("User already belongs to this tenant. No changes made.")
            return 1
        user_collection.update_one(
            {"_id": user_doc["_id"]},
            {"$push": {"tenants": {"tenant_id": tenant_id, "roles": roles}}},
        )
        print(f"Added {args.username} to tenant {tenant_id}.")
        return 0

    if args.command == "remove-user-tenant":
        try:
            tenant_id = uuid.UUID(args.tenant_id)
        except ValueError as exc:
            raise SystemExit(f"Invalid tenant-id (not a UUID): {args.tenant_id}") from exc
        user_doc = user_collection.find_one({"username": args.username})
        if not user_doc:
            print("User not found. No changes made.")
            return 1
        if not next((t for t in user_doc.get("tenants", []) if t.get("tenant_id") == tenant_id), None):
            print("User does not belong to this tenant. No changes made.")
            return 1
        user_collection.update_one(
            {"_id": user_doc["_id"]},
            {"$pull": {"tenants": {"tenant_id": tenant_id}}},
        )
        print(f"Removed {args.username} from tenant {tenant_id}.")
        return 0

    if args.command == "add-integration":
        try:
            tenant_id = uuid.UUID(args.tenant_id)
        except ValueError as exc:
            raise SystemExit(f"Invalid tenant-id (not a UUID): {args.tenant_id}") from exc
        
        if not tenant_collection.find_one({"_id": tenant_id}):
            print("Tenant not found. No changes made.")
            return 1

        if integration_collection.find_one({"platform": args.platform, "identifier": args.identifier}):
            print(f"Integration already exists for {args.platform}:{args.identifier}. Overwriting.")
            integration_collection.delete_one({"platform": args.platform, "identifier": args.identifier})

        integration_doc = {
            "_id": uuid.uuid4(),
            "platform": args.platform,
            "identifier": args.identifier,
            "tenant_id": tenant_id,
            "credentials": {
                "app_secret": args.app_secret,
                "bearer_token": args.bearer_token,
                "verification_token": args.verify_token
            }
        }
        integration_collection.insert_one(integration_doc)
        print(f"Integration added for tenant {tenant_id} on {args.platform}:{args.identifier}.")
        return 0

    if args.command == "remove-integration":
        res = integration_collection.delete_one({"platform": args.platform, "identifier": args.identifier})
        if res.deleted_count > 0:
            print(f"Removed integration for {args.platform}:{args.identifier}.")
            return 0
        print(f"Integration not found for {args.platform}:{args.identifier}.")
        return 1

    if args.command == "list-integrations":
        query = {}
        if args.tenant_id:
            try:
                query["tenant_id"] = uuid.UUID(args.tenant_id)
            except ValueError as exc:
                raise SystemExit(f"Invalid tenant-id (not a UUID): {args.tenant_id}") from exc
        
        integrations = list(integration_collection.find(query))
        if not integrations:
            print("No integrations found.")
            return 0
        
        print(f"{'Platform':<15} {'Identifier':<20} {'Tenant ID':<40} {'Verify Token'}")
        print("-" * 100)
        for i in integrations:
            verify_token = i.get('credentials', {}).get('verification_token', 'N/A')
            print(f"{i.get('platform', 'N/A'):<15} {i.get('identifier', 'N/A'):<20} {str(i.get('tenant_id', 'N/A')):<40} {verify_token}")
        return 0

    raise SystemExit("Unknown command.")


if __name__ == "__main__":
    raise SystemExit(main())
