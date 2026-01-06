#!/usr/bin/env python3
"""
Demo script for the FileEngine Python client
"""

from fileengine import ManagedFiles
from fileengine import fileservice_pb2

def main():
    # Create a ManagedFiles instance with connection to the gRPC service
    # Using root user with superuser privileges to bypass permission issues
    mf = ManagedFiles(
        user_name="root",
        user_roles=["admin", "superuser"],
        user_claims=["read", "write", "delete", "admin"],
        server_address="localhost:50051",  # Default gRPC server address
        tenant="default"
    )

    try:
        # Create a root directory
        root_dir = mf.mkdir("", "demo_root")
        print(f"Created root directory with UID: {root_dir}")

        # Create a subdirectory
        sub_dir = mf.mkdir(root_dir, "subdir")
        print(f"Created subdirectory with UID: {sub_dir}")

        # Create a file in the subdirectory
        file_uid = mf.touch(sub_dir, "demo_file.txt")
        print(f"Created file with UID: {file_uid}")

        # Write content to the file
        content = b"This is a demo file for the FileEngine Python client."
        version = mf.put(file_uid, content)
        print(f"Written content with version timestamp: {version}")

        # Read content from the file
        file_content = mf.get(file_uid)
        print(f"Read content: {file_content.getvalue()}")

        # List directory contents
        contents = mf.dir(root_dir)
        print(f"Directory contents: {contents}")

        # Get file revisions
        revisions = mf.revisions(file_uid)
        print(f"File revisions: {revisions}")

        # Get file metadata
        file_name = mf.file_name(file_uid)
        print(f"File name: {file_name}")

        # Get modification time
        mtime = mf.get_file_mtime(file_uid)
        print(f"Modification time: {mtime}")

        # Demonstrate permission operations
        print("\n--- Permission Operations ---")

        # Grant read permission to a user (example)
        permission_granted = mf.grant_permission(
            resource_uid=file_uid,
            principal="demo_user",
            permission=fileservice_pb2.Permission.READ
        )
        print(f"Granted read permission: {permission_granted}")

        # Check if user has permission
        has_permission = mf.check_permission(
            resource_uid=file_uid,
            required_permission=fileservice_pb2.Permission.READ
        )
        print(f"User has read permission: {has_permission}")

        # Revoke the permission
        permission_revoked = mf.revoke_permission(
            resource_uid=file_uid,
            principal="demo_user",
            permission=fileservice_pb2.Permission.READ
        )
        print(f"Revoked read permission: {permission_revoked}")

        # Demonstrate status operations
        print("\n--- Status Operations ---")

        # Get storage usage
        storage_info = mf.get_storage_usage()
        if storage_info:
            print(f"Storage usage: {storage_info}")
        else:
            print("Could not retrieve storage usage")

        # Trigger sync
        sync_triggered = mf.trigger_sync()
        print(f"Sync triggered: {sync_triggered}")

        # Restore to version (if multiple versions exist)
        revisions = mf.revisions(file_uid)
        if len(revisions) > 1:
            # Restore to the second-to-last version as an example
            restore_version = mf.restore_to_version(file_uid, revisions[1]['version'])
            print(f"Restored to version: {restore_version}")

        # Purge old versions (keep only the latest 2)
        purged = mf.purge_old_versions(file_uid, keep_count=2)
        print(f"Purged old versions: {purged}")

        print("\nDemo completed successfully!")

    except Exception as e:
        print(f"Error during demo: {e}")

    finally:
        # Close the connection
        mf.close()

if __name__ == "__main__":
    main()