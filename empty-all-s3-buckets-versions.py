import boto3
from botocore.exceptions import ClientError


def check_bucket_versioning(bucket):
    """Check if bucket has versioning enabled"""
    try:
        versioning = bucket.Versioning()
        return versioning.status == 'Enabled'
    except ClientError as e:
        print(f"Error checking versioning: {e}")
        return False

def delete_all_versions(bucket):
    """Delete all versions and delete markers in the bucket"""
    try:
        print(f"Deleting all versions from {bucket.name}...")
        
        # Delete all object versions (including delete markers)
        versions = []
        for obj_version in bucket.object_versions.all():
            versions.append({'Key': obj_version.object_key, 
                           'VersionId': obj_version.id})
            # Process deletions in batches of 1000 (AWS limit)
            if len(versions) >= 1000:
                bucket.delete_objects(Delete={'Objects': versions})
                versions = []
        
        # Delete remaining versions
        if versions:
            bucket.delete_objects(Delete={'Objects': versions})
            
        print("All versions deleted successfully")
        return True
    except ClientError as e:
        print(f"Error deleting versions: {e}")
        return False


def list_and_delete_buckets():
    s3 = boto3.resource('s3')
    
    # List all buckets
    buckets = list(s3.buckets.all())
    if not buckets:
        print("No buckets found in your account.")
        return

    # Print buckets with numbers for selection
    print("\nAvailable buckets:")
    for index, bucket in enumerate(buckets, 1):
        versioning_status = "Versioned" if check_bucket_versioning(bucket) else "XXXXXXXXXXXXX"
        print(f"{index}. {bucket.name} ({versioning_status})")

    # Get user selection
    while True:
        try:
            selection = input("\nEnter the number of the bucket to delete (or 'q' to quit): ")
            if selection.lower() == 'q':
                return
            
            bucket_index = int(selection) - 1
            if 0 <= bucket_index < len(buckets):
                selected_bucket = buckets[bucket_index]
                bucket = s3.Bucket(selected_bucket.name)
                
                # First confirmation
                confirm = input(f"\nAre you sure you want to delete '{selected_bucket.name}'? "
                              f"This action cannot be undone! (yes/no): ")
                
                if confirm.lower() == 'yes':
                    try:
                        # Check if bucket is versioned
                        is_versioned = check_bucket_versioning(bucket)
                        
                        # Check if bucket has any objects (including versions)
                        objects_exist = False
                        versions_exist = False
                        
                        # Check for current objects
                        for _ in bucket.objects.limit(1):
                            objects_exist = True
                            break
                            
                        # Check for versions if bucket is versioned
                        if is_versioned:
                            for _ in bucket.object_versions.limit(1):
                                versions_exist = True
                                break

                        if objects_exist or versions_exist:
                            if is_versioned:
                                version_warning = ("\nWARNING: This is a versioned bucket. "
                                                 "Deleting it will remove ALL versions of ALL objects.\n")
                                print(version_warning)
                                empty_confirm = input("Delete all versions and empty bucket? (yes/no): ")
                                
                                if empty_confirm.lower() == 'yes':
                                    if not delete_all_versions(bucket):
                                        continue
                            else:
                                empty_confirm = input("Bucket is not empty. Empty and delete? (yes/no): ")
                                if empty_confirm.lower() == 'yes':
                                    print(f"Emptying bucket {selected_bucket.name}...")
                                    bucket.objects.all().delete()
                        
                        # Final deletion
                        print(f"Deleting bucket {selected_bucket.name}...")
                        bucket.delete()
                        print("Bucket deleted successfully!")
                        
                    except ClientError as e:
                        print(f"Error: {e}")
                else:
                    print("Deletion cancelled.")
            else:
                print("Invalid selection. Please try again.")
                
        except ValueError:
            print("Please enter a valid number.")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    list_and_delete_buckets()
