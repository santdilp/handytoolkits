import boto3
from botocore.exceptions import ClientError

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
        print(f"{index}. {bucket.name}")

    # Get user selection
    while True:
        try:
            selection = input("\nEnter the number of the bucket to delete (or 'q' to quit): ")
            if selection.lower() == 'q':
                return
            
            bucket_index = int(selection) - 1
            if 0 <= bucket_index < len(buckets):
                selected_bucket = buckets[bucket_index]
                
                # Confirm deletion
                confirm = input(f"\nAre you sure you want to delete '{selected_bucket.name}'? "
                              f"This action cannot be undone! (yes/no): ")
                
                if confirm.lower() == 'yes':
                    try:
                        # Check if bucket is empty
                        bucket = s3.Bucket(selected_bucket.name)
                        objects_exist = False
                        
                        for _ in bucket.objects.limit(1):
                            objects_exist = True
                            break
                            
                        if objects_exist:
                            empty_confirm = input("Bucket is not empty. Empty and delete? (yes/no): ")
                            if empty_confirm.lower() == 'yes':
                                print(f"Emptying bucket {selected_bucket.name}...")
                                bucket.objects.all().delete()
                        
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
