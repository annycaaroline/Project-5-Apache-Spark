#!/usr/bin/env python3
import tarfile
import boto3
import time
from datetime import datetime, timedelta

# Configuration
BUCKET = 'scalabillity-data-ghcnd'
TAR_KEY = 'ghcnd_all.tar.gz'
OUTPUT_PREFIX = 'ghcnd_data_full/'

print(f"Starting at {datetime.now().strftime('%H:%M:%S')}")
print("=" * 60)
print(" EXTRACTING ALL FILES (may take 2–3 hours)")
print("=" * 60)

# Connect to S3
s3 = boto3.client('s3')

try:
    # Stream download of tar.gz from S3
    print("Downloading tar.gz from S3 using streaming...")
    response = s3.get_object(Bucket=BUCKET, Key=TAR_KEY)
    
    print("Extracting ALL .dly files...")
    count = 0
    start_time = time.time()
    
    with tarfile.open(fileobj=response['Body'], mode='r|gz') as tar:
        for member in tar:
            if member.isfile() and member.name.endswith('.dly'):
                file_obj = tar.extractfile(member)
                if file_obj:
                    station_id = member.name.split('/')[-1]
                    output_key = f"{OUTPUT_PREFIX}{station_id}"
                    s3.put_object(
                        Bucket=BUCKET,
                        Key=output_key,
                        Body=file_obj.read()
                    )
                    count += 1
                    
                    if count % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = count / elapsed if elapsed > 0 else 0
                        print(
                            f"  {count:,} files | "
                            f"{rate:.1f} files/s | "
                            f"Time: {elapsed/60:.1f} min"
                        )
    
    elapsed = time.time() - start_time
    print("=" * 60)
    print("COMPLETED")
    print(f"Total files: {count:,}")
    print(f"Total time: {elapsed/60:.1f} min ({elapsed/3600:.2f} hours)")
    print(f"Output location: s3://{BUCKET}/{OUTPUT_PREFIX}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
