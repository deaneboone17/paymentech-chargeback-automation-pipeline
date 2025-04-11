import base64
from google.cloud import storage, secretmanager
from google.oauth2.service_account import Credentials
import boto3
import json
import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict
import time
import zipfile
import shutil
import os
import csv


# Configuration for GCP and file paths
bucket_name = "ramove"  # Your GCP bucket name
dfr_bucket = "reporting.raw.charonraassurance.pandora.com"
pdf_template_file = "chargeback_automation/terms_conditions/SiriusXM_Customer_Agreement_2024_Legacy.pdf"
last_run_time_file = "chargeback_automation/last_run/last_run_time.txt"
dfr_prefix = "paymentech/dfr_a"

# Access secret payload from GCP Secret Manager
def access_secret_version(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return json.loads(response.payload.data.decode('UTF-8'))

def access_secret_version_csv(project_id, secret_id_aws, version_id):
    # Access secret payload from Google Cloud Secret Manager
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id_aws}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    payload = response.payload.data.decode('UTF-8')
    # Parse the CSV file to obtain the credentials
    csv_data = csv.reader(payload.splitlines())
    headers = next(csv_data)
    values = next(csv_data)
    """
    # Map the values to the corresponding headers
    credentials = dict(zip(headers, values))
    access_key = credentials.get('aws_access_key')  
    secret_key = credentials.get('aws_secret_key')
    """
    aws_access_key, aws_secret_key = values

    return aws_access_key, aws_secret_key


# Establish connection with GCP Storage
def establish_gcs_connection(gcp_credentials):
    credentials = Credentials.from_service_account_info(gcp_credentials)
    return storage.Client(credentials=credentials)


def establish_s3_connection(access_key, secret_key):
    # Establish connection with AWS S3
    s3 = boto3.resource(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return s3


# Read text file content from GCP Storage
def read_file_from_gcp(storage_client, dfr_bucket, file_name):
    bucket = storage_client.bucket(dfr_bucket)
    blob = bucket.blob(file_name)
    return blob.download_as_text()

# Read binary file (PDF) content from GCP Storage
def read_binary_file_from_gcp(storage_client, bucket_name, pdf_template_file):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(pdf_template_file)
    return blob.download_as_bytes()

# Read the last run time from GCP Storage
def read_last_run_time_from_gcp(storage_client, bucket_name, file_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        last_run_time_str = blob.download_as_text()
        last_run_time = datetime.strptime(last_run_time_str, '%Y-%m-%d %H:%M:%S')
        return last_run_time.replace(tzinfo=timezone.utc)
    except Exception as e:
        print(f"Error reading last run time: {e}")
        return datetime.fromtimestamp(0, tz=timezone.utc)

# Write the current time as the last run time to GCP Storage
def write_last_run_time_to_gcp(storage_client, bucket_name, file_name):
    current_time = datetime.now()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(current_time.strftime('%Y-%m-%d %H:%M:%S'))

# List and filter files in GCP Storage based on last run time
def list_and_filter_files(storage_client, dfr_bucket, dfr_prefix, last_run_time):
    bucket = storage_client.bucket(dfr_bucket)
    blobs = bucket.list_blobs(prefix=dfr_prefix)
    filter_name = "0000078319"
    return [blob.name for blob in blobs if filter_name in blob.name and blob.time_created > last_run_time]

# Extract YYYYMMDD date from filename
def extract_date_from_filename(file_name):
    date_str = file_name[28:34]
    return "20" + date_str  # Prefix '20' for years 2000-2099

# Function to create header file content
def create_header_file_content():
    file_creation_date_full = datetime.today().strftime('%Y%m%d%H%M%S')
    file_creation_date = datetime.today().strftime('%y%m%d')
    presenter_id = "581008"
    pid_password = "SIRIUSXM"
    submitter_id = "581008"
    company_id = "078319"
    content = f"PID={presenter_id} {pid_password} SID={submitter_id} CBZTIFF  START  {file_creation_date} 3.0.0            {file_creation_date_full}.{company_id}.txt                       "+'\n'
    content = content.ljust(120)
    return content

# Function to create index header
def create_index_header():
    file_creation_date = datetime.today().strftime('%Y%m%d')
    company_id = "078319".zfill(15)
    company_name = "Sirius XM Radio Inc."
    company_name = company_name.ljust(32)
    #file_creation_date = datetime.today().strftime('%Y%m%d')
    content = f"H1.00{company_id}{company_name}{file_creation_date}"
    content = content.ljust(60)
    return content

# Function to create index detail records
def create_index_detail(df, storage_client):
    detail_records = []
    pdfs = []
    sequence_counts = defaultdict(int)
    pdf_template_content = read_binary_file_from_gcp(storage_client, bucket_name, pdf_template_file)

    for _, row in df.iterrows():
        pde_sequence_number = '{0:0>12}'.format(row['sequence_number'])
        division_number = '{0:0>15}'.format(row['entity_number'])
        account_number = row['account_number'][-4:]
        file_creation_date = datetime.today().strftime('%Y%m%d')
        sequence_counts[pde_sequence_number] += 1
        sequence_count_formatted = '{0:0>2}'.format(sequence_counts[pde_sequence_number] - 1)
        detail = f"D{pde_sequence_number}{division_number}{account_number}{pde_sequence_number}.{division_number}.{file_creation_date}.{sequence_count_formatted}.pdf"
        new_pdf_name = f"{pde_sequence_number}.{division_number}.{file_creation_date}.{sequence_count_formatted}.pdf"
        new_pdf_path = f'/tmp/{new_pdf_name}'
        with open(new_pdf_path, 'wb') as new_pdf_file:
            new_pdf_file.write(pdf_template_content)

        detail = detail.ljust(76)
        detail_records.append(detail)
        pdfs.append(new_pdf_name)

    return [detail_records, pdfs]

# Function to create index trailer record
def create_index_trailer(detail_records):
    total_records = '{0:0>9}'.format(len(detail_records[1]))
    trailer = f"T{total_records}"
    trailer = trailer.ljust(10)
    return trailer

# Create ZIP file from generated files
def create_zip_file(index_file_path, pdfs):
    original_zip_path = '/tmp/original_zip.zip'
    # Create temporary zip file
    with zipfile.ZipFile(original_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(index_file_path, arcname=os.path.basename(index_file_path))
        for pdf in pdfs[1]:
            zipf.write(f'/tmp/{pdf}', arcname=os.path.basename(pdf))
            


def prepend_text_to_zip(header_file_path, original_zip_path, zip_file_path):
    try:
        # Read the contents of the text file
        with open(header_file_path, 'rb') as text_file:
            text_content = text_file.read()

        # Read the contents of the ZIP file
        with open(original_zip_path, 'rb') as zip_file:
            zip_content = zip_file.read()

        # Create a new ZIP file and write the text content followed by the ZIP content
        with open(zip_file_path, 'wb') as output_file:
            output_file.write(text_content)
            output_file.write(zip_content)

        print(f"New ZIP file created with text prepended: {zip_file_path}")
    except IOError as e:
        print(f"An error occurred: {e}")


            
def upload_file_to_gcp(storage_client, bucket_name, zip_file_path, zip_file_datetime):
    subfolder = "chargeback_automation"
    destination_blob_name = f"{subfolder}/p_0000078319.{zip_file_datetime}.zip"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(zip_file_path)
    print(f"File {zip_file_path} uploaded to {destination_blob_name} in bucket {bucket_name}.")

def upload_file_to_aws(s3, bucket_name_s3, subfolder_s3, zip_file_path, zip_file_datetime): 
    destination_obj_name = f"{subfolder_s3}/p_0000078319.{zip_file_datetime}.zip"
    s3.meta.client.upload_file(zip_file_path, bucket_name_s3, destination_obj_name )
    print(f"File {zip_file_path} uploaded to {destination_obj_name} in bucket {bucket_name_s3}.")


# Write logs to GCP bucket
def write_log_to_gcp(
    storage_client, bucket_name, df, file_name, rtm_count, zip_file_date
):
    
    log_name = file_name
    log_name_str = log_name.split("/")
    log_file_name = f"chargeback_automation/logs/paymentech_DFRs/{log_name_str[2]}.log"

    log_file_path = f"/tmp/{log_name_str[2]}.log"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(log_file_name)

    if rtm_count == 0:
        with open(log_file_path, "w") as f:
            column_names = "date,file_name,sequence_number,issuer_chargeback_amount,mop,record_count"
            empty_log_content = f"{zip_file_date},{file_name},,,,{rtm_count}"
            f.write(column_names + "\n")
            f.write(empty_log_content)
            blob.upload_from_filename(log_file_path)
    else:
        selected_columns = ["sequence_number", "issuer_chargeback_amount", "mop"]
        dfr_content = df[selected_columns].copy()
        dfr_content["date"] = zip_file_date
        dfr_content["record_count"] = rtm_count
        dfr_content["file_name"] = file_name

        log_content = dfr_content
        log_content = log_content.reindex(
            columns=[
                "date",
                "file_name",
                "sequence_number",
                "issuer_chargeback_amount",
                "mop",
                "record_count",
            ]
        )
        log_content.to_csv(log_file_path, sep=",", index=False)

        blob.upload_from_filename(log_file_path)


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Get GCP credentials and establish connection
    secret_id_gcp = "ra_dev_cred"
    version_id = "latest"
    project_id = "1072076586660"
    secret_id_aws = "s3-sxm-ctrl-rev-dev"
    bucket_name_s3 = "sxm-ctrl-rev-dev"
    subfolder_s3 = "chargeback_automation"
    archive_s3 = "chargeback_automation/archive"
    gcp_credentials = access_secret_version(project_id, secret_id_gcp, version_id)
    aws_credentials = access_secret_version_csv(project_id, secret_id_aws, version_id)
    storage_client = establish_gcs_connection(gcp_credentials)

    s3 = establish_s3_connection(aws_credentials[0], aws_credentials[1])

    # Read the last run time and filter files
    last_run_time = read_last_run_time_from_gcp(storage_client, bucket_name, last_run_time_file)
    filtered_files = list_and_filter_files(storage_client, dfr_bucket, dfr_prefix, last_run_time)

    # Process each filtered file
    for file_name in filtered_files:
        file_content = read_file_from_gcp(storage_client, dfr_bucket, file_name)
        data = []
        rtm_count = 0

        codes = ['13.1','07','08','12','31','34','37','40','41','42','46','49','53','54','55','59','60',
                '63','70','71','10.1','10.2','10.3','10.5','11.1','11.2','11.3','12.1','12.2','12.3','12.4','12.5',
                '12.6.1','12.6.2','12.7','13.2','13.3','13.4','13.5','13.6','13.7','13.8','13.9']


        for line in file_content.splitlines():
            if any(f'|{code}|' in line for code in codes) and 'RTM' in line and 'USD' in line:
                parsed_line = [field.strip() for field in line.split('|')]
                data.append(parsed_line)
                rtm_count += 1 


        df = pd.DataFrame(data, columns=['record_type', 'entity_type', 'entity_number', 'issuer_chargeback_amount', 'partial_representment', 'presentment_currency', 'category', 'status_flag',
                                         'sequence_number', 'merchant_order_number', 'account_number', 'reason_code', 'transaction_date', 'chargeback_initiated_date', 'activity_date',
                                         'current_action_chargeback_amount', 'fee_amount', 'usage_code', 'unknown', 'mop', 'authorization_date', 'chargeback_due_date', 'ticket_number',
                                         'bundled_chargebacks', 'token_indicator'])
        df = df[df['category'] == 'RTM']
        
        df['issuer_chargeback_amount'] = pd.to_numeric(df['issuer_chargeback_amount'], errors='coerce') #explicitly cast issuer_chargeback_amount to float
        df = df[df['issuer_chargeback_amount'] <= -1] #filter amounts for less than $1, all RTM values are negative

        rtm_df_count = len(df)

        file_creation_date = datetime.now().strftime('%Y%m%d%H%M%S')  
        zip_file_creation_time = datetime.now().strftime('%H%M%S') 
        zip_file_date = extract_date_from_filename(file_name)
        zip_file_datetime = f"{zip_file_date}{zip_file_creation_time}"

        if rtm_count == 0:
            # Write log and skip processing if no RTM/10.4 records are found
            write_log_to_gcp(storage_client, bucket_name, df, file_name, rtm_count, zip_file_date)
            continue

        # Generate header, index header, index details, and trailer
        header_content = create_header_file_content()
        index_header_content = create_index_header()
        index_detail_records = create_index_detail(df, storage_client)
        index_trailer_content = create_index_trailer(index_detail_records)
        pdfs = create_index_detail(df, storage_client)

        # Define paths for output files
        header_file_path = f'/tmp/0000078319.{file_creation_date}.txt'
        index_file_path = f'/tmp/{file_creation_date}.078319.txt'
        original_zip_path = '/tmp/original_zip.zip'
        zip_file_path = f'/tmp/0000078319.{zip_file_datetime}.zip'
        

        # Write contents to files
        with open(header_file_path, 'w') as f:
            f.write(header_content)

        with open(index_file_path, 'wb') as f:  # Open file in binary mode
            f.write(index_header_content.encode('utf-8')+b'\n')  # Convert to bytes and add newline
            for record in index_detail_records[0]:  # Assuming each record is a list of strings
                record_line = ''.join(record) +'\n'  # Concatenate record elements and add newline
                f.write(record_line.encode('utf-8'))  # Convert the entire line to bytes and write
            f.write(index_trailer_content.encode('utf-8'))  # Convert trailer to bytes and write


        # Create zip file
        create_zip_file(index_file_path,pdfs)
        
        #prepend_header_to_zip(header_file_path, zip_file_path, final_zip_path)
        prepend_text_to_zip(header_file_path, original_zip_path, zip_file_path)
        
        # Upload the zip file to the GCP bucket
        upload_file_to_gcp(storage_client, bucket_name, zip_file_path, zip_file_datetime)

        upload_file_to_aws(s3, bucket_name_s3, subfolder_s3, zip_file_path, zip_file_datetime)

        # Log output details
        print(f"Header file created at: {header_file_path}")
        print(f"Index file created at: {index_file_path}")
        print(f"Zip file created at: {zip_file_path}")

        # Update the log with the count of RTM/10.4 records
        write_log_to_gcp(storage_client, bucket_name, df, file_name, rtm_df_count, zip_file_date)

        # Pause for 10 seconds before processing the next file
        time.sleep(10)

    # Update the last run time at the end of the script
    write_last_run_time_to_gcp(storage_client, bucket_name, last_run_time_file)