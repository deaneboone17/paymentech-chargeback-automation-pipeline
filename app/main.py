from flask import Blueprint, jsonify
from datetime import datetime
import pandas as pd
import time

from .utils import (
    access_secret_version,
    access_secret_version_csv,
    establish_gcs_connection,
    establish_s3_connection,
    read_last_run_time_from_gcp,
    list_and_filter_files,
    read_file_from_gcp,
    write_log_to_gcp,
    create_header_file_content,
    create_index_header,
    create_index_detail,
    create_index_trailer,
    create_zip_file,
    prepend_text_to_zip,
    upload_file_to_gcp,
    upload_file_to_aws,
    write_last_run_time_to_gcp,
    extract_date_from_filename,
)

bp = Blueprint("main", __name__)

bucket_name = "ramove"
dfr_bucket = "reporting.raw.charonraassurance.pandora.com"
pdf_template_file = (
    "chargeback_automation/terms_conditions/SiriusXM_Customer_Agreement_2024_Legacy.pdf"
)
last_run_time_file = "chargeback_automation/last_run/last_run_time.txt"
dfr_prefix = "paymentech/dfr_a"


@bp.route("/run", methods=["GET"])
def chargeback_handler():
    try:
        project_id = "1072076586660"
        secret_id_gcp = "ra_dev_cred"
        secret_id_aws = "s3-sxm-ctrl-rev-dev"
        version_id = "latest"
        bucket_name_s3 = "sxm-ctrl-rev-dev"
        subfolder_s3 = "chargeback_automation"

        gcp_credentials = access_secret_version(project_id, secret_id_gcp, version_id)
        aws_access_key, aws_secret_key = access_secret_version_csv(
            project_id, secret_id_aws, version_id
        )

        storage_client = establish_gcs_connection(gcp_credentials)
        s3 = establish_s3_connection(aws_access_key, aws_secret_key)

        last_run_time = read_last_run_time_from_gcp(
            storage_client, bucket_name, last_run_time_file
        )
        filtered_files = list_and_filter_files(
            storage_client, dfr_bucket, dfr_prefix, last_run_time
        )

        for file_name in filtered_files:
            file_content = read_file_from_gcp(storage_client, dfr_bucket, file_name)
            data = []
            rtm_count = 0

            codes = [
                "13.1", "07", "08", "12", "31", "34", "37", "40", "41", "42", "46", "49",
                "53", "54", "55", "59", "60", "63", "70", "71", "10.1", "10.2", "10.3",
                "10.5", "11.1", "11.2", "11.3", "12.1", "12.2", "12.3", "12.4", "12.5",
                "12.6.1", "12.6.2", "12.7", "13.2", "13.3", "13.4", "13.5", "13.6",
                "13.7", "13.8", "13.9"
            ]

            for line in file_content.splitlines():
                if any(f"|{code}|" in line for code in codes) and "RTM" in line and "USD" in line:
                    parsed_line = [field.strip() for field in line.split("|")]
                    data.append(parsed_line)
                    rtm_count += 1

            df = pd.DataFrame(
                data,
                columns=[
                    "record_type", "entity_type", "entity_number", "issuer_chargeback_amount",
                    "partial_representment", "presentment_currency", "category", "status_flag",
                    "sequence_number", "merchant_order_number", "account_number", "reason_code",
                    "transaction_date", "chargeback_initiated_date", "activity_date",
                    "current_action_chargeback_amount", "fee_amount", "usage_code",
                    "unknown", "mop", "authorization_date", "chargeback_due_date",
                    "ticket_number", "bundled_chargebacks", "token_indicator",
                ],
            )

            df = df[df["category"] == "RTM"]
            df["issuer_chargeback_amount"] = pd.to_numeric(
                df["issuer_chargeback_amount"], errors="coerce"
            )
            df = df[df["issuer_chargeback_amount"] <= -1]
            rtm_df_count = len(df)

            file_creation_date = datetime.now().strftime("%Y%m%d%H%M%S")
            zip_file_creation_time = datetime.now().strftime("%H%M%S")
            zip_file_date = extract_date_from_filename(file_name)
            zip_file_datetime = f"{zip_file_date}{zip_file_creation_time}"

            if rtm_count == 0:
                write_log_to_gcp(
                    storage_client, bucket_name, df, file_name, rtm_count, zip_file_date
                )
                continue

            header_content = create_header_file_content()
            index_header_content = create_index_header()
            index_detail_records = create_index_detail(df, storage_client)
            index_trailer_content = create_index_trailer(index_detail_records)

            header_file_path = f"/tmp/0000078319.{file_creation_date}.txt"
            index_file_path = f"/tmp/{file_creation_date}.078319.txt"
            original_zip_path = "/tmp/original_zip.zip"
            zip_file_path = f"/tmp/0000078319.{zip_file_datetime}.zip"

            with open(header_file_path, "w") as f:
                f.write(header_content)

            with open(index_file_path, "wb") as f:
                f.write(index_header_content.encode("utf-8") + b"\n")
                for record in index_detail_records[0]:
                    f.write((record + "\n").encode("utf-8"))
                f.write(index_trailer_content.encode("utf-8"))

            create_zip_file(index_file_path, index_detail_records)
            prepend_text_to_zip(header_file_path, original_zip_path, zip_file_path)
            upload_file_to_gcp(storage_client, bucket_name, zip_file_path, zip_file_datetime)
            upload_file_to_aws(
                s3, bucket_name_s3, subfolder_s3, zip_file_path, zip_file_datetime
            )
            write_log_to_gcp(
                storage_client, bucket_name, df, file_name, rtm_df_count, zip_file_date
            )

            time.sleep(10)

        write_last_run_time_to_gcp(storage_client, bucket_name, last_run_time_file)

        return jsonify({"status": "success", "processed_files": len(filtered_files)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
