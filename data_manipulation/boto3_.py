import os
from typing import Any, Dict, List, Optional, Union

from loguru import logger

# Constants - now serving as defaults
DEFAULT_MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024  # 10MB
DEFAULT_MAX_RETRIES = 3


def send_aws_ses_email(
    sender: str,
    recipient: List[str],
    subject: str,
    body_text: str,
    body_type: str,
    ses_client: Any,
    attachment: Optional[str] = None,
    max_attachment_size: int = DEFAULT_MAX_ATTACHMENT_SIZE,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> Optional[Dict[str, Any]]:
    """Sends an email using AWS SES service.

    Args:
        sender (str): Sender's email address.
        recipient (list): List of recipient email addresses.
        subject (str): Email subject line.
        body_text (str): Email body content.
        body_type (str): MIME type of email body (e.g., 'plain', 'html').
        ses_client: AWS SES client instance.
        attachment (str, optional): Path to file to attach. Defaults to None.
        max_attachment_size (int, optional): Maximum allowed attachment size in bytes.
            Defaults to 10MB.
        max_retries (int, optional): Maximum number of retry attempts for failed sends.
            Defaults to 3.

    Returns:
        dict: AWS SES response dictionary if successful, None if failed.

    Note:
        Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ses/client/send_raw_email.html
    """
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    import boto3.exceptions
    from botocore.exceptions import BotoCoreError, ClientError

    # Input validation
    if not sender or not recipient or not subject or not body_text:
        logger.error("Missing required email parameters")
        return None

    if not isinstance(recipient, list):
        logger.error("Recipient must be a list")
        return None

    if body_type not in ["plain", "html"]:
        logger.error("Invalid body type. Must be 'plain' or 'html'")
        return None

    # Validate retry count
    if max_retries < 1:
        logger.error("max_retries must be at least 1")
        return None

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipient)

    part = MIMEText(body_text, body_type)
    msg.attach(part)

    if attachment:
        try:
            if not os.path.exists(attachment):
                logger.error(f"Attachment not found: {attachment}")
                return None

            # Check file size with parameterized max size
            if os.path.getsize(attachment) > max_attachment_size:
                logger.error(
                    f"Attachment size exceeds maximum allowed size of {max_attachment_size/1024/1024}MB"
                )
                return None

            with open(attachment, "rb") as f:
                part = MIMEApplication(f.read())
                part.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=os.path.basename(attachment),
                )
                msg.attach(part)
        except (IOError, OSError) as e:
            logger.error(f"Error processing attachment: {str(e)}")
            return None

    # Implement retry logic with parameterized max retries
    for attempt in range(max_retries):
        try:
            response = ses_client.send_raw_email(
                Source=sender,
                Destinations=recipient,
                RawMessage={"Data": msg.as_string()},
            )
            logger.info(f"Email sent successfully on attempt {attempt + 1}")
            return response
        except (BotoCoreError, ClientError) as error:
            if attempt == max_retries - 1:
                logger.error(
                    f"Failed to send email after {max_retries} attempts: {error}"
                )
                return None
            logger.warning(f"Attempt {attempt + 1} failed, retrying...")
            continue


def list_s3_bucket_files(
    bucket: str,
    to_dateframe: bool = False,
    prefix: Optional[str] = None,
) -> Union[List[str], "pd.DataFrame"]:
    """Lists all files in an S3 bucket.

    Args:
        bucket (str): Name of the S3 bucket.
        to_dateframe (bool, optional): Whether to return results as pandas DataFrame. Defaults to False.
        prefix (Optional[str], optional): Filter results to files with this prefix. Defaults to None.

    Returns:
        Union[List[str], pd.DataFrame]: List of file keys or DataFrame containing file keys.
            If to_dateframe is True, returns DataFrame with 'key' column.
            If to_dateframe is False, returns list of file keys.

    Examples:
        >>> files = list_s3_bucket_files('my-bucket')
        >>> type(files)
        <class 'list'>

        >>> df = list_s3_bucket_files('my-bucket', to_dateframe=True)
        >>> type(df)
        <class 'pandas.core.frame.DataFrame'>
    """
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    if not bucket:
        raise ValueError("Bucket name cannot be empty")

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []

    try:
        params = {"Bucket": bucket}
        if prefix:
            params["Prefix"] = prefix

        for page in paginator.paginate(**params):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                if obj["Key"].endswith("/"):
                    continue
                keys.append(obj["Key"])

    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error accessing S3 bucket {bucket}: {str(e)}")
        raise

    if to_dateframe:
        import pandas as pd

        return pd.DataFrame(keys, columns=["key"])
    return keys


if __name__ == "__main__":
    import doctest

    doctest.testmod()
