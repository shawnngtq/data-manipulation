import os

from loguru import logger


def send_aws_ses_email(
    sender: str,
    recipient: list,
    subject: str,
    body_text: str,
    body_type: str,
    ses_client,
    attachment: str = None,
):
    """Sends an email using AWS SES service.

    Args:
        sender (str): Sender's email address.
        recipient (list): List of recipient email addresses.
        subject (str): Email subject line.
        body_text (str): Email body content.
        body_type (str): MIME type of email body (e.g., 'plain', 'html').
        ses_client: AWS SES client instance.
        attachment (str, optional): Path to file to attach. Defaults to None.

    Returns:
        dict: AWS SES response dictionary if successful, None if failed.

    Note:
        Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ses/client/send_raw_email.html
    """
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    from botocore.exceptions import BotoCoreError, ClientError

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipient)

    part = MIMEText(body_text, body_type)
    msg.attach(part)

    if attachment:
        try:
            with open(attachment, "rb") as f:
                part = MIMEApplication(f.read())
                part.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=os.path.basename(attachment),
                )
                msg.attach(part)
        except FileNotFoundError:
            logger.error(f"{attachment} not found")
            return

    try:
        response = ses_client.send_raw_email(
            Source=sender,
            Destinations=recipient,
            RawMessage={
                "Data": msg.as_string(),
            },
        )
        return response
    except (BotoCoreError, ClientError) as error:
        logger.error(f"Error: {error}")
        return


def list_s3_bucket_files(
    bucket: str,
    to_dateframe: bool = False,
):
    """Lists all files in an S3 bucket.

    Args:
        bucket (str): Name of the S3 bucket.
        to_dateframe (bool, optional): Whether to return results as pandas DataFrame. Defaults to False.

    Returns:
        Union[list, pd.DataFrame]: List of file keys or DataFrame containing file keys.
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

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = list()

    for page in paginator.paginate(Bucket=bucket):
        for obj in page["Contents"]:
            if obj["Key"].endswith("/"):
                continue
            keys.append(obj["Key"])

    if to_dateframe:
        import pandas as pd

        df = pd.DataFrame(keys, columns=["key"])
        return df
    else:
        return keys


if __name__ == "__main__":
    import doctest

    doctest.testmod()
