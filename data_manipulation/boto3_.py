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
    """
    Send AWS SES email

    Parameters
    ----------
    sender : str
        sender email
    recipient : list
        list of recipient emails
    subject : str
        email subject
    body_text : str
        email body
    body_type : str
        email body type
    ses_client : _type_
        aws ses client
    attachment : str, optional
        attachment path, by default None

    Returns
    -------
    dict
        aws ses client email response or none

    Reference
    ---------
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ses/client/send_raw_email.html
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
    """
    List s3 bucket files

    Parameters
    ----------
    bucket : str
        bucket name
    to_dateframe : bool, optional
        to convert to pandas dataframe, by default False

    Returns
    -------
    list | pandas.DataFrame
        default list, pandas.DataFrame if requested
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
