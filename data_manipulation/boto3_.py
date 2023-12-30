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
    str
        aws ses client email response or none
    """
    import os
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
            print(f"{attachment} not found")
            return

    try:
        response = ses_client.send_raw_email(
            Source=sender,
            Destinations=recipient,
            RawMessage={
                "Data": msg.as_string(),
            },
        )
    except (BotoCoreError, ClientError) as error:
        print(f"Error: {error}")
        return
    else:
        return response


if __name__ == "__main__":
    import doctest

    doctest.testmod()
