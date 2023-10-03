def send_email(
    logname: str,
    message_subject: str,
    message_sender: str,
    message_receiver: str,
    html: str,
    smtp_address: str,
) -> None:
    """
    Send html email

    Parameters
    ----------
    logname : str
        Log
    message_subject : str
        Message subject
    message_sender : str
        Sender email
    message_receiver : str
        Receiver email
    html : str
        HTML string
    smtp_address: str
        SMTP address
    """
    import logging
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    if all(
        isinstance(i, str)
        for i in [
            logname,
            message_subject,
            message_sender,
            message_receiver,
            html,
            smtp_address,
        ]
    ):
        logging.basicConfig(
            filename=logname,
            level=logging.DEBUG,
            format="%(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        message = MIMEMultipart("alternative")
        message["Subject"] = message_subject
        message["From"] = message_sender
        message["To"] = message_receiver
        html = MIMEText(html, "html")
        message.attach(html)
        try:
            server = smtplib.SMTP(smtp_address)
            server.sendmail(message_sender, message_receiver, message.as_string())
            server.quit()
            logging.info("Email sent")
        except Exception as e:
            logging.error(f"Email not send: {str(e)}")
    else:
        raise TypeError("Wrong datatype(s)")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
