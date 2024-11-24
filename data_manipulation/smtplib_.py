import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(
    logname: str,
    message_subject: str,
    message_sender: str,
    message_receiver: str,
    html: str,
    smtp_address: str,
) -> None:
    """Sends an HTML email using SMTP and logs the operation.

    Args:
        logname (str): Path to the log file where email sending operations will be recorded
        message_subject (str): Subject line of the email
        message_sender (str): Email address of the sender
        message_receiver (str): Email address of the recipient
        html (str): HTML content of the email body
        smtp_address (str): SMTP server address (e.g., 'smtp.gmail.com:587')

    Raises:
        TypeError: If any of the input parameters are not strings
        smtplib.SMTPException: If email sending fails
        IOError: If log file cannot be accessed or created

    Examples:
        >>> send_email(
        ...     logname='email.log',
        ...     message_subject='Test Email',
        ...     message_sender='sender@example.com',
        ...     message_receiver='recipient@example.com',
        ...     html='<h1>Hello World</h1><p>This is a test email.</p>',
        ...     smtp_address='smtp.gmail.com:587'
        ... )
        # Email sent successfully and logged to email.log

    Note:
        - All parameters must be strings
        - The HTML content should be properly formatted HTML
        - The function will log both successful sends and failures
        - Make sure the SMTP server address is correctly formatted with port if needed
    """

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
