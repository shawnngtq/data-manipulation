from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP, SMTPException
from typing import Optional

from loguru import logger


def send_email(
    logname: str,
    message_subject: str,
    message_sender: str,
    message_receiver: str,
    html: str,
    smtp_address: str,
    smtp_port: int = 587,
    smtp_username: Optional[str] = None,
    smtp_password: Optional[str] = None,
) -> bool:
    """Sends an HTML email using SMTP with TLS and logs the operation.

    Args:
        logname (str): Path to the log file
        message_subject (str): Subject line of the email
        message_sender (str): Email address of the sender
        message_receiver (str): Email address of the recipient
        html (str): HTML content of the email body
        smtp_address (str): SMTP server address (e.g., 'smtp.gmail.com')
        smtp_port (int, optional): SMTP server port. Defaults to 587.
        smtp_username (str, optional): SMTP authentication username
        smtp_password (str, optional): SMTP authentication password

    Returns:
        bool: True if email was sent successfully, False otherwise

    Raises:
        ValueError: If required parameters are empty or invalid
        SMTPException: If email sending fails
        IOError: If log file cannot be accessed
    """
    # Input validation
    if not all(
        [logname, message_subject, message_sender, message_receiver, html, smtp_address]
    ):
        raise ValueError("Required parameters cannot be empty")

    # Prepare email message
    message = MIMEMultipart("alternative")
    message["Subject"] = message_subject
    message["From"] = message_sender
    message["To"] = message_receiver
    message.attach(MIMEText(html, "html"))

    try:
        with SMTP(smtp_address, smtp_port) as server:
            logger.info(f"Connecting to SMTP server: {smtp_address}:{smtp_port}")

            # Enable TLS encryption
            server.starttls()

            # Authenticate if credentials provided
            if smtp_username and smtp_password:
                logger.debug("Attempting SMTP authentication")
                server.login(smtp_username, smtp_password)

            # Send email
            server.send_message(message)
            logger.info(f"Email sent successfully to {message_receiver}")
            return True

    except SMTPException as e:
        logger.error(f"SMTP error occurred: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return False


if __name__ == "__main__":
    import doctest

    doctest.testmod()
