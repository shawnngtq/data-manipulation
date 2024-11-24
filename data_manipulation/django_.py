import os
import sys
from typing import Optional

from loguru import logger


def init_django(django_dir: str, project_name: str) -> None:
    """Initializes Django environment for external Python scripts or notebooks.

    Args:
        django_dir (str): Path to Django project directory.
        project_name (str): Name of the Django project.

    Raises:
        Exception: If project_name is not provided and DJANGO_PROJECT environment variable is not set.

    Note:
        Reference: https://gist.github.com/codingforentrepreneurs/76e570d759f83d690bf36a8a8fa4cfbe
    """

    import django

    # django_dir = os.getenv("PWD")
    # os.chdir(django_dir)

    project_name = project_name or os.environ.get("DJANGO_PROJECT") or None
    if not project_name:
        raise Exception(
            """Set an environment variable: `DJANGO_PROJECT=your_project_name` or call `init_django(your_project_name)`"""
        )

    sys.path.insert(0, django_dir)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", f"{project_name}.settings")
    os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    django.setup()


def get_django_countries_dict() -> tuple[dict, dict]:
    """Retrieves dictionaries mapping country codes to names and vice versa.

    Returns:
        tuple[dict, dict]: A tuple containing two dictionaries:
            - code_name: Dict mapping country codes to uppercase country names (e.g., "SG": "SINGAPORE")
            - name_code: Dict mapping uppercase country names to codes (e.g., "SINGAPORE": "SG")

    Note:
        Requires django-countries package to be installed.
    """
    from django_countries import countries

    code_name = {k: v.upper() for k, v in dict(countries).items()}
    name_code = {v: k for k, v in code_name.items()}
    return code_name, name_code


def django_validate_email(email: str) -> Optional[str]:
    """Validates an email address using Django's validator.

    Args:
        email (str): Email address to validate.

    Returns:
        Optional[str]: The validated email address if valid, None if invalid.

    Examples:
        >>> django_validate_email("valid@email.com")
        'valid@email.com'
        >>> print(django_validate_email("invalid@email"))
        None
    """
    from django.core.validators import validate_email

    try:
        validate_email(email)
        return email
    except Exception as e:
        return


def django_validate_url(url: str) -> Optional[str]:
    """Validates a URL using Django's URL validator.

    Args:
        url (str): URL to validate.

    Returns:
        Optional[str]: The validated URL if valid, None if invalid.

    Examples:
        >>> django_validate_url("https://www.example.com")
        'https://www.example.com'
        >>> print(django_validate_url("invalid-url"))
        None
    """
    from django.core.validators import URLValidator

    validator = URLValidator()
    try:
        validator(url)
        return url
    except Exception as e:
        return


def django_validate_phone(phone: str, region: Optional[str] = None) -> Optional[str]:
    """Validates and formats a phone number using Django's phone number field.

    Args:
        phone (str): Phone number to validate.
        region (Optional[str], optional): Region code for parsing local numbers. Defaults to None.

    Returns:
        Optional[str]: The phone number in E.164 format if valid, None if invalid.

    Examples:
        >>> django_validate_phone("+1234567890")
        '+1234567890'
        >>> print(django_validate_phone("invalid"))
        None

    Note:
        Requires django-phonenumber-field package to be installed.
    """
    from phonenumber_field.phonenumber import PhoneNumber

    try:
        return PhoneNumber.from_string(phone).as_e164
    except:
        try:
            return PhoneNumber.from_string(phone, region=region).as_e164
        except Exception as e:
            logger.error(e)
            return


if __name__ == "__main__":
    import doctest

    doctest.testmod()
