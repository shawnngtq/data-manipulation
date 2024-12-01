import os
import sys
from typing import Dict, List, Optional, Tuple

from loguru import logger


def init_django(
    django_dir: str,
    project_name: Optional[str] = None,
) -> None:
    """Initializes Django environment for external Python scripts or notebooks.

    Args:
        django_dir (str): Path to Django project directory.
        project_name (Optional[str]): Name of the Django project. If None, will check DJANGO_PROJECT env var.

    Raises:
        ValueError: If project_name is not provided and DJANGO_PROJECT environment variable is not set.
        ImportError: If Django cannot be imported or setup fails.
    """
    try:
        import django
    except ImportError as e:
        logger.error(f"Failed to import Django: {e}")
        raise ImportError("Django is not installed") from e

    project_name = project_name or os.environ.get("DJANGO_PROJECT")
    if not project_name:
        raise ValueError(
            "Set an environment variable: `DJANGO_PROJECT=your_project_name` or call `init_django(your_project_name)`"
        )

    try:
        sys.path.insert(0, django_dir)
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", f"{project_name}.settings")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
    except Exception as e:
        logger.error(f"Failed to setup Django: {e}")
        raise


def get_django_countries_dict() -> Tuple[Dict[str, str], Dict[str, str]]:
    """Retrieves dictionaries mapping country codes to names and vice versa.

    Returns:
        Tuple[Dict[str, str], Dict[str, str]]: A tuple containing:
            - code_name: Dict mapping country codes to uppercase country names
            - name_code: Dict mapping uppercase country names to codes

    Raises:
        ImportError: If django-countries package is not installed.
    """
    try:
        from django_countries import countries
    except ImportError as e:
        logger.error("django-countries package is not installed")
        raise ImportError("Please install django-countries package") from e

    code_name = {k: v.upper() for k, v in dict(countries).items()}
    name_code = {v: k for k, v in code_name.items()}
    return code_name, name_code


def django_validate_email(
    email: str,
    whitelist_domains: Optional[List[str]] = None,
) -> Optional[str]:
    """Validates an email address using Django's validator.

    Args:
        email (str): Email address to validate.
        whitelist_domains (Optional[List[str]]): List of allowed email domains.

    Returns:
        Optional[str]: The validated email address if valid, None if invalid.

    Examples:
        >>> django_validate_email("valid@email.com")
        'valid@email.com'
        >>> django_validate_email("valid@email.com", whitelist_domains=["email.com"])
        'valid@email.com'
        >>> print(django_validate_email("valid@email.com", whitelist_domains=["other.com"]))
        None
    """
    from django.core.validators import validate_email

    try:
        validate_email(email)
        if whitelist_domains:
            domain = email.split("@")[1].lower()
            if domain not in whitelist_domains:
                logger.warning(f"Email domain {domain} not in whitelist")
                return None
        return email
    except Exception as e:
        logger.debug(f"Email validation failed: {e}")
        return None


def django_validate_url(
    url: str,
    allowed_schemes: Optional[List[str]] = None,
) -> Optional[str]:
    """Validates a URL using Django's URL validator.

    Args:
        url (str): URL to validate.
        allowed_schemes (Optional[List[str]]): List of allowed URL schemes (e.g., ['http', 'https']).

    Returns:
        Optional[str]: The validated URL if valid, None if invalid.
    """
    from django.core.validators import URLValidator

    validator = URLValidator(
        schemes=allowed_schemes if allowed_schemes else ["http", "https", "ftp", "ftps"]
    )
    try:
        validator(url)
        return url
    except Exception as e:
        logger.debug(f"URL validation failed: {e}")
        return None


def django_validate_phone(
    phone: str,
    region: Optional[str] = None,
) -> Optional[str]:
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
