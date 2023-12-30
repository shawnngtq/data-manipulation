def init_django(django_dir: str, project_name: str) -> None:
    """
    Setup Django in Jupyter. Reference from https://gist.github.com/codingforentrepreneurs/76e570d759f83d690bf36a8a8fa4cfbe

    Parameters
    ----------
    django_dir : str
        Django project location
    project_name : str
        Django project name
    """
    import os
    import sys

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


def get_django_countries_dict():
    """
    Get django_countries package's dictionaries
        code_name - "SG": "SINGAPORE"
        name_code - "SINGAPORE": "SG"
    """
    from django_countries import countries

    code_name = {k: v.upper() for k, v in dict(countries).items()}
    name_code = {v: k for k, v in code_name.items()}
    return code_name, name_code


def django_validate_email(email: str) -> str | None:
    """
    Django validate email

    Parameters
    ----------
    email : str
        email to check

    Returns
    -------
    str | None
        email or none
    """
    from django.core.validators import validate_email

    try:
        validate_email(email)
        return email
    except Exception as e:
        return


def django_validate_url(url: str) -> str | None:
    """
    Django validate url

    Parameters
    ----------
    url : str
        url to validate

    Returns
    -------
    str | None
        url or None
    """
    from django.core.validators import URLValidator

    validator = URLValidator()
    try:
        validator(url)
        return url
    except Exception as e:
        return


def django_validate_phone(phone: str, region=None) -> str | None:
    """
    Django validate phone

    Parameters
    ----------
    phone : str
        phone to check
    region : _type_, optional
        phone region, by default None

    Returns
    -------
    str | None
        parsed phone or none
    """
    from phonenumber_field.phonenumber import PhoneNumber

    try:
        return PhoneNumber.from_string(phone).as_e164
    except:
        try:
            return PhoneNumber.from_string(phone, region=region).as_e164
        except Exception as e:
            print(e)
            return


if __name__ == "__main__":
    import doctest

    doctest.testmod()
