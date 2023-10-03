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


if __name__ == "__main__":
    import doctest

    doctest.testmod()
