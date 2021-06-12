def init_django(project_name):
    """
    Setup Django in Jupyter. Reference from https://gist.github.com/codingforentrepreneurs/76e570d759f83d690bf36a8a8fa4cfbe

    Parameters
    ----------
    project_name: str
        Django project name

    Returns
    -------
    None
    """
    import os
    import sys
    import django

    present_working_dir = os.getenv("PWD")
    os.chdir(present_working_dir)

    project_name = project_name or os.environ.get("DJANGO_PROJECT") or None
    if not project_name:
        raise Exception("""
        Set an environment variable: `DJANGO_PROJECT=your_project_name` or call `init_django(your_project_name)`
        """)
    
    sys.path.insert(0, present_working_dir)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", f"{project_name}.settings")
    os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    django.setup()


if __name__ == "__main__":
    import doctest

    doctest.testmod()
