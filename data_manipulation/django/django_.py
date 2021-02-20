def init_django(project_name=None):
    """
    Django for jupyter
    https://gist.github.com/codingforentrepreneurs/76e570d759f83d690bf36a8a8fa4cfbe

    Parameters
    ----------
    project_name: str
        Django project name
    """
    import django
    import os
    import sys

    present_working_dir = os.getenv('PWD')
    project_missing_msg = """
    Set an environment variable:\n
    `DJANGO_PROJECT=your_project_name`\n
    or call:\n
    `init_django(your_project_name)`
    """

    os.chdir(present_working_dir)
    project_name = project_name or os.environ.get('DJANGO_PROJECT') or None
    if not project_name:
        raise Exception(project_missing_msg)
    sys.path.insert(0, os.getenv('PWD'))
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', f'{project_name}.settings')
    os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    django.setup()


if __name__ == "__main__":
    import doctest

    doctest.testmod()
