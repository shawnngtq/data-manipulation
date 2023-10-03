import logging
import subprocess


def keytab_valid(
    keytab_filepath: str,
    principal_name: str,
) -> subprocess.CompletedProcess:
    """
    Check keytab validity

    Parameters
    ----------
    keytab_filepath : str
        Keytab linux filepath
    principal_name : str
        Keytab principal name

    Returns
    -------
    subprocess.CompletedProcess
        Subprocess object
    """
    output = subprocess.run(
        f"kinit -kt {keytab_filepath} {principal_name}",
        capture_output=True,
        shell=True,
        text=True,
    )
    try:
        if output.returncode == 0:
            logging.info("keytab_valid: True")
        else:
            logging.info("keytab_valid: False")
    except Exception as e:
        logging.error(f"keytab_valid: False ({e})")
    return output


if __name__ == "__main__":
    import doctest

    doctest.testmod()
