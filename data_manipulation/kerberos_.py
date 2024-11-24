import subprocess

from loguru import logger


def keytab_valid(
    keytab_filepath: str,
    principal_name: str,
) -> subprocess.CompletedProcess:
    """Validates a Kerberos keytab file using kinit.

    Args:
        keytab_filepath (str): Path to the Kerberos keytab file.
        principal_name (str): Kerberos principal name associated with the keytab.

    Returns:
        subprocess.CompletedProcess: Result of the kinit command execution containing:
            - returncode: 0 if successful, non-zero if failed
            - stdout: Standard output from the command
            - stderr: Standard error from the command

    Examples:
        >>> result = keytab_valid("/path/to/keytab", "user@REALM.COM")
        >>> result.returncode == 0  # True if keytab is valid
        True

    Note:
        Requires a working Kerberos installation with kinit command available.
        Logs the validation result using loguru logger.
    """

    output = subprocess.run(
        f"kinit -kt {keytab_filepath} {principal_name}",
        capture_output=True,
        shell=True,
        text=True,
    )
    try:
        if output.returncode == 0:
            logger.info("keytab_valid: True")
        else:
            logger.info("keytab_valid: False")
    except Exception as e:
        logger.error(f"keytab_valid: False ({e})")
    return output


if __name__ == "__main__":
    import doctest

    doctest.testmod()
