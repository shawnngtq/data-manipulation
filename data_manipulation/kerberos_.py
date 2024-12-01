import os
import shlex
import subprocess

from loguru import logger


def keytab_valid(
    keytab_filepath: str,
    principal_name: str,
) -> subprocess.CompletedProcess[str]:
    """Validates a Kerberos keytab file using kinit.

    Args:
        keytab_filepath (str): Path to the Kerberos keytab file.
        principal_name (str): Kerberos principal name associated with the keytab.

    Returns:
        subprocess.CompletedProcess[str]: Result of the kinit command execution containing:
            - returncode: 0 if successful, non-zero if failed
            - stdout: Standard output from the command
            - stderr: Standard error from the command

    Raises:
        ValueError: If keytab file doesn't exist or inputs are invalid
        FileNotFoundError: If kinit command is not available
        subprocess.SubprocessError: If command execution fails

    Examples:
        >>> result = keytab_valid("/path/to/keytab", "user@REALM.COM")
        >>> result.returncode == 0  # True if keytab is valid
        True

    """
    # Input validation
    if not keytab_filepath or not principal_name:
        raise ValueError("Both keytab_filepath and principal_name must be provided")

    if not os.path.isfile(keytab_filepath):
        raise ValueError(f"Keytab file not found: {keytab_filepath}")

    # Safely quote the arguments
    safe_keytab = shlex.quote(keytab_filepath)
    safe_principal = shlex.quote(principal_name)

    try:
        # Use list form with shell=False for better security
        cmd = ["kinit", "-kt", safe_keytab, safe_principal]
        logger.debug(f"Executing command: {' '.join(cmd)}")

        output = subprocess.run(
            cmd,
            capture_output=True,
            shell=False,
            text=True,
            check=False,  # Don't raise on non-zero exit
        )

        if output.returncode == 0:
            logger.info(
                "Keytab validation successful",
                keytab=keytab_filepath,
                principal=principal_name,
            )
        else:
            logger.error(
                "Keytab validation failed",
                keytab=keytab_filepath,
                principal=principal_name,
                stderr=output.stderr,
            )

        return output

    except FileNotFoundError:
        logger.error("kinit command not found. Is Kerberos installed?")
        raise
    except subprocess.SubprocessError as e:
        logger.error(f"Command execution failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during keytab validation: {str(e)}")
        raise


if __name__ == "__main__":
    import doctest

    doctest.testmod()
