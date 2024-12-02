import subprocess
from urllib.parse import urlparse


def validate_ldap_uri(uri: str) -> bool:
    """Validate LDAP URI format."""
    try:
        parsed = urlparse(uri)
        return parsed.scheme in ("ldap", "ldaps") and bool(parsed.netloc)
    except ValueError:
        return False


def ldapsearch(
    search_base: str,
    ldap_uri: str,
    bind_dn: str,
    password: str,
    search_filter: str,
) -> subprocess.CompletedProcess:
    """Executes an LDAP search query using the ldapsearch command.

    Args:
        search_base (str): Base DN for the search operation.
        ldap_uri (str): LDAP server URI (e.g., "ldap://example.com:389").
        bind_dn (str): Distinguished Name (DN) for binding to the LDAP server.
        password (str): Password for authentication.
        search_filter (str): LDAP search filter (e.g., "(objectClass=person)").

    Returns:
        subprocess.CompletedProcess: Result of the ldapsearch command execution.

    Raises:
        ValueError: If any input parameters are empty or invalid.
        subprocess.SubprocessError: If the ldapsearch command fails to execute.
        FileNotFoundError: If ldapsearch command is not available.

    Examples:
        >>> result = ldapsearch(
        ...     "dc=example,dc=com",
        ...     "ldap://ldap.example.com",
        ...     "cn=admin,dc=example,dc=com",
        ...     "password",
        ...     "(objectClass=person)"
        ... )
        >>> result.returncode == 0  # True if search was successful
        True
    """
    # Input validation
    if not all([search_base, ldap_uri, bind_dn, password, search_filter]):
        raise ValueError("All parameters must be non-empty strings")

    if not validate_ldap_uri(ldap_uri):
        raise ValueError(f"Invalid LDAP URI format: {ldap_uri}")

    try:
        # Use list arguments instead of shell=True for security
        cmd = [
            "ldapsearch",
            "-b",
            search_base,
            "-H",
            ldap_uri,
            "-D",
            bind_dn,
            "-w",
            password,
            search_filter,
        ]

        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,  # Don't raise on non-zero exit
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "ldapsearch command not found. Please install OpenLDAP client tools."
        )
    except subprocess.SubprocessError as e:
        raise subprocess.SubprocessError(f"Failed to execute ldapsearch: {str(e)}")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
