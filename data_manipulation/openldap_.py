import subprocess


def ldapsearch(
    search_base: str,
    ldap_uri: str,
    bind_dn: str,
    password: str,
    filter: str,
) -> subprocess.CompletedProcess:
    """Executes an LDAP search query using the ldapsearch command.

    Args:
        search_base (str): Base DN for the search operation.
        ldap_uri (str): LDAP server URI (e.g., "ldap://example.com:389").
        bind_dn (str): Distinguished Name (DN) for binding to the LDAP server.
        password (str): Password for authentication.
        filter (str): LDAP search filter (e.g., "(objectClass=person)").

    Returns:
        subprocess.CompletedProcess: Result of the ldapsearch command execution containing:
            - returncode: 0 if successful, non-zero if failed
            - stdout: Standard output containing search results
            - stderr: Standard error messages if any

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

    Note:
        Requires OpenLDAP client tools to be installed (ldapsearch command).
        Command is executed through subprocess with shell=True, use caution with input validation.
    """
    cmd = (
        "ldapsearch -b {search_base} -H {ldap_uri} -D {bind_dn} -w {password} {filter}"
    )
    output = subprocess.run(
        cmd,
        capture_output=True,
        shell=True,
        text=True,
    )
    return output


if __name__ == "__main__":
    import doctest

    doctest.testmod()
