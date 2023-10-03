import logging
import subprocess


def ldapsearch(
    search_base: str,
    ldap_uri: str,
    bind_dn: str,
    password: str,
    filter: str,
) -> subprocess.CompletedProcess:
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
