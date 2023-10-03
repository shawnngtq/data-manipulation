import logging

from cryptography.fernet import Fernet


def generate_fernet_key(output_directory: str, output_filename: str) -> bytes:
    """
    Generate cryptography fernet key

    Parameters
    ----------
    output_directory : str
        Linux directory
    output_filename : str
        Linux filename

    Returns
    -------
    bytes
        Fernet key in bytes
    """
    import os

    key = None
    if os.path.isdir(output_directory):
        try:
            key = Fernet.generate_key()
            filepath = os.path.join(output_directory, output_filename)
            f = open(filepath, "wb")
            f.write(key)
            f.close()
            logging.info(f"generate_fernet_key: True")
        except Exception as e:
            logging.error(f"generate_fernet_key: False ({e})")
    return key


def encrypt_fernet_file(keypath: str, filepath: str) -> str:
    """
    Encrypt file

    Parameters
    ----------
    keypath : str
        keypath
    filepath : str
        File path

    Returns
    -------
    str
        Encrypted data
    """
    if isinstance(keypath, str) and isinstance(filepath, str):
        fernet = Fernet(open(keypath, "rb").read())
        data = open(filepath, "rb").read()
        encrypt_data = fernet.encrypt(data)
        return encrypt_data
    else:
        raise TypeError("Wrong datatype(s)")


def decrypt_fernet_data(keypath: str, filepath: str) -> str:
    """
    Decrypt file

    Parameters
    ----------
    keypath : str
        keypath
    filepath : str
        File path

    Returns
    -------
    str
        Decrypted data
    """
    if isinstance(keypath, str) and isinstance(filepath, str):
        fernet = Fernet(open(keypath, "rb").read())
        data = open(filepath, "rb").read()
        decrypt_data = fernet.decrypt(data)
        return decrypt_data
    else:
        raise TypeError("Wrong datatype(s)")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
