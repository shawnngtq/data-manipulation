import os

from cryptography.fernet import Fernet
from loguru import logger


def generate_fernet_key(output_directory: str, output_filename: str) -> bytes:
    """Generates and saves a Fernet encryption key.

    Args:
        output_directory (str): Directory path where the key file will be saved.
        output_filename (str): Name of the key file to be created.

    Returns:
        bytes: Generated Fernet key in bytes format.

    Raises:
        Exception: If key generation or file writing fails.

    Examples:
        >>> key = generate_fernet_key('/path/to/keys', 'encryption.key')
        >>> isinstance(key, bytes)
        True
    """

    key = None
    if os.path.isdir(output_directory):
        try:
            key = Fernet.generate_key()
            filepath = os.path.join(output_directory, output_filename)
            f = open(filepath, "wb")
            f.write(key)
            f.close()
            logger.info(f"generate_fernet_key: True")
        except Exception as e:
            logger.error(f"generate_fernet_key: False ({e})")
    return key


def encrypt_fernet_file(keypath: str, filepath: str) -> str:
    """Encrypts a file using Fernet symmetric encryption.

    Args:
        keypath (str): Path to the Fernet key file.
        filepath (str): Path to the file to be encrypted.

    Returns:
        str: Encrypted data.

    Raises:
        TypeError: If keypath or filepath are not strings.

    Examples:
        >>> encrypted = encrypt_fernet_file('key.txt', 'data.txt')
        >>> isinstance(encrypted, str)
        True
    """
    if isinstance(keypath, str) and isinstance(filepath, str):
        fernet = Fernet(open(keypath, "rb").read())
        data = open(filepath, "rb").read()
        encrypt_data = fernet.encrypt(data)
        return encrypt_data
    else:
        raise TypeError("Wrong datatype(s)")


def decrypt_fernet_data(keypath: str, filepath: str) -> str:
    """Decrypts a file using Fernet symmetric encryption.

    Args:
        keypath (str): Path to the Fernet key file.
        filepath (str): Path to the encrypted file.

    Returns:
        str: Decrypted data.

    Raises:
        TypeError: If keypath or filepath are not strings.

    Examples:
        >>> decrypted = decrypt_fernet_data('key.txt', 'encrypted_data.txt')
        >>> isinstance(decrypted, str)
        True
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
