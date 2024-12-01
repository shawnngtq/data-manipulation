import os
from pathlib import Path
from typing import Union

from cryptography.fernet import Fernet, InvalidToken
from loguru import logger


def generate_fernet_key(
    output_directory: Union[str, Path],
    output_filename: str,
) -> bytes:
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
    output_dir = Path(output_directory)
    if not output_dir.is_dir():
        raise FileNotFoundError(f"Directory not found: {output_directory}")

    try:
        key = Fernet.generate_key()
        filepath = output_dir / output_filename

        with open(filepath, "wb") as f:
            f.write(key)

        logger.info("Fernet key generated successfully")
        return key
    except (PermissionError, OSError) as e:
        logger.error(f"Failed to generate/save Fernet key: {e}")
        raise


def encrypt_fernet_file(
    keypath: Union[str, Path],
    filepath: Union[str, Path],
) -> bytes:
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
    key_path = Path(keypath)
    file_path = Path(filepath)

    if not key_path.is_file():
        raise FileNotFoundError(f"Key file not found: {keypath}")
    if not file_path.is_file():
        raise FileNotFoundError(f"Input file not found: {filepath}")

    try:
        with open(key_path, "rb") as key_file:
            fernet = Fernet(key_file.read())

        with open(file_path, "rb") as file:
            data = file.read()

        return fernet.encrypt(data)
    except (PermissionError, OSError) as e:
        logger.error(f"File operation failed: {e}")
        raise
    except InvalidToken as e:
        logger.error(f"Invalid encryption key: {e}")
        raise


def decrypt_fernet_data(
    keypath: Union[str, Path],
    filepath: Union[str, Path],
) -> bytes:
    """Decrypts a file using Fernet symmetric encryption.

    Args:
        keypath (str): Path to the Fernet key file.
        filepath (str): Path to the encrypted file.

    Returns:
        str: Decrypted data.

    Raises:
        FileNotFoundError: If key file or input file doesn't exist
        InvalidToken: If the key is invalid or data is corrupted
        TypeError: If input types are incorrect
    """
    key_path = Path(keypath)
    file_path = Path(filepath)

    if not key_path.is_file():
        raise FileNotFoundError(f"Key file not found: {keypath}")
    if not file_path.is_file():
        raise FileNotFoundError(f"Input file not found: {filepath}")

    try:
        with open(key_path, "rb") as key_file:
            fernet = Fernet(key_file.read())

        with open(file_path, "rb") as file:
            data = file.read()

        return fernet.decrypt(data)
    except (PermissionError, OSError) as e:
        logger.error(f"File operation failed: {e}")
        raise
    except InvalidToken as e:
        logger.error(f"Invalid key or corrupted data: {e}")
        raise


if __name__ == "__main__":
    import doctest

    doctest.testmod()
