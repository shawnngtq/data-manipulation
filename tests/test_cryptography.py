"""Tests for data_manipulation.cryptography_ (skipped without cryptography)."""

import pytest

pytest.importorskip("cryptography")

from data_manipulation import cryptography_  # noqa: E402


def test_fernet_round_trip(tmp_path):
    key = cryptography_.generate_fernet_key(tmp_path, "enc.key")
    assert isinstance(key, bytes)
    assert (tmp_path / "enc.key").is_file()

    plaintext = b"top secret payload"
    data_file = tmp_path / "data.txt"
    data_file.write_bytes(plaintext)

    ciphertext = cryptography_.encrypt_fernet_file(
        tmp_path / "enc.key", data_file
    )
    assert isinstance(ciphertext, bytes)
    assert ciphertext != plaintext

    enc_file = tmp_path / "data.enc"
    enc_file.write_bytes(ciphertext)

    decrypted = cryptography_.decrypt_fernet_data(
        tmp_path / "enc.key", enc_file
    )
    assert decrypted == plaintext


def test_generate_fernet_key_missing_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        cryptography_.generate_fernet_key(tmp_path / "nope", "enc.key")


def test_encrypt_missing_key(tmp_path):
    data_file = tmp_path / "data.txt"
    data_file.write_bytes(b"x")
    with pytest.raises(FileNotFoundError):
        cryptography_.encrypt_fernet_file(tmp_path / "absent.key", data_file)
