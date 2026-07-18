"""Tests for data_manipulation.smtplib_ (SMTP is mocked; no real connection)."""

import pytest

from data_manipulation import smtplib_


def test_send_email_success(mocker):
    server = mocker.MagicMock()
    smtp_cls = mocker.patch("data_manipulation.smtplib_.SMTP")
    smtp_cls.return_value.__enter__.return_value = server

    ok = smtplib_.send_email(
        logname="log.txt",
        message_subject="subject",
        message_sender="a@b.com",
        message_receiver="c@d.com",
        html="<p>hi</p>",
        smtp_address="smtp.test",
    )
    assert ok is True
    server.starttls.assert_called_once()
    server.send_message.assert_called_once()


def test_send_email_missing_required():
    with pytest.raises(ValueError):
        smtplib_.send_email(
            logname="",
            message_subject="s",
            message_sender="a@b.com",
            message_receiver="c@d.com",
            html="<p>hi</p>",
            smtp_address="smtp.test",
        )
