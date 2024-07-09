import pytest
from unittest.mock import patch
from mock import MagicMock

from src.server import Server


class CDProtoException(Exception):
    pass


def test_server():
    """Test that the server used CDProto methods."""

    def fail(s):
        raise CDProtoException()

    with patch("socket.socket") as mock_socket, patch(
        "selectors.DefaultSelector.select", side_effect=CDProtoException
    ) as mock_selector, patch("selectors.DefaultSelector.register", side_effect=MagicMock) as mock_register:
        s = Server()

        with pytest.raises(CDProtoException):
            s.loop()

        assert mock_socket.call_count == 1
        assert mock_selector.call_count == 1
        assert mock_register.call_count == 1
