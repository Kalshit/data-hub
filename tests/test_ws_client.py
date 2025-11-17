from kalshi_platform.ingestion.ws_client import KalshiWebSocketClient


def test_detect_sequence_gap():
    async def noop_handler(_):
        return None

    client = KalshiWebSocketClient(
        ws_url="wss://example.com",
        auth_token=None,
        message_handler=noop_handler,
    )

    assert client.detect_sequence_gap("TEST", 1) is False
    assert client.detect_sequence_gap("TEST", 2) is False
    assert client.detect_sequence_gap("TEST", 4) is True

