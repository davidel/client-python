import threading
from typing import Optional, Callable

import websocket

STOCKS_CLUSTER = "stocks"
FOREX_CLUSTER = "forex"
CRYPTO_CLUSTER = "crypto"


def _format_params(params):
    return ','.join(params)


class WebSocketClient:
    def __init__(self, cluster: str, auth_key: str, service: str = None,
                 process_message: Optional[Callable[[websocket.WebSocketApp, str], None]] = None,
                 on_close: Optional[Callable[[websocket.WebSocketApp], None]] = None,
                 on_error: Optional[Callable[[websocket.WebSocketApp, str], None]] = None):
        self.url = f'wss://{service or "socket"}.polygon.io/{cluster}'
        self.auth_key = auth_key

        self.ws: websocket.WebSocketApp = websocket.WebSocketApp(self.url,
                                                                 on_close=on_close,
                                                                 on_error=on_error,
                                                                 on_message=process_message)

        self._run_thread: Optional[threading.Thread] = None

    def run(self, **kwargs):
        self.ws.run_forever(**kwargs)

    def run_async(self, **kwargs):
        self._run_thread = threading.Thread(target=self.run, kwargs=kwargs)
        self._run_thread.start()

    def close_connection(self):
        self.ws.close()
        if self._run_thread:
            self._run_thread.join()

    def subscribe(self, *params):
        fparams = _format_params(params)
        self.ws.send(f'{{"action":"subscribe","params":"{fparams}"}}')

    def unsubscribe(self, *params):
        fparams = _format_params(params)
        self.ws.send(f'{{"action":"unsubscribe","params":"{fparams}"}}')

    def authenticate(self):
        self.ws.send(f'{{"action":"auth","params":"{self.auth_key}"}}')

