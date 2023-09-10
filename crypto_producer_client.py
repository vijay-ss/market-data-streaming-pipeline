import json
import websocket
from datetime import datetime, timezone


class CoinCapProducer():
    """
    Streaming websocket client that retrieves real-time crypto 
    prices via CoinCap api. Received messages are sent to
    Kafka topic for downstream consumers.
    """

    def __init__(self) -> None:
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            'wss://ws.coincap.io/prices?assets=bitcoin,ethereum,monero,litecoin,dogecoin,xrp',
            on_open=self.on_open,
            on_message = self.on_message,
            on_error = self.on_error,
            on_close = self.on_close
            )
        self.ws.run_forever()

    def on_message(self, ws, message):
        message_json = json.loads(message)
        message_json["timestamp"] = datetime.now(timezone.utc).timestamp() * 1000
        message_json = json.dumps(message_json)
        print(message_json)

    def on_error(self, ws, error):
        print(error)

    def on_close(self,ws, close_status_code, close_msg):
        print("### closed connection ###")
        if close_status_code or close_msg:
            print("close status code: " + str(close_status_code))
            print("close message: " + str(close_msg))

    def on_open(self, ws):
        print("Opened connection")


if __name__ == "__main__":
    CoinCapProducer()