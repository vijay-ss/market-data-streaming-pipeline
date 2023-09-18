import os
import json
import websocket
from datetime import datetime, timezone
from utils import functions as f


class FinnhubProducer():
    """
    Streaming websocket client that retrieves real-time stock
    market prices via Finnhub api. Received messages are sent
    to Kafka topic for downstream consumers.
    """
    def __init__(self) -> None:

        self.producer = f.load_producer(os.environ["KAFKA_SERVER"], os.environ["KAFKA_PORT"])

        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            f'wss://ws.finnhub.io?token={os.environ["FINNHUB_API_KEY"]}',
            on_open = self.on_open,
            on_message = self.on_message,
            on_error = self.on_error,
            on_close = self.on_close,
            )
        self.ws.run_forever()

    def on_message(self, ws, message):
        if message == """{"type":"ping"}""":
            date_fmt = "%Y-%m-%d, %H:%M:%S"
            print(f"Stock market is currently closed. Current UTC time: {datetime.now(timezone.utc).strftime(date_fmt)}")
        else:
            message_json = json.loads(message)
            message_json["timestamp"] = datetime.now(timezone.utc).timestamp() * 1000
            message_json = json.dumps(message_json)
            print(f"Message payload: {message_json}")

            self.producer.send(os.environ["KAFKA_TOPIC"], value=message_json) \
                .add_callback(f.on_send_success).add_errback(f.on_send_error)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed connection ###")
        if close_status_code or close_msg:
            print("close status code: " + str(close_status_code))
            print("close message: " + str(close_msg))

    def on_open(self, ws):
        print("Opened connection")
        ticker = "AAPL"
        ws.send('{"type":"subscribe","symbol":"{ticker}"}'.replace("ticker", ticker))
        print(f'Subscription for {ticker} succeeded')

if __name__ == "__main__":
    f.load_env_variables()
    FinnhubProducer()