import json
from kafka import KafkaConsumer, KafkaProducer
import tkinter as tk
import threading

KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"
TOPIC_NOTIFICATIONS = "notifications"

consumer = KafkaConsumer(
    TOPIC_VALIDATED,
    bootstrap_servers=KAFKA_BROKER
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

root = tk.Tk()
root.withdraw()  

def show_popup(message):
    popup = tk.Toplevel(root)
    popup.title("Payment Status")
    tk.Label(popup, text=message, padx=20, pady=10).pack()
    tk.Button(popup, text="OK", command=popup.destroy).pack()

def consume_messages():
    for message in consumer:
        try:
            order = json.loads(message.value.decode("utf-8"))
            if not all(key in order for key in ["customer", "book", "status", "payment_option"]):
                print("Skipping invalid payment message")
                continue

            if order["status"] == "Available":
                print(f"Processing payment for {order['customer']}, Book Title {order['book']} using Payment option: {order['payment_option']}")
                payment_status = "Payment Confirmed"
            else:
                print(f"Payment failed for {order['customer']}, Book Title {order['book']} (Status: {order['status']})")
                payment_status = "Payment Failed"

            root.after(0, show_popup, payment_status)

            notification = {
                "customer": order["customer"],
                "message": payment_status
            }

            producer.send(TOPIC_NOTIFICATIONS, json.dumps(notification).encode("utf-8"))

        except Exception as e:
            print(f"Error in payment consumer: {e}")

consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

root.mainloop()