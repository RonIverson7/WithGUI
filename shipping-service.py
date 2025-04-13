import json
from kafka import KafkaConsumer
import tkinter as tk
import threading

KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_VALIDATED,
    bootstrap_servers=KAFKA_BROKER
)

root = tk.Tk()
root.title("Shipping Status")

shipping_text = tk.Text(root, height=20, width=90)
shipping_text.pack()

def update_shipping_status(message):
    shipping_text.insert(tk.END, message + "\n")
    shipping_text.yview(tk.END) 

def consume_messages():
    for message in consumer:
        order = json.loads(message.value.decode("utf-8"))
        if order.get("status") == "Available":
            update_shipping_status(f"Shipping order for {order['customer']}, Book Title: {order['book']}")
        elif order.get("status") == "Out of Stock":
            update_shipping_status(f"Cannot ship order for {order['customer']}, Book Title: {order['book']} due to Inavailability")
        else:
            update_shipping_status(f"Cannot ship order for {order['customer']}, Book Title: {order['book']} (Status: {order['status']})")

threading.Thread(target=consume_messages, daemon=True).start()

root.mainloop()
