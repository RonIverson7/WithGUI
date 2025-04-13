import json
from kafka import KafkaConsumer
import tkinter as tk
import threading

KAFKA_BROKER = "localhost:9092"
TOPIC_NOTIFICATIONS = "notifications"

consumer = KafkaConsumer(
    TOPIC_NOTIFICATIONS,
    bootstrap_servers=KAFKA_BROKER
)   

root = tk.Tk()
root.withdraw()

def show_popup(message):
    popup = tk.Toplevel(root)
    popup.title("Notification")
    tk.Label(popup, text=message, padx=20, pady=10).pack()
    tk.Button(popup, text="OK", command=popup.destroy).pack()

def consume_messages():
    for message in consumer:
        try:
            notification = json.loads(message.value.decode("utf-8"))
            if "customer" in notification and "message" in notification:
                notification_message = f"Notification: {notification['message']} for Customer {notification['customer']}"
                print(notification_message)
                
                root.after(7000, show_popup, notification_message)
            else:
                print("Invalid notification message")
        except Exception as e:
            print(f"Error in notification consumer: {e}")

threading.Thread(target=consume_messages, daemon=True).start()

root.mainloop()
