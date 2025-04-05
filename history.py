import json
from kafka import KafkaConsumer
import tkinter as tk
import threading
##Hello
KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_VALIDATED,
    bootstrap_servers=KAFKA_BROKER
)

customer_orders = {}
def update_order_history():
    order_history_text.delete("1.0", tk.END) 
    for customer, orders in customer_orders.items():
        order_history_text.insert(tk.END, f"Order history for {customer}:\n")
        for past_order in orders:
            order_history_text.insert(tk.END, f"Book Title: {past_order['book']} | Status: {past_order['status']} | Payment: {past_order['payment_option']}\n")
        order_history_text.insert(tk.END, "\n")

def consume_messages():
    for message in consumer:
        try:
            order = json.loads(message.value.decode("utf-8"))
            if not all(k in order for k in ["customer", "book", "status", "payment_option"]):
                continue

            customer = order["customer"]
            customer_orders.setdefault(customer, []).append(order)

            root.after(100, update_order_history)  
        except Exception as e:
            print(f"Error in history consumer: {e}")

root = tk.Tk()
root.title("Customer Order History")

order_history_text = tk.Text(root, height=20, width=70)
order_history_text.pack()

threading.Thread(target=consume_messages, daemon=True).start()
root.mainloop()