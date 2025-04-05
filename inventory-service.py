import json
from kafka import KafkaConsumer, KafkaProducer
import tkinter as tk
import threading

KAFKA_BROKER = "localhost:9092"
TOPIC_ORDERS = "book_orders"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_ORDERS,
    bootstrap_servers=KAFKA_BROKER
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

inventory = {
    1: {"name": "Harry Potter", "quantity": 50},
    2: {"name": "Pride", "quantity": 50},
    3: {"name": "Monster", "quantity": 30},
    4: {"name": "Alchemist", "quantity": 20},
    5: {"name": "The Hobbit", "quantity": 10}
}

def update_inventory_display():
    inventory_text.delete("1.0", tk.END)
    for book_id, book in inventory.items():
        inventory_text.insert(tk.END, f"Book: {book['name']}, Available Quantity: {book['quantity']}\n")

def show_message(status, book_name):
    if status == "Available":
        message_label.config(text=f"Order confirmed: {book_name}", fg="green")
    else:
        message_label.config(text=f"Order failed: {status} - {book_name}", fg="red")

def schedule_message_update(status, book_name):
    show_message(status, book_name)

def process_orders():
    for message in consumer:
        try:
            order = json.loads(message.value.decode("utf-8"))
            required_fields = ["book_id", "quantity", "customer", "payment_option"]
            if not all(field in order for field in required_fields):
                continue

            book_id = order["book_id"]
            quantity = order["quantity"]
            customer = order["customer"]
            payment_option = order["payment_option"]

            book = inventory.get(book_id)
            if not book:
                status = "Invalid Book"
                book_name = "Unknown"
            elif quantity <= 0:
                status = "Invalid Quantity"
                book_name = book["name"]
            elif book["quantity"] < quantity:
                status = "Out of Stock"
                book_name = book["name"]
            else:
                book["quantity"] -= quantity
                status = "Available"
                book_name = book["name"]

            response = {
                "book_id": book_id,
                "book": book_name,
                "status": status,
                "customer": customer,
                "payment_option": payment_option
            }

            producer.send(TOPIC_VALIDATED, json.dumps(response).encode("utf-8"))
            
         
            root.after(100, update_inventory_display)
            root.after(100, schedule_message_update, status, book_name)  

        except Exception as e:
            print(f"Error processing order: {e}") 

root = tk.Tk()
root.title("Inventory Status")

inventory_text = tk.Text(root, height=10, width=50)
inventory_text.pack()

message_label = tk.Label(root, text="", font=("Arial", 12))
message_label.pack()

update_inventory_display()

threading.Thread(target=process_orders, daemon=True).start()

root.mainloop()