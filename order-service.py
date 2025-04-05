from fastapi import FastAPI
from kafka import KafkaProducer
import json
import tkinter as tk
from tkinter import messagebox

app = FastAPI()

TOPIC_ORDERS = "book_orders"
KAFKA_BROKER = "localhost:9092"

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

@app.post("/placeOrder/")
def placeOrder(book_id: int, quantity: int, customer: str, payment_option: str):
    if book_id not in inventory:
        return {"error": f"Invalid book_id: {book_id}"}
    if quantity <= 0:
        return {"error": "Quantity must be greater than 0"}
    if not customer.strip():
        return {"error": "Customer name cannot be empty"}
    if payment_option.lower() not in ["cash", "card"]:
        return {"error": "Invalid payment option. Choose 'cash' or 'card'"}

    order = {
        "book_id": book_id,
        "quantity": quantity,
        "customer": customer.strip(),
        "payment_option": payment_option.lower()
    }

    producer.send(TOPIC_ORDERS, json.dumps(order).encode("utf-8"))
    return {"message": "Order placed successfully"}


def place_order():
    book_id = int(book_id_entry.get())
    quantity = int(quantity_entry.get())
    customer = customer_entry.get()
    payment_option = payment_var.get()
    
    response = placeOrder(book_id, quantity, customer, payment_option)
    if "error" in response:
        messagebox.showerror("Error", response["error"])
    else:
        messagebox.showinfo("Success", response["message"])

root = tk.Tk()
root.title("Book Order Form")
root.geometry("350x200")

tk.Label(root, text="Book ID:").grid(row=0, column=0, padx=10, pady=5)
tk.Label(root, text="Quantity:").grid(row=1, column=0, padx=10, pady=5)
tk.Label(root, text="Customer:").grid(row=2, column=0, padx=10, pady=5)
tk.Label(root, text="Payment Option:").grid(row=3, column=0, padx=10, pady=5)

book_id_entry = tk.Entry(root)
book_id_entry.grid(row=0, column=1, padx=10, pady=5)

quantity_entry = tk.Entry(root)
quantity_entry.grid(row=1, column=1, padx=10, pady=5)

customer_entry = tk.Entry(root)
customer_entry.grid(row=2, column=1, padx=10, pady=5)

payment_var = tk.StringVar(value="cash")
payment_option_menu = tk.OptionMenu(root, payment_var, "cash", "card")
payment_option_menu.grid(row=3, column=1, padx=10, pady=5)

tk.Button(root, text="Place Order", command=place_order).grid(row=4, column=0, columnspan=2, pady=10)

root.mainloop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)
