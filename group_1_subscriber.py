"""Group 1 MQTT temperature subscriber.

Expected publisher message:
{"temperature": 22.5, "packet_id": 1, "timestamp": "2026-08-15 12:00:00"}

"""

import json
import math
import queue
import smtplib
import subprocess
import threading
import time
import tkinter as tk
from datetime import datetime
from email.message import EmailMessage
from tkinter import messagebox


class SubscriberGUI:
    """Receive temperature data and show it in a Tkinter window."""

    def __init__(self):
        self.window = tk.Tk()
        self.window.title("Group 1 Temperature Subscriber")
        self.window.geometry("850x610")
        self.window.configure(bg="#eaf1f5")
        self.window.protocol("WM_DELETE_WINDOW", self.close_program)

        self.broker = tk.StringVar(value="localhost")
        self.port = tk.StringVar(value="1883")
        self.topic = tk.StringVar(value="group/1/temperature")
        self.missing_seconds = tk.StringVar(value="15")

        self.connection_text = tk.StringVar(value="Not connected")
        self.reading_text = tk.StringVar(value="Temperature: waiting for data")
        self.status_text = tk.StringVar(value="Status: no message received")
        self.packet_text = tk.StringVar(value="Packet ID: --")
        self.timestamp_text = tk.StringVar(value="Timestamp: --")

        self.email_enabled = tk.BooleanVar(value=False)
        self.smtp_host = tk.StringVar(value="smtp.gmail.com")
        self.smtp_port = tk.StringVar(value="465")
        self.email_from = tk.StringVar()
        self.email_password = tk.StringVar()
        self.email_to = tk.StringVar()

        self.temperature = 21.0
        self.process = None
        self.connected = False
        self.subscribed = False
        self.last_message_time = None
        self.last_packet_id = None
        self.missing_alert_sent = False
        self.messages = queue.Queue()

        self.build_gui()
        self.window.after(100, self.check_messages)
        self.window.after(1000, self.check_missing_data)

    def build_gui(self):
        tk.Label(
            self.window, text="Temperature Subscriber", font=("Arial", 22, "bold"),
            bg="#eaf1f5", fg="#17324d"
        ).pack(pady=(15, 5))

        main = tk.Frame(self.window, bg="#eaf1f5")
        main.pack(fill="both", expand=True, padx=20, pady=10)

        left = tk.Frame(main, bg="white", bd=1, relief="solid")
        left.pack(side="left", fill="both", expand=True, padx=(0, 8))
        right = tk.Frame(main, bg="white", bd=1, relief="solid")
        right.pack(side="right", fill="y", padx=(8, 0))

        self.canvas = tk.Canvas(left, width=450, height=340, bg="white",
                                highlightthickness=0)
        self.canvas.pack(fill="both", expand=True, padx=10, pady=10)
        self.canvas.bind("<Configure>", lambda event: self.draw_gauge())

        self.log_box = tk.Text(left, height=9, state="disabled", wrap="word")
        self.log_box.pack(fill="x", padx=12, pady=(0, 12))

        tk.Label(right, text="MQTT Settings", font=("Arial", 12, "bold"),
                 bg="white").grid(row=0, column=0, columnspan=2, sticky="w",
                                  padx=12, pady=(12, 5))
        self.add_entry(right, 1, "Broker:", self.broker)
        self.add_entry(right, 2, "Port:", self.port)
        self.add_entry(right, 3, "Topic:", self.topic)
        self.add_entry(right, 4, "Missing after:", self.missing_seconds)

        self.connect_button = tk.Button(right, text="Connect",
                                        command=self.connect_to_broker)
        self.connect_button.grid(row=5, column=0, sticky="ew", padx=12, pady=8)
        self.subscribe_button = tk.Button(right, text="Unsubscribe", state="disabled",
                                          command=self.change_subscription)
        self.subscribe_button.grid(row=5, column=1, sticky="ew", padx=12, pady=8)

        tk.Label(right, textvariable=self.connection_text, bg="#fff1cc").grid(
            row=6, column=0, columnspan=2, sticky="ew", padx=12, pady=3)

        details = [self.reading_text, self.status_text,
                   self.packet_text, self.timestamp_text]
        for row, variable in enumerate(details, start=7):
            tk.Label(right, textvariable=variable, bg="white", anchor="w",
                     wraplength=290).grid(row=row, column=0, columnspan=2,
                                          sticky="w", padx=12, pady=2)

        tk.Label(right, text="Email Alerts", font=("Arial", 12, "bold"),
                 bg="white").grid(row=11, column=0, columnspan=2, sticky="w",
                                  padx=12, pady=(14, 3))
        tk.Checkbutton(right, text="Enable email alerts", variable=self.email_enabled,
                       bg="white").grid(row=12, column=0, columnspan=2,
                                        sticky="w", padx=12)
        self.add_entry(right, 13, "SMTP host:", self.smtp_host)
        self.add_entry(right, 14, "SMTP port:", self.smtp_port)
        self.add_entry(right, 15, "Email/from:", self.email_from)
        self.add_entry(right, 16, "Password:", self.email_password, password=True)
        self.add_entry(right, 17, "Send to:", self.email_to)
        tk.Button(right, text="Send Test Email", command=self.send_test_email).grid(
            row=18, column=0, columnspan=2, sticky="ew", padx=12, pady=(8, 12))

        self.draw_gauge()

    def add_entry(self, parent, row, label, variable, password=False):
        tk.Label(parent, text=label, bg="white").grid(
            row=row, column=0, sticky="w", padx=12, pady=2)
        tk.Entry(parent, textvariable=variable,
                 show="*" if password else "").grid(
            row=row, column=1, padx=12, pady=2)

    def connect_to_broker(self):
        if self.connected:
            self.disconnect_from_broker()
            return

        try:
            port = int(self.port.get())
            if port < 1 or float(self.missing_seconds.get()) <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", "Enter a valid port and missing-data time.")
            return

        command = ["mosquitto_sub", "-h", self.broker.get(), "-p", str(port),
                   "-t", self.topic.get(), "-q", "1"]
        try:
            self.process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1)
        except FileNotFoundError:
            messagebox.showerror("Mosquitto not found",
                                 "Install Mosquitto and put mosquitto_sub on PATH.")
            return
        except OSError as error:
            messagebox.showerror("Connection error", str(error))
            return

        self.connected = True
        self.subscribed = True
        self.last_message_time = time.time()
        self.connection_text.set("Connected and subscribed")
        self.connect_button.config(text="Disconnect")
        self.subscribe_button.config(text="Unsubscribe", state="normal")
        self.write_log("Listening to " + self.topic.get())
        threading.Thread(target=self.read_output, daemon=True).start()
        threading.Thread(target=self.read_errors, daemon=True).start()

    def disconnect_from_broker(self):
        self.stop_mosquitto()
        self.connected = False
        self.subscribed = False
        self.last_message_time = None
        self.connection_text.set("Not connected")
        self.connect_button.config(text="Connect")
        self.subscribe_button.config(text="Unsubscribe", state="disabled")
        self.write_log("Disconnected")

    def change_subscription(self):
        if self.subscribed:
            self.stop_mosquitto()
            self.subscribed = False
            self.connection_text.set("Connected but unsubscribed")
            self.subscribe_button.config(text="Subscribe")
            self.write_log("Unsubscribed")
        elif self.connected:
            self.connected = False
            self.connect_to_broker()

    def stop_mosquitto(self):
        old_process = self.process
        self.process = None
        if old_process is not None and old_process.poll() is None:
            old_process.terminate()

    def read_output(self):
        current_process = self.process
        if current_process is None:
            return
        for line in current_process.stdout:
            if current_process is not self.process:
                break
            self.messages.put(("message", line.strip()))

    def read_errors(self):
        current_process = self.process
        if current_process is None:
            return
        for line in current_process.stderr:
            if current_process is not self.process:
                break
            if line.strip():
                self.messages.put(("log", "Mosquitto: " + line.strip()))

    def check_messages(self):
        while not self.messages.empty():
            message_type, text = self.messages.get()
            if message_type == "message":
                self.handle_message(text)
            else:
                self.write_log(text)
        self.window.after(100, self.check_messages)

    def handle_message(self, message):
        self.last_message_time = time.time()
        self.missing_alert_sent = False

        try:
            data = json.loads(message)
            temperature = float(data["temperature"])
            packet_id = data.get("packet_id", "--")
            timestamp = data.get("timestamp", "--")
            if not math.isfinite(temperature):
                raise ValueError("Temperature is not a normal number")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            self.show_alert("Invalid message", str(error))
            return

        self.reading_text.set(f"Temperature: {temperature:.1f} °C")
        self.packet_text.set("Packet ID: " + str(packet_id))
        self.timestamp_text.set("Timestamp: " + str(timestamp))

        if self.last_packet_id is not None:
            try:
                if int(packet_id) > int(self.last_packet_id) + 1:
                    self.show_alert("Missing packet",
                                    f"Packet {self.last_packet_id} was followed by {packet_id}.")
            except (ValueError, TypeError):
                pass
        self.last_packet_id = packet_id

        if temperature < 5 or temperature > 35:
            self.show_alert("Out-of-range temperature",
                            f"Received {temperature:.1f} °C; valid range is 5-35 °C.")
            return

        self.temperature = temperature
        status = self.temperature_status(temperature)
        self.status_text.set("Status: " + status)
        self.write_log(f"Packet {packet_id}: {temperature:.1f} °C ({status})")
        self.draw_gauge()

    def check_missing_data(self):
        if self.connected and self.subscribed and self.last_message_time is not None:
            try:
                limit = float(self.missing_seconds.get())
            except ValueError:
                limit = 15
            elapsed = time.time() - self.last_message_time
            if elapsed > limit and not self.missing_alert_sent:
                self.missing_alert_sent = True
                self.show_alert("Missing transmission",
                                f"No data received for {elapsed:.0f} seconds.")
        self.window.after(1000, self.check_missing_data)

    def show_alert(self, subject, details):
        self.status_text.set("Status: ALERT - " + subject)
        self.write_log("ALERT: " + details)
        if self.email_enabled.get():
            threading.Thread(target=self.send_email, args=(subject, details),
                             daemon=True).start()

    def send_test_email(self):
        if not self.email_enabled.get():
            messagebox.showinfo("Email", "Enable email alerts first.")
            return
        threading.Thread(target=self.send_email,
                         args=("Test", "Subscriber email settings are working."),
                         daemon=True).start()

    def send_email(self, subject, details):
        try:
            email = EmailMessage()
            email["Subject"] = "Group 1 subscriber alert: " + subject
            email["From"] = self.email_from.get()
            email["To"] = self.email_to.get()
            email.set_content(details)
            with smtplib.SMTP_SSL(self.smtp_host.get(), int(self.smtp_port.get()),
                                  timeout=15) as server:
                server.login(self.email_from.get(), self.email_password.get())
                server.send_message(email)
            self.messages.put(("log", "Email alert sent"))
        except Exception as error:
            self.messages.put(("log", "Email failed: " + str(error)))

    def draw_gauge(self):
        self.canvas.delete("all")
        width = max(self.canvas.winfo_width(), 400)
        height = max(self.canvas.winfo_height(), 300)
        center_x, center_y = width / 2, height * 0.53
        radius = min(width * 0.32, height * 0.35)
        circle = (center_x - radius, center_y - radius,
                  center_x + radius, center_y + radius)
        start, total = 210, -240

        for low, high, colour in [(5, 18, "#4f9dd9"),
                                  (18, 24, "#40a66b"),
                                  (24, 35, "#e9814d")]:
            low_part = (low - 5) / 30
            high_part = (high - 5) / 30
            self.canvas.create_arc(circle, start=start + total * low_part,
                                   extent=total * (high_part - low_part),
                                   style="arc", width=22, outline=colour)

        part = (self.temperature - 5) / 30
        angle = math.radians(start + total * part)
        needle_x = center_x + (radius - 35) * math.cos(angle)
        needle_y = center_y - (radius - 35) * math.sin(angle)
        self.canvas.create_line(center_x, center_y, needle_x, needle_y,
                                fill="#17324d", width=5)
        self.canvas.create_oval(center_x - 9, center_y - 9,
                                center_x + 9, center_y + 9, fill="#17324d")
        self.canvas.create_text(center_x, center_y + radius * 0.50,
                                text=f"{self.temperature:.1f} °C",
                                font=("Arial", 26, "bold"), fill="#17324d")

    def temperature_status(self, temperature):
        if temperature < 18:
            return "Cool"
        if temperature <= 24:
            return "Comfortable"
        return "Warm"

    def write_log(self, text):
        now = datetime.now().strftime("%H:%M:%S")
        self.log_box.config(state="normal")
        self.log_box.insert("end", f"[{now}] {text}\n")
        self.log_box.see("end")
        self.log_box.config(state="disabled")

    def close_program(self):
        self.stop_mosquitto()
        self.window.destroy()

    def run(self):
        self.window.mainloop()


if __name__ == "__main__":
    app = SubscriberGUI()
    app.run()
