import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import sqlite3
import customtkinter as ctk
import json
import os, sys
from pymongo import MongoClient, mongo_client
import threading
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import pandas as pd
from pathlib import Path
 
# --- Modern GUI Settings ---
try:
    import psycopg2
except ImportError:
    messagebox.showerror("Missing Dependency", "psycopg2 is not installed. Please run 'pip install psycopg2-binary' to use PostgreSQL.")
    sys.exit(1)

MYSQL_AVAILABLE = False
try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    pass # Will be handled in the UI

PYODBC_AVAILABLE = False
try:
    import pyodbc
    from sqlalchemy import create_engine, inspect
    PYODBC_AVAILABLE = True
except ImportError:
    pass # Will be handled in the UI

ctk.set_appearance_mode("System")  # "System", "Dark", "Light"
ctk.set_default_color_theme("blue") # "blue", "green", "dark-blue"
 
 
class SQLNoSQLConverterApp:
    """
    A Tkinter GUI application for converting data between SQLite (SQL)
    and MongoDB (NoSQL) databases.
    """

    def __init__(self, root):
        self.root = root
        self.root.title("SQL (SQLite) <-> NoSQL (MongoDB) Converter")
        self.root.geometry("850x750")
 
        # Configure grid for the root window to make the scrollable frame expandable
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(0, weight=1)

        # Create a main scrollable frame to hold all widgets
        self.main_frame = ctk.CTkScrollableFrame(self.root)
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.main_frame.grid_columnconfigure(0, weight=1)

        # --- Member variables ---
        self.sql_type = tk.StringVar(value="SQLite")
        
        # SQLite
        self.sqlite_path = tk.StringVar()
        
        # PostgreSQL
        self.pg_host = tk.StringVar(value="localhost")
        self.pg_port = tk.StringVar(value="5432")
        self.pg_dbname = tk.StringVar(value="postgres")
        self.pg_user = tk.StringVar(value="postgres")
        self.pg_password = tk.StringVar()
        self.pg_conn = None # To store active PG connection

        # MS SQL Server
        self.mssql_server = tk.StringVar(value="localhost")
        self.mssql_dbname = tk.StringVar(value="master")
        self.mssql_user = tk.StringVar()
        self.mssql_password = tk.StringVar()
        self.mssql_driver = "" # To store the detected driver
        self.mssql_conn = None # To store active MS SQL connection

        # MySQL
        self.mysql_host = tk.StringVar(value="localhost")
        self.mysql_port = tk.StringVar(value="3306")
        self.mysql_dbname = tk.StringVar()
        self.mysql_user = tk.StringVar(value="root")
        self.mysql_password = tk.StringVar()
        self.mysql_conn = None # To store active MySQL connection

        # MongoDB
        self.mongo_uri = tk.StringVar(value="mongodb://localhost:27017/")
        self.mongo_db_name = tk.StringVar(value="converted_db")
        self.mongo_client = None
        self.use_custom_query = tk.BooleanVar(value=False)
        self.custom_collection_name = tk.StringVar()

        self.create_widgets()
        self.create_menu()

    def create_widgets(self):
        """Create and layout all the GUI widgets."""

        # --- SQLite Frame ---
        sqlite_frame = ctk.CTkFrame(self.main_frame)
        sqlite_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
        sqlite_frame.grid_columnconfigure(0, minsize=150) # Set a minimum width for the label column
        sqlite_frame.grid_columnconfigure(1, weight=1) # Make the entry column expandable
        ctk.CTkLabel(sqlite_frame, text="SQL Source", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, columnspan=3, padx=10, pady=(10,5), sticky="w")
        
        db_values = ["SQLite", "PostgreSQL"]
        if MYSQL_AVAILABLE:
            db_values.insert(1, "MySQL")
        if PYODBC_AVAILABLE:
            db_values.append("SQL Server")
        ctk.CTkLabel(sqlite_frame, text="DB Type:", font=ctk.CTkFont(weight="bold")).grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.combo_sql_type = ctk.CTkComboBox(sqlite_frame, variable=self.sql_type, values=db_values, command=self._on_sql_type_change)
        self.combo_sql_type.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        # --- SQLite Widgets ---
        self.sqlite_widgets_frame = ctk.CTkFrame(sqlite_frame, fg_color="transparent")
        self.sqlite_widgets_frame.grid(row=2, column=0, columnspan=3, sticky="ew")
        self.sqlite_widgets_frame.grid_columnconfigure(0, minsize=140) # Align with parent frame's label column
        self.sqlite_widgets_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(self.sqlite_widgets_frame, text="Database File:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.entry_sqlite_path = ctk.CTkEntry(self.sqlite_widgets_frame, textvariable=self.sqlite_path, state="disabled")
        self.entry_sqlite_path.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        ctk.CTkButton(self.sqlite_widgets_frame, text="Browse...", command=self.browse_sqlite_db).grid(row=0, column=2, padx=10, pady=5)

        # --- PostgreSQL Widgets ---
        self.postgres_widgets_frame = ctk.CTkFrame(sqlite_frame, fg_color="transparent")
        self.postgres_widgets_frame.grid_columnconfigure(0, minsize=140) # Match the parent frame's label column
        self.postgres_widgets_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(self.postgres_widgets_frame, text="Host:").grid(row=0, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.postgres_widgets_frame, textvariable=self.pg_host).grid(row=0, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.postgres_widgets_frame, text="Port:").grid(row=1, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.postgres_widgets_frame, textvariable=self.pg_port).grid(row=1, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.postgres_widgets_frame, text="DB Name:").grid(row=2, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.postgres_widgets_frame, textvariable=self.pg_dbname).grid(row=2, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.postgres_widgets_frame, text="User:").grid(row=3, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.postgres_widgets_frame, textvariable=self.pg_user).grid(row=3, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.postgres_widgets_frame, text="Password:").grid(row=4, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.postgres_widgets_frame, textvariable=self.pg_password, show="*").grid(row=4, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkButton(self.postgres_widgets_frame, text="Connect", command=self.connect_and_load_postgres_tables).grid(row=0, column=2, rowspan=5, padx=10, pady=5, ipady=40)

        # --- MS SQL Server Widgets ---
        self.mssql_widgets_frame = ctk.CTkFrame(sqlite_frame, fg_color="transparent")
        self.mssql_widgets_frame.grid_columnconfigure(0, minsize=140) # Match the parent frame's label column
        self.mssql_widgets_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(self.mssql_widgets_frame, text="Server:").grid(row=0, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mssql_widgets_frame, textvariable=self.mssql_server).grid(row=0, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.mssql_widgets_frame, text="DB Name:").grid(row=1, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mssql_widgets_frame, textvariable=self.mssql_dbname).grid(row=1, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.mssql_widgets_frame, text="User (optional):").grid(row=2, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mssql_widgets_frame, textvariable=self.mssql_user).grid(row=2, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.mssql_widgets_frame, text="Password (optional):").grid(row=3, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mssql_widgets_frame, textvariable=self.mssql_password, show="*").grid(row=3, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkButton(self.mssql_widgets_frame, text="Connect", command=self.connect_and_load_mssql_tables).grid(row=0, column=2, rowspan=4, padx=10, pady=5, ipady=30)

        # --- MySQL Widgets ---
        self.mysql_widgets_frame = ctk.CTkFrame(sqlite_frame, fg_color="transparent")
        self.mysql_widgets_frame.grid_columnconfigure(0, minsize=140) # Match the parent frame's label column
        self.mysql_widgets_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(self.mysql_widgets_frame, text="Host:").grid(row=0, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mysql_widgets_frame, textvariable=self.mysql_host).grid(row=0, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.mysql_widgets_frame, text="Port:").grid(row=1, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mysql_widgets_frame, textvariable=self.mysql_port).grid(row=1, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.mysql_widgets_frame, text="DB Name:").grid(row=2, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mysql_widgets_frame, textvariable=self.mysql_dbname).grid(row=2, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.mysql_widgets_frame, text="User:").grid(row=3, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mysql_widgets_frame, textvariable=self.mysql_user).grid(row=3, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkLabel(self.mysql_widgets_frame, text="Password:").grid(row=4, column=0, padx=10, pady=2, sticky="w")
        ctk.CTkEntry(self.mysql_widgets_frame, textvariable=self.mysql_password, show="*").grid(row=4, column=1, padx=5, pady=2, sticky="ew")
        ctk.CTkButton(self.mysql_widgets_frame, text="Connect", command=self.connect_and_load_mysql_tables).grid(row=0, column=2, rowspan=5, padx=10, pady=5, ipady=40)

        # --- Common SQL Widgets ---
        ctk.CTkLabel(sqlite_frame, text="Select Table:", font=ctk.CTkFont(weight="bold")).grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.combo_sql_tables = ctk.CTkComboBox(sqlite_frame, state="readonly", values=[])
        self.combo_sql_tables.grid(row=3, column=1, padx=5, pady=5, sticky="ew")
        ctk.CTkButton(sqlite_frame, text="Preview Data", command=self.preview_sql_data).grid(row=3, column=2, padx=10, pady=5)

        # --- Custom Query Section ---
        self.custom_query_checkbox = ctk.CTkCheckBox(
            sqlite_frame, text="Use Custom Query", variable=self.use_custom_query,
            command=self.toggle_custom_query_widgets
        )
        self.custom_query_checkbox.grid(row=4, column=0, columnspan=3, padx=10, pady=(10, 0), sticky="w")
        self.custom_query_frame = ctk.CTkFrame(sqlite_frame)
        self.custom_query_frame.grid(row=5, column=0, columnspan=3, padx=5, pady=5, sticky="ew")
        self.custom_query_frame.columnconfigure(1, weight=1)

        ctk.CTkLabel(self.custom_query_frame, text="SQL Query:").grid(row=0, column=0, padx=10, pady=5, sticky="nw")
        self.custom_query_text = ctk.CTkTextbox(self.custom_query_frame, wrap=tk.WORD, height=150)
        self.custom_query_text.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(self.custom_query_frame, text="Target Collection Name:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.entry_custom_collection = ctk.CTkEntry(self.custom_query_frame, textvariable=self.custom_collection_name)
        self.entry_custom_collection.grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        # --- MongoDB Frame ---
        mongo_frame = ctk.CTkFrame(self.main_frame)
        mongo_frame.grid(row=2, column=0, padx=10, pady=10, sticky="ew")
        mongo_frame.grid_columnconfigure(0, minsize=150) # Set a minimum width for the label column
        mongo_frame.grid_columnconfigure(1, weight=1) # Make the entry column expandable
        ctk.CTkLabel(mongo_frame, text="NoSQL Destination/Source (MongoDB)", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, columnspan=3, padx=10, pady=(10,5), sticky="w")

        ctk.CTkLabel(mongo_frame, text="Connection URI:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.entry_mongo_uri = ctk.CTkEntry(mongo_frame, textvariable=self.mongo_uri)
        self.entry_mongo_uri.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        ctk.CTkLabel(mongo_frame, text="Database Name:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.entry_mongo_db = ctk.CTkEntry(mongo_frame, textvariable=self.mongo_db_name)
        self.entry_mongo_db.grid(row=2, column=1, padx=5, pady=5, sticky="ew")

        ctk.CTkButton(mongo_frame, text="Connect & Refresh Collections", command=self.connect_and_load_mongo).grid(row=1, column=2, rowspan=2, padx=10, pady=5, ipady=10)

        ctk.CTkLabel(mongo_frame, text="Select Collection:").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.combo_mongo_collections = ctk.CTkComboBox(mongo_frame, state="readonly", values=[])
        self.combo_mongo_collections.grid(row=3, column=1, padx=5, pady=5, sticky="ew")
        ctk.CTkButton(mongo_frame, text="Preview Data", command=self.preview_mongo_data).grid(row=3, column=2, padx=10, pady=5)

        # --- Conversion Buttons Frame ---
        conversion_frame = ctk.CTkFrame(self.main_frame)
        conversion_frame.grid(row=4, column=0, padx=10, pady=10, sticky="ew")
        conversion_frame.grid_columnconfigure((0, 1), weight=1)

        self.btn_sql_to_mongo = ctk.CTkButton(conversion_frame, text="Convert Selected Table to MongoDB >>", command=self.convert_sql_to_mongo)
        self.btn_sql_to_mongo.grid(row=0, column=0, padx=5, pady=(10, 5), sticky="nsew")
        self.btn_mongo_to_sql = ctk.CTkButton(conversion_frame, text="<< Convert MongoDB to SQL", command=self.convert_mongo_to_sql)
        self.btn_mongo_to_sql.grid(row=0, column=1, padx=5, pady=(10, 5), sticky="nsew")
        self.btn_entire_db_to_mongo = ctk.CTkButton(conversion_frame, text="Convert Entire SQL DB to MongoDB", command=self.convert_entire_db_to_mongo)
        self.btn_entire_db_to_mongo.grid(row=1, column=0, padx=5, pady=(5, 10), sticky="nsew")
        self.btn_entire_mongo_to_sql = ctk.CTkButton(conversion_frame, text="Convert Entire MongoDB to SQL", command=self.convert_entire_mongo_to_sql)
        self.btn_entire_mongo_to_sql.grid(row=1, column=1, padx=5, pady=(5, 10), sticky="nsew")
        
        # --- Export Frame ---
        self.export_frame = ctk.CTkFrame(self.main_frame)
        self.export_frame.grid(row=3, column=0, padx=10, pady=10, sticky="ew")
        self.export_frame.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkLabel(self.export_frame, text="Export to CSV", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, columnspan=2, padx=10, pady=(10,5), sticky="w")
        self.btn_export_sql_csv = ctk.CTkButton(self.export_frame, text="Export SQL Source to CSV", command=self.export_sql_to_csv)
        self.btn_export_sql_csv.grid(row=1, column=0, padx=5, pady=10, sticky="ew")
        self.btn_export_mongo_csv = ctk.CTkButton(self.export_frame, text="Export NoSQL Source to CSV", command=self.export_mongo_to_csv)
        self.btn_export_mongo_csv.grid(row=1, column=1, padx=5, pady=10, sticky="ew")

        # --- Progress Bar ---
        self.progress_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.progress_bar = ctk.CTkProgressBar(self.progress_frame, orientation="horizontal", mode="determinate")
        self.progress_bar.set(0)

        # --- Appearance Mode Frame ---
        appearance_frame = ctk.CTkFrame(self.main_frame)
        appearance_frame.grid(row=5, column=0, padx=10, pady=10, sticky="ew")
        appearance_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(appearance_frame, text="Appearance Mode:").grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.appearance_mode_menu = ctk.CTkSegmentedButton(appearance_frame, values=["Light", "Dark", "System"],
                                                           command=self._change_appearance_mode)
        self.appearance_mode_menu.set(ctk.get_appearance_mode())
        self.appearance_mode_menu.grid(row=0, column=1, padx=10, pady=10, sticky="w")

        # --- Log Area ---
        log_frame = ctk.CTkFrame(self.main_frame)
        log_frame.grid(row=6, column=0, padx=10, pady=10, sticky="nsew")
        log_frame.grid_rowconfigure(1, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(6, weight=1)
        ctk.CTkLabel(log_frame, text="Logs", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, padx=10, pady=(5,0), sticky="w")
        self.log_text = ctk.CTkTextbox(log_frame, wrap=tk.WORD, state="disabled")
        self.log_text.grid(row=1, column=0, padx=10, pady=(0,10), sticky="nsew")

        self._on_sql_type_change() # Set initial UI state
        self.toggle_custom_query_widgets() # Set initial state
        
        if not MYSQL_AVAILABLE:
            self.log("WARNING: 'mysql-connector-python' not found. MySQL functionality is disabled. Run 'pip install mysql-connector-python' to enable it.")
        if not PYODBC_AVAILABLE:
            self.log("WARNING: 'pyodbc' or 'sqlalchemy' not found. SQL Server functionality is disabled. Run 'pip install pyodbc sqlalchemy' to enable it.")

    def create_menu(self):
        """Creates the main menu bar for the application."""
        menu_bar = tk.Menu(self.root)

        # Help Menu
        help_menu = tk.Menu(menu_bar, tearoff=0)
        help_menu.add_command(label="About", command=self._show_about_dialog)
        menu_bar.add_cascade(label="Help", menu=help_menu)

        self.root.config(menu=menu_bar)

    def _show_about_dialog(self):
        """Displays the about dialog box."""
        messagebox.showinfo(
            "About SQL/NoSQL Converter",
            "SQL <-> NoSQL Converter\n\nVersion: 1.1\n\nAn application to convert data between various SQL databases (SQLite, PostgreSQL, MySQL, SQL Server) and MongoDB."
        )

    def _on_sql_type_change(self, *args):
        """Shows/hides UI elements based on the selected SQL database type."""
        sql_type = self.sql_type.get()
        if sql_type == "SQLite":
            self.mssql_widgets_frame.grid_forget()
            self.mysql_widgets_frame.grid_forget()
            self.postgres_widgets_frame.grid_forget()
            self.sqlite_widgets_frame.grid(row=2, column=0, columnspan=3, padx=5, pady=5, sticky="ew")
        elif sql_type == "MySQL":
            if not MYSQL_AVAILABLE:
                messagebox.showwarning("Dependency Missing", "MySQL support is not available. Please run 'pip install mysql-connector-python' to enable it.")
                self.sql_type.set("SQLite") # Revert to default
                self._on_sql_type_change() # Refresh UI
                return
            self.mssql_widgets_frame.grid_forget()
            self.sqlite_widgets_frame.grid_forget()
            self.postgres_widgets_frame.grid_forget()
            self.mysql_widgets_frame.grid(row=2, column=0, columnspan=3, padx=5, pady=5, sticky="ew")
        elif sql_type == "PostgreSQL":
            self.mssql_widgets_frame.grid_forget()
            self.sqlite_widgets_frame.grid_forget()
            self.mysql_widgets_frame.grid_forget()
            self.postgres_widgets_frame.grid(row=2, column=0, columnspan=3, padx=5, pady=5, sticky="ew")
        elif sql_type == "SQL Server":
            if not PYODBC_AVAILABLE:
                messagebox.showwarning("Dependency Missing", "SQL Server support is not available. Please run 'pip install pyodbc sqlalchemy' to enable it.")
                self.sql_type.set("SQLite") # Revert to default
                self._on_sql_type_change() # Refresh UI
                return
            self.sqlite_widgets_frame.grid_forget()
            self.postgres_widgets_frame.grid_forget()
            self.mysql_widgets_frame.grid_forget()
            self.mssql_widgets_frame.grid(row=2, column=0, columnspan=3, padx=5, pady=5, sticky="ew")

    def _change_appearance_mode(self, new_appearance_mode: str):
        """Changes the appearance mode of the application."""
        ctk.set_appearance_mode(new_appearance_mode)
        self.log(f"Appearance mode changed to {new_appearance_mode}.")

    def log(self, message):
        """Appends a message to the log text area."""
        def _log():
            self.log_text.configure(state="normal")
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.configure(state="disabled")
            self.log_text.see(tk.END) # Auto-scroll
        # Ensure UI updates are done in the main thread
        self.root.after(0, _log)

    def browse_sqlite_db(self):
        """Opens a file dialog to select a SQLite DB and loads its tables."""
        path = filedialog.askopenfilename(
            title="Select SQLite Database File",
            filetypes=[("SQLite Databases", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")]
        )
        if not path:
            return

        self.sqlite_path.set(path)
        self.log(f"Selected SQLite DB: {path}")
        try:
            conn = sqlite3.connect(path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

            if tables:
                self.combo_sql_tables.configure(values=tables)
                self.combo_sql_tables.set(tables[0])
                self.log(f"Found tables: {', '.join(tables)}")
            else:
                self.combo_sql_tables.configure(values=[])
                self.combo_sql_tables.set("")
                self.log("No tables found in the selected database.")
        except Exception as e:
            messagebox.showerror("Error Reading DB", f"Failed to read tables from the database.\nError: {e}")
            self.log(f"ERROR: Could not read tables from {path}. Reason: {e}")

    def connect_and_load_postgres_tables(self):
        """Connects to PostgreSQL and loads schema tables."""
        try:
            self.log("Connecting to PostgreSQL...")
            conn = psycopg2.connect(
                host=self.pg_host.get(),
                port=self.pg_port.get(),
                dbname=self.pg_dbname.get(),
                user=self.pg_user.get(),
                password=self.pg_password.get()
            )
            self.pg_conn = conn # Store the connection
            self.log("✅ PostgreSQL connection successful.")
            
            cursor = conn.cursor()
            # Query to get tables from the public schema
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()

            if tables:
                self.combo_sql_tables.configure(values=tables)
                self.combo_sql_tables.set(tables[0])
                self.log(f"Found tables: {', '.join(tables)}")
            else:
                self.combo_sql_tables.configure(values=[])
                self.combo_sql_tables.set("")
                self.log("No tables found in the public schema.")

        except psycopg2.Error as e:
            self.pg_conn = None
            messagebox.showerror("PostgreSQL Connection Error", f"Could not connect to PostgreSQL.\nError: {e}")
            self.log(f"ERROR: PostgreSQL connection failed. Reason: {e}")

    def connect_and_load_mssql_tables(self):
        """Connects to MS SQL Server and loads schema tables."""
        try:
            self.log("Connecting to MS SQL Server...")
            server = self.mssql_server.get()
            dbname = self.mssql_dbname.get()
            user = self.mssql_user.get()
            password = self.mssql_password.get()

            # Find the best available ODBC driver
            drivers = [d for d in pyodbc.drivers() if d.startswith('ODBC Driver') and 'for SQL Server' in d]
            if not drivers:
                raise ConnectionError("No suitable MS SQL Server ODBC driver found. Please install it from Microsoft's website.")
            driver = drivers[-1] # Get the latest version
            self.mssql_driver = driver # Store the driver for later use
            self.log(f"Using ODBC Driver: {driver}")

            def build_conn_str(database):
                base_conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};TrustServerCertificate=yes;'
                if user:
                    return base_conn_str + f'UID={user};PWD={password};'
                else: # Use Windows Authentication
                    return base_conn_str + 'Trusted_Connection=yes;'

            try:
                # First, try connecting to the specified database
                conn_str = build_conn_str(dbname)
                conn = pyodbc.connect(conn_str)
                self.mssql_conn = conn
                self.log(f"✅ MS SQL Server connection successful to database '{dbname}'.")
            except pyodbc.Error as e:
                # If it fails (e.g., DB doesn't exist or no access), try connecting to 'master'
                if '4060' in str(e): # Error 4060 is "Cannot open database"
                    self.log(f"Could not connect to '{dbname}'. Trying to connect to 'master' database instead to verify login...")
                    try:
                        conn_str_master = build_conn_str('master')
                        conn = pyodbc.connect(conn_str_master)
                        # If this succeeds, the issue is DB-specific. The user can still convert from other DBs.
                        self.mssql_conn = conn
                        self.log("✅ MS SQL Server connection successful to 'master' DB.")
                        messagebox.showwarning("Database Access Issue", f"Successfully connected to the SQL Server instance, but could not access the database '{dbname}'.\n\nPlease check that the database exists and that your user has permission to access it. You can still use the app to convert data *to* this server.")
                    except pyodbc.Error as master_e:
                        raise master_e # If master also fails, raise the more fundamental error
                else:
                    raise e # Re-raise the original error if it's not a DB access issue

            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()

            if tables:
                self.combo_sql_tables.configure(values=tables)
                self.combo_sql_tables.set(tables[0])
                self.log(f"Found tables: {', '.join(tables)}")
            else:
                self.combo_sql_tables.configure(values=[])
                self.combo_sql_tables.set("")
                self.log(f"No tables found in database '{dbname}'.")

        except (pyodbc.Error, ConnectionError) as e:
            self.mssql_conn = None
            messagebox.showerror("MS SQL Server Connection Error", f"Could not connect to SQL Server.\nError: {e}")
            self.log(f"ERROR: MS SQL Server connection failed. Reason: {e}")

    def connect_and_load_mysql_tables(self):
        """Connects to MySQL and loads schema tables."""
        try:
            db_name = self.mysql_dbname.get()
            if not db_name:
                messagebox.showwarning("Input Required", "Please enter a Database Name for MySQL.")
                return

            self.log("Connecting to MySQL...")
            conn = mysql.connector.connect(
                host=self.mysql_host.get(),
                port=self.mysql_port.get(),
                database=db_name,
                user=self.mysql_user.get(),
                password=self.mysql_password.get()
            )
            self.mysql_conn = conn # Store the connection
            self.log("✅ MySQL connection successful.")

            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()

            if tables:
                self.combo_sql_tables.configure(values=tables)
                self.combo_sql_tables.set(tables[0])
                self.log(f"Found tables: {', '.join(tables)}")
            else:
                self.combo_sql_tables.configure(values=[])
                self.combo_sql_tables.set("")
                self.log(f"No tables found in database '{db_name}'.")

        except mysql.connector.Error as e:
            self.mysql_conn = None
            messagebox.showerror("MySQL Connection Error", f"Could not connect to MySQL.\nError: {e}")
            self.log(f"ERROR: MySQL connection failed. Reason: {e}")

    def connect_and_load_mongo(self):
        """Connects to MongoDB and loads the list of collections for the selected DB."""
        uri = self.mongo_uri.get()
        db_name = self.mongo_db_name.get()
        if not uri or not db_name:
            messagebox.showwarning("Input Required", "Please provide both MongoDB URI and Database Name.")
            return

        def _connect():
            self._toggle_buttons(False)
            try:
                self.log(f"Connecting to MongoDB at {uri}...")
                client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                client.server_info()  # Force connection check
                self.mongo_client = client
                self.log("✅ MongoDB connection successful.")

                db = self.mongo_client[db_name]
                collections = db.list_collection_names()
                
                def _update_ui_success():
                    if collections:
                        self.combo_mongo_collections.configure(values=collections)
                        self.combo_mongo_collections.set(collections[0])
                        self.log(f"Found collections in '{db_name}': {', '.join(collections)}")
                    else:
                        self.combo_mongo_collections.configure(values=[])
                        self.combo_mongo_collections.set("")
                        self.log(f"No collections found in database '{db_name}'.")
                
                self.root.after(0, _update_ui_success)

            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                self.mongo_client = None
                self.root.after(0, lambda: messagebox.showerror("MongoDB Connection Error", f"Could not connect to MongoDB.\nError: {e}"))
                self.log(f"ERROR: MongoDB connection failed. Reason: {e}")
            except Exception as e:
                self.mongo_client = None
                self.root.after(0, lambda: messagebox.showerror("Error", f"An unexpected error occurred: {e}"))
                self.log(f"ERROR: An unexpected error occurred: {e}")
            finally:
                self.root.after(0, lambda: self._toggle_buttons(True))

        threading.Thread(target=_connect, daemon=True).start()


    def _show_preview_window(self, df, title):
        """Creates a Toplevel window to display a DataFrame in a Treeview."""
        if df.empty:
            messagebox.showinfo("Preview", "The selected table or collection is empty.")
            return

        preview_win = ctk.CTkToplevel(self.root)
        preview_win.title(title)
        preview_win.geometry("700x400")

        frame = ctk.CTkFrame(preview_win)
        frame.pack(padx=10, pady=10, fill="both", expand=True)

        cols = list(df.columns)
        tree = ttk.Treeview(frame, columns=cols, show="headings")

        for col in cols:
            tree.heading(col, text=col)
            tree.column(col, width=100, anchor="w")

        for index, row in df.iterrows():
            # Convert any non-string/numeric types to string for display
            values = [str(v) for v in row]
            tree.insert("", "end", values=values)

        # Scrollbars
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")
        tree.pack(side="left", fill="both", expand=True)

    def toggle_custom_query_widgets(self):
        """Enables or disables custom query widgets based on the checkbox state."""
        state = "normal" if self.use_custom_query.get() else "disabled"
        self.custom_query_text.configure(state=state)
        self.entry_custom_collection.configure(state=state)
        
        # Also disable/enable the table selector
        combo_state = "disabled" if self.use_custom_query.get() else "readonly"
        self.combo_sql_tables.configure(state=combo_state)

    def _quote_sql_identifier(self, identifier):
        """Quotes an SQL identifier correctly based on the selected DB type."""
        sql_type = self.sql_type.get()
        if sql_type == "PostgreSQL":
            return f'"{identifier}"'
        elif sql_type == "SQL Server":
            return f'[{identifier}]'
        else:  # SQLite and default
            return f'`{identifier}`'


    def preview_sql_data(self):
        """Shows a preview of the first 10 rows of the selected SQL table/query."""
        sql_type = self.sql_type.get()
        conn = None

        if sql_type == "SQLite":
            if not self.sqlite_path.get():
                messagebox.showwarning("Input Missing", "Please select a SQLite DB to preview.")
                return
        elif sql_type == "PostgreSQL":
            if not self.pg_conn or self.pg_conn.closed:
                messagebox.showwarning("Not Connected", "Please connect to PostgreSQL first.")
                return
        elif sql_type == "MySQL":
            if not self.mysql_conn or not self.mysql_conn.is_connected():
                messagebox.showwarning("Not Connected", "Please connect to MySQL first.")
                return
        elif sql_type == "SQL Server":
            if not self.mssql_conn:
                messagebox.showwarning("Not Connected", "Please connect to SQL Server first.")
                return

        try:
            query = ""
            title_name = ""
            if self.use_custom_query.get():
                query = self.custom_query_text.get("1.0", tk.END).strip()
                if not query:
                    messagebox.showwarning("Input Missing", "Please enter a custom query to preview.")
                    return
                title_name = "Custom Query"
            else:
                table_name = self.combo_sql_tables.get()
                if not table_name:
                    messagebox.showwarning("Input Missing", "Please select a table to preview.")
                    return
                
                if sql_type == "SQL Server": # MS SQL uses TOP N
                    query = f"SELECT TOP 10 * FROM {self._quote_sql_identifier(table_name)}"
                else: # SQLite, MySQL, and PostgreSQL use LIMIT N
                    query = f"SELECT * FROM {self._quote_sql_identifier(table_name)} LIMIT 10"
                title_name = table_name

            if sql_type == "SQLite":
                conn = sqlite3.connect(self.sqlite_path.get())
            elif sql_type == "PostgreSQL":
                conn = self.pg_conn
            elif sql_type == "MySQL":
                conn = self.mysql_conn
            else: # SQL Server
                conn = self.mssql_conn

            df = pd.read_sql(query, conn)
            if sql_type == "SQLite":
                conn.close()  # Close SQLite connection, keep PG connection open

            self._show_preview_window(df, f"Preview of '{title_name}'")
        except Exception as e:
            messagebox.showerror("Preview Error", f"Could not fetch data for preview.\nError: {e}")
            self.log(f"ERROR during SQL preview: {e}")

    def preview_mongo_data(self):
        """Shows a preview of the first 10 documents of the selected MongoDB collection."""
        collection_name = self.combo_mongo_collections.get()
        if not self.mongo_client:
            messagebox.showwarning("Not Connected", "Please connect to MongoDB first.")
            return
        if not collection_name:
            messagebox.showwarning("Input Missing", "Please select a MongoDB collection to preview.")
            return

        try:
            db = self.mongo_client[self.mongo_db_name.get()]
            cursor = db[collection_name].find().limit(10)
            # Normalize nested JSON for better preview
            df = pd.json_normalize(list(cursor))
            self._show_preview_window(df, f"Preview of '{collection_name}'")
        except Exception as e:
            messagebox.showerror("Preview Error", f"Could not fetch data for preview.\nError: {e}")
            self.log(f"ERROR during MongoDB preview: {e}")

    def _start_progress(self):
        """Shows and resets the progress bar."""
        self.progress_frame.grid(row=7, column=0, padx=10, pady=(0, 5), sticky="ew") # Moved down to accommodate appearance frame
        self.progress_bar.set(0)

    def _update_progress(self, value):
        """Updates the progress bar's value."""
        self.progress_bar.set(value / 100)

    def _stop_progress(self):
        """Hides the progress bar."""
        self.progress_bar.set(0)
        self.progress_frame.grid_forget()


    def _toggle_buttons(self, enabled):
        """Enable or disable conversion buttons."""
        state = "normal" if enabled else "disabled" 
        self.btn_sql_to_mongo.configure(state=state)
        self.btn_entire_db_to_mongo.configure(state=state)
        self.btn_mongo_to_sql.configure(state=state)
        self.btn_entire_mongo_to_sql.configure(state=state)
        self.btn_export_sql_csv.configure(state=state)
        self.btn_export_mongo_csv.configure(state=state)
        # Also toggle other interactive widgets to prevent changes during conversion
        self.entry_sqlite_path.configure(state="disabled" if not enabled else "normal")
        self.combo_sql_tables.configure(state="disabled" if not enabled else "readonly")
        self.entry_mongo_uri.configure(state="disabled" if not enabled else "normal")
        self.entry_mongo_db.configure(state="disabled" if not enabled else "normal")
        self.combo_mongo_collections.configure(state="disabled" if not enabled else "readonly")


    def _run_conversion_in_thread(self, target_func):
        """
        Runs the given conversion function in a separate thread to keep the UI responsive.
        """
        self._toggle_buttons(False)
        self.root.after(0, self._start_progress)
        thread = threading.Thread(target=target_func, daemon=True)
        thread.start()

    def convert_sql_to_mongo(self):
        """Starts the SQLite to MongoDB conversion in a new thread."""
        self._run_conversion_in_thread(self._worker_sql_to_mongo)

    def _worker_sql_to_mongo(self):
        """The actual worker for SQLite to MongoDB conversion."""
        sql_type = self.sql_type.get()
        use_custom = self.use_custom_query.get()
        conn = None

        if sql_type == "SQLite" and not self.sqlite_path.get():
            self.root.after(0, lambda: messagebox.showwarning("Input Missing", "Please select a SQLite database file."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return
        elif sql_type == "PostgreSQL" and (not self.pg_conn or self.pg_conn.closed):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to PostgreSQL first."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return
        elif sql_type == "MySQL" and (not self.mysql_conn or not self.mysql_conn.is_connected()):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MySQL first."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return
        elif sql_type == "SQL Server" and not self.mssql_conn:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to SQL Server first."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return

        if not self.mongo_client:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MongoDB first."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return

        db_name = self.mongo_db_name.get()
        
        try: # Main logic
            query = ""
            if use_custom:
                query = self.custom_query_text.get("1.0", tk.END).strip()
                collection_name = self.custom_collection_name.get().strip()
                if not query or not collection_name:
                    self.root.after(0, lambda: messagebox.showwarning("Input Missing", "Custom query and target collection name are required."))
                    raise ValueError("Custom query or collection name missing.")
            else:
                table_name = self.combo_sql_tables.get()
                if not table_name:
                    self.root.after(0, lambda: messagebox.showwarning("Input Missing", "Please select a table to convert."))
                    raise ValueError("Table name missing.")
                query = f"SELECT * FROM {self._quote_sql_identifier(table_name)}"
                collection_name = table_name

            self.log(f"Starting conversion: {sql_type} to MongoDB collection '{collection_name}'...")

            # 1. Read data from SQLite
            self.root.after(0, lambda: self._update_progress(25))
            if sql_type == "SQLite":
                conn = sqlite3.connect(self.sqlite_path.get())
            elif sql_type == "PostgreSQL":
                conn = self.pg_conn
            elif sql_type == "MySQL":
                conn = self.mysql_conn
            else: # SQL Server
                conn = self.mssql_conn

            df = pd.read_sql(query, conn)
            if sql_type == "SQLite": # Only close SQLite connection, others are persistent
                conn.close()
            self.log(f"Read {len(df)} rows from {sql_type}.")

            # 2. Convert DataFrame to list of dictionaries (records)
            self.root.after(0, lambda: self._update_progress(50))
            data_to_insert = df.to_dict(orient='records')

            # Convert datetime objects to ISO format strings to prevent encoding errors
            for record in data_to_insert:
                for key, value in record.items():
                    if hasattr(value, 'isoformat'):
                        record[key] = value.isoformat()

            if not data_to_insert:
                self.log("Warning: Source is empty. Nothing to convert.")
                self.root.after(0, lambda: messagebox.showinfo("Complete", "The source table/query is empty. No data was converted."))
                return

            # 3. Insert data into MongoDB
            db = self.mongo_client[db_name]
            collection = db[collection_name]

            # Ask for overwrite confirmation in the main thread
            overwrite = False
            if collection_name in db.list_collection_names():
                # Use a blocking mechanism to get user input from the main thread
                confirm_event = threading.Event()
                user_choice = tk.BooleanVar()
                def ask():
                    user_choice.set(messagebox.askyesno("Confirm Overwrite", f"Collection '{collection_name}' already exists. Overwrite it?"))
                    confirm_event.set()
                self.root.after(0, ask)
                confirm_event.wait() # Wait for the user to click yes/no
                overwrite = user_choice.get()

                if overwrite:
                    collection.drop()
                    self.log(f"Dropped existing collection '{collection_name}'.")
                else:
                    self.log("Conversion cancelled by user.")
                    return

            self.root.after(0, lambda: self._update_progress(75))
            result = collection.insert_many(data_to_insert)
            self.log(f"Successfully inserted {len(result.inserted_ids)} documents into '{collection_name}'.")
            self.root.after(0, lambda: messagebox.showinfo("Success", f"Successfully converted {len(df)} records to MongoDB collection '{collection_name}'."))

            # Refresh collections list
            self.root.after(0, lambda: self._update_progress(100))
            # After conversion, refresh the list and select the newly created collection
            def refresh_and_select():
                self.connect_and_load_mongo()
                self.combo_mongo_collections.set(collection_name)
            self.root.after(100, refresh_and_select) # Use a small delay to ensure connection is established

        except Exception as e: # Error handling
            self.root.after(0, lambda: messagebox.showerror("Conversion Error", f"An error occurred during conversion: {e}"))
            self.log(f"ERROR during SQL to NoSQL conversion: {e}")
        finally: # Always re-enable buttons
            self.root.after(0, self._stop_progress)
            self.root.after(0, lambda: self._toggle_buttons(True))
            
    def convert_entire_db_to_mongo(self):
        """Starts the full DB to MongoDB conversion in a new thread."""
        self._run_conversion_in_thread(self._worker_convert_entire_db_to_mongo)

    def _worker_convert_entire_db_to_mongo(self):
        """The actual worker for converting all tables in an SQL DB to MongoDB."""
        sql_type = self.sql_type.get()
        # Initialize conn here to be used throughout the function
        conn = None

        # --- Connection and Input Validation ---
        if sql_type == "SQLite" and not self.sqlite_path.get():
            self.root.after(0, lambda: messagebox.showwarning("Input Missing", "Please select a SQLite database file."))
            return
        if sql_type == "PostgreSQL" and (not self.pg_conn or self.pg_conn.closed):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to PostgreSQL first."))
            return
        if sql_type == "MySQL" and (not self.mysql_conn or not self.mysql_conn.is_connected()):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MySQL first."))
            return
        if sql_type == "SQL Server" and not self.mssql_conn:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to SQL Server first."))
            return
        if not self.mongo_client:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MongoDB first."))
            return

        try:
            # --- Establish connection and get all tables ---
            tables_to_convert = []
            if sql_type == "SQLite":
                conn = sqlite3.connect(self.sqlite_path.get())
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables_to_convert = [row[0] for row in cursor.fetchall()]
            elif sql_type == "PostgreSQL":
                conn = self.pg_conn
                cursor = conn.cursor()
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                tables_to_convert = [row[0] for row in cursor.fetchall()]
            elif sql_type == "MySQL":
                conn = self.mysql_conn
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                tables_to_convert = [row[0] for row in cursor.fetchall()]
            else: # SQL Server
                conn = self.mssql_conn
                cursor = conn.cursor()
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
                tables_to_convert = [row[0] for row in cursor.fetchall()]
            cursor.close()

            if not tables_to_convert:
                self.root.after(0, lambda: messagebox.showinfo("No Tables", "No tables found in the selected SQL database to convert."))
                return

            # --- Ask for overwrite strategy once ---
            confirm_event = threading.Event()
            user_choice = tk.StringVar()
            def ask_strategy():
                result = messagebox.askquestion("Confirm Overwrite Strategy", "For collections that already exist in MongoDB, do you want to Overwrite them?\n\n- 'Yes' to Overwrite existing collections.\n- 'No' to Skip existing collections.", type=messagebox.YESNO)
                user_choice.set(result)
                confirm_event.set()
            self.root.after(0, ask_strategy)
            confirm_event.wait()
            strategy = user_choice.get() # 'yes' or 'no'

            self.log(f"Starting full database conversion ({len(tables_to_convert)} tables) with strategy: {'Overwrite' if strategy == 'yes' else 'Skip'}.")

            # --- Initialize MongoDB connection and counters ---
            mongo_db = self.mongo_client[self.mongo_db_name.get()]
            existing_collections = mongo_db.list_collection_names()
            
            converted_count = 0
            skipped_count = 0

            # --- Loop through each table and convert ---
            for i, table_name in enumerate(tables_to_convert):
                progress = (i / len(tables_to_convert)) * 100
                self.root.after(0, lambda p=progress: self._update_progress(p))

                if table_name in existing_collections and strategy == 'no':
                    self.log(f"Skipping table '{table_name}' as it already exists in MongoDB.")
                    skipped_count += 1
                    continue

                self.log(f"Converting table '{table_name}' ({i+1}/{len(tables_to_convert)})...")

                # Read data from SQL table
                query = f"SELECT * FROM {self._quote_sql_identifier(table_name)}"
                df = pd.read_sql(query, conn)

                # Convert to list of dicts and handle datetime
                data_to_insert = df.to_dict(orient='records')
                for record in data_to_insert:
                    for key, value in record.items():
                        if hasattr(value, 'isoformat'):
                            record[key] = value.isoformat()

                if not data_to_insert:
                    self.log(f"Table '{table_name}' is empty. Skipping.")
                    continue

                # Insert into MongoDB
                collection = mongo_db[table_name]
                if table_name in existing_collections and strategy == 'yes':
                    collection.drop()
                
                collection.insert_many(data_to_insert)
                self.log(f"✅ Successfully inserted {len(data_to_insert)} documents into '{table_name}'.")
                converted_count += 1

            # --- Finalization ---
            self.root.after(0, lambda: self._update_progress(100))
            self.log("Full database conversion finished.")
            self.root.after(0, lambda: messagebox.showinfo("Conversion Complete", f"Finished converting database.\n\n- Converted: {converted_count} tables\n- Skipped: {skipped_count} tables"))
            self.root.after(100, self.connect_and_load_mongo) # Refresh collection list

        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Conversion Error", f"An error occurred during full DB conversion: {e}"))
            self.log(f"ERROR during full DB conversion: {e}")
        finally:
            # Close the connection only if it's SQLite, as others are persistent
            if sql_type == "SQLite" and conn:
                conn.close()
            self.root.after(0, self._stop_progress)
            self.root.after(0, lambda: self._toggle_buttons(True))

    def convert_entire_mongo_to_sql(self):
        """Starts the full MongoDB to SQL conversion in a new thread."""
        self._run_conversion_in_thread(self._worker_convert_entire_mongo_to_sql)

    def _worker_convert_entire_mongo_to_sql(self):
        """The actual worker for converting an entire MongoDB database to SQL."""
        sql_type = self.sql_type.get()
        db_name = self.mongo_db_name.get()

        # --- Input Validation ---
        if not self.mongo_client:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MongoDB first."))
            return
        if sql_type == "PostgreSQL" and (not self.pg_conn or self.pg_conn.closed):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to PostgreSQL to use it as a destination."))
            return
        if sql_type == "MySQL" and (not self.mysql_conn or not self.mysql_conn.is_connected()):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MySQL to use it as a destination."))
            return
        if sql_type == "SQL Server" and not self.mssql_conn:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to SQL Server to use it as a destination."))
            return

        output_db_path = None
        engine = None
        try:
            # --- Get all collections from MongoDB ---
            mongo_db = self.mongo_client[db_name]
            collections_to_convert = mongo_db.list_collection_names()

            if not collections_to_convert:
                self.root.after(0, lambda: messagebox.showinfo("No Collections", f"No collections found in MongoDB database '{db_name}' to convert."))
                return

            # --- Ask for overwrite strategy ---
            confirm_event = threading.Event()
            user_choice = tk.StringVar()
            def ask_strategy():
                msg = f"This will convert {len(collections_to_convert)} collections to {sql_type} tables. For tables that already exist, do you want to Overwrite them?\n\n- 'Yes' to Overwrite existing tables.\n- 'No' to Skip existing tables."
                result = messagebox.askquestion("Confirm Overwrite Strategy", msg, type=messagebox.YESNO)
                user_choice.set(result)
                confirm_event.set()
            self.root.after(0, ask_strategy)
            confirm_event.wait()
            strategy = user_choice.get() # 'yes' or 'no'

            self.log(f"Starting full MongoDB conversion ({len(collections_to_convert)} collections) with strategy: {'Overwrite' if strategy == 'yes' else 'Skip'}.")

            # --- Setup SQL Engine/Connection ---
            if sql_type == "SQLite":
                output_db_path = Path(f"{db_name}_from_mongo.db")
                if output_db_path.exists() and strategy == 'yes':
                    self.log(f"Deleting existing SQLite DB file: {output_db_path}")
                    output_db_path.unlink()
                conn = sqlite3.connect(output_db_path)
            else:
                if sql_type == "PostgreSQL":
                    uri = f"postgresql+psycopg2://{self.pg_user.get()}:{self.pg_password.get()}@{self.pg_host.get()}:{self.pg_port.get()}/{self.pg_dbname.get()}" # pragma: allowlist secret
                elif sql_type == "MySQL":
                    uri = f"mysql+mysqlconnector://{self.mysql_user.get()}:{self.mysql_password.get()}@{self.mysql_host.get()}:{self.mysql_port.get()}/{self.mysql_dbname.get()}" # pragma: allowlist secret
                else: # SQL Server
                    server = self.mssql_server.get()
                    dbname = self.mssql_dbname.get()
                    driver_name = self.mssql_driver.replace(' ', '+')
                    user = self.mssql_user.get()
                    password = self.mssql_password.get()
                    if user:
                        uri = f"mssql+pyodbc://{user}:{password}@{server}/{dbname}?driver={driver_name}&TrustServerCertificate=yes" # pragma: allowlist secret
                    else:
                        uri = f"mssql+pyodbc://{server}/{dbname}?driver={driver_name}&TrustServerCertificate=yes&trusted_connection=yes" # pragma: allowlist secret
                engine = create_engine(uri)
                conn = engine.connect()

            converted_count = 0
            skipped_count = 0

            # --- Get existing tables from SQL DB for skip logic ---
            existing_sql_tables = []
            if strategy == 'no':
                if sql_type == "SQLite":
                    # For SQLite, we need to inspect the file directly
                    if output_db_path.exists():
                        temp_conn_sqlite = sqlite3.connect(output_db_path)
                        cursor = temp_conn_sqlite.cursor()
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        existing_sql_tables = [row[0] for row in cursor.fetchall()]
                        temp_conn_sqlite.close()
                elif engine: # For other DBs, use the created engine
                    inspector = inspect(engine)
                    existing_sql_tables = inspector.get_table_names()
                self.log(f"Found existing SQL tables for skip check: {existing_sql_tables}")

            # --- Loop through each collection and convert ---
            for i, coll_name in enumerate(collections_to_convert):
                progress = (i / len(collections_to_convert)) * 100
                self.root.after(0, lambda p=progress: self._update_progress(p))

                table_name_sql = ''.join(e for e in coll_name if e.isalnum() or e == '_')
                self.log(f"Processing collection '{coll_name}' ({i+1}/{len(collections_to_convert)})...")

                if strategy == 'no' and table_name_sql in existing_sql_tables:
                    self.log(f"Skipping collection '{coll_name}' as table '{table_name_sql}' already exists in SQL database.")
                    skipped_count += 1
                    continue

                # Read data from MongoDB
                cursor = mongo_db[coll_name].find()
                data = list(cursor)

                if not data:
                    self.log(f"Collection '{coll_name}' is empty. Skipping.")
                    continue

                # Convert to DataFrame and sanitize
                df = pd.json_normalize(data, sep='_')
                if '_id' in df.columns: df['_id'] = df['_id'].astype(str)
                sanitized_columns = {col: ''.join(e for e in col if e.isalnum() or e == '_') for col in df.columns}
                df.rename(columns=sanitized_columns, inplace=True)
                for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, list)).any():
                        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

                # Write to SQL
                # Pass the engine directly to to_sql, not the connection object.
                # This allows pandas to manage the connection with the correct authentication context.
                target_conn = engine if sql_type != "SQLite" else conn
                df.to_sql(table_name_sql, target_conn, if_exists= 'replace' if strategy == 'yes' else 'append', index=False)
                self.log(f"✅ Successfully wrote {len(df)} documents to table '{table_name_sql}'.")
                converted_count += 1

            # --- Finalization ---
            self.root.after(0, lambda: self._update_progress(100))
            self.log("Full MongoDB to SQL conversion finished.")
            self.root.after(0, lambda: messagebox.showinfo("Conversion Complete", f"Finished converting database.\n\n- Converted: {converted_count} collections\n- Skipped: {skipped_count} collections"))
            if sql_type != "SQLite":
                self.root.after(100, getattr(self, f"connect_and_load_{sql_type.lower().replace(' ', '')}_tables"))

        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Conversion Error", f"An error occurred during full MongoDB conversion: {e}"))
            self.log(f"ERROR during full MongoDB conversion: {e}")
        finally:
            # The engine's connection is closed, but for SQLite we close the direct connection.
            if sql_type == "SQLite" and 'conn' in locals() and conn:
                conn.close()
            self.root.after(0, self._stop_progress)
            self.root.after(0, lambda: self._toggle_buttons(True))

    def convert_mongo_to_sql(self):
        """Starts the MongoDB to SQLite conversion in a new thread."""
        self._run_conversion_in_thread(self._worker_mongo_to_sql)

    def _worker_mongo_to_sql(self):
        """The actual worker for MongoDB to SQLite conversion."""
        sql_type = self.sql_type.get()
        collection_name = self.combo_mongo_collections.get()
        db_name = self.mongo_db_name.get()

        if not collection_name:
            self.root.after(0, lambda: messagebox.showwarning("Input Missing", "Please select a MongoDB collection."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return # Added return here
        
        if sql_type == "PostgreSQL" and (not self.pg_conn or self.pg_conn.closed): # Changed from elif to if
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to PostgreSQL to use it as a destination."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return # Added return here
        if sql_type == "MySQL" and (not self.mysql_conn or not self.mysql_conn.is_connected()):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MySQL to use it as a destination."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return # Added return here
        if sql_type == "SQL Server" and not self.mssql_conn: # Changed from elif to if
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to SQL Server to use it as a destination."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return

        if not self.mongo_client:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MongoDB first."))
            self.root.after(0, lambda: self._toggle_buttons(True))
            return

        # Define output SQLite DB path
        output_db_path = None
        if sql_type == "SQLite":
            output_db_path = Path(f"{collection_name}_from_mongo.db")
            if output_db_path.exists():
                confirm_event = threading.Event()
                user_choice = tk.BooleanVar()
                def ask():
                    user_choice.set(messagebox.askyesno("Confirm Overwrite", f"Database file '{output_db_path}' already exists. Overwrite it?"))
                    confirm_event.set()
                self.root.after(0, ask)
                confirm_event.wait()
                if not user_choice.get():
                    self.log("Conversion cancelled by user.")
                    self.root.after(0, self._stop_progress)
                    self.root.after(0, lambda: self._toggle_buttons(True))
                    return

        try: # Main logic
            self.log(f"Starting conversion: MongoDB collection '{collection_name}' to {sql_type} table...")

            # 1. Read data from MongoDB
            self.root.after(0, lambda: self._update_progress(25))
            db = self.mongo_client[db_name]
            collection = db[collection_name]
            cursor = collection.find()
            data = list(cursor)

            if not data:
                self.log("Warning: Collection is empty. Nothing to convert.")
                self.root.after(0, lambda: messagebox.showinfo("Complete", "The MongoDB collection is empty. No data was converted."))
                return

            self.log(f"Read {len(data)} documents from collection '{collection_name}'.")

            # 2. Convert to Pandas DataFrame using json_normalize for nested data
            self.root.after(0, lambda: self._update_progress(50))
            df = pd.json_normalize(data, sep='_')

            if '_id' in df.columns:
                df['_id'] = df['_id'].astype(str) # Convert ObjectId to string
                self.log("Converted MongoDB '_id' field to string.")

            # Sanitize column names for SQL (e.g., remove spaces, special chars)
            sanitized_columns = {col: ''.join(e for e in col if e.isalnum() or e == '_') for col in df.columns}
            df.rename(columns=sanitized_columns, inplace=True)
            self.log(f"Sanitized column names for SQL compatibility.")

            # Convert any remaining complex types (list) to JSON strings
            self.root.after(0, lambda: self._update_progress(75))
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, list)).any():
                    self.log(f"Converting list data in column '{col}' to JSON strings.")
                    # Custom JSON encoder to handle date/time objects within lists
                    def json_converter(o):
                        if hasattr(o, 'isoformat'):
                            return o.isoformat()
                        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
                    df[col] = df[col].apply(lambda x: json.dumps(x, default=json_converter) if isinstance(x, list) else x)
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

            # 3. Write data to SQLite
            self.root.after(0, lambda: self._update_progress(90))
            # Use the collection name as the table name, ensuring it's a valid SQL identifier
            table_name_sql = ''.join(e for e in collection_name if e.isalnum() or e == '_')

            if sql_type == "SQLite":
                conn = sqlite3.connect(output_db_path)
                df.to_sql(table_name_sql, conn, if_exists='replace', index=False)
                conn.close()
                self.log(f"Successfully wrote {len(df)} rows to table '{table_name_sql}' in '{output_db_path}'.")
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Successfully converted {len(df)} records to SQLite table '{table_name_sql}' in the file:\n{output_db_path.resolve()}"))

                # Ask to open the folder containing the new DB
                self.root.after(0, lambda: self._update_progress(100))
                
                confirm_event_open = threading.Event()
                open_choice = tk.BooleanVar()
                def ask_open():
                    open_choice.set(messagebox.askyesno("Open Folder", "Do you want to open the folder containing the new database file?"))
                    confirm_event_open.set()
                self.root.after(0, ask_open)
                confirm_event_open.wait()
                if open_choice.get():
                    self.root.after(0, lambda: os.startfile(output_db_path.parent))
            
            else: # PostgreSQL or SQL Server
                if sql_type == "PostgreSQL":
                    uri = f"postgresql+psycopg2://{self.pg_user.get()}:{self.pg_password.get()}@{self.pg_host.get()}:{self.pg_port.get()}/{self.pg_dbname.get()}" # pragma: allowlist secret
                elif sql_type == "MySQL":
                    uri = f"mysql+mysqlconnector://{self.mysql_user.get()}:{self.mysql_password.get()}@{self.mysql_host.get()}:{self.mysql_port.get()}/{self.mysql_dbname.get()}" # pragma: allowlist secret
                else: # SQL Server
                    server = self.mssql_server.get()
                    dbname = self.mssql_dbname.get()
                    driver_name = self.mssql_driver.replace(' ', '+')
                    user = self.mssql_user.get()
                    password = self.mssql_password.get()
                    if user:
                        uri = f"mssql+pyodbc://{user}:{password}@{server}/{dbname}?driver={driver_name}&TrustServerCertificate=yes" # pragma: allowlist secret
                    else:
                        uri = f"mssql+pyodbc://{server}/{dbname}?driver={driver_name}&TrustServerCertificate=yes&trusted_connection=yes" # pragma: allowlist secret
                engine = create_engine(uri)
                df.to_sql(table_name_sql, engine, if_exists='replace', index=False)
                self.log(f"Successfully wrote {len(df)} rows to {sql_type} table '{table_name_sql}'.")
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Successfully converted {len(df)} records to {sql_type} table '{table_name_sql}'."))
                if sql_type == "PostgreSQL": self.root.after(0, self.connect_and_load_postgres_tables) # Refresh table list
                if sql_type == "MySQL": self.root.after(0, self.connect_and_load_mysql_tables) # Refresh table list
                if sql_type == "SQL Server": self.root.after(0, self.connect_and_load_mssql_tables) # Refresh table list

        except Exception as e: # Error handling
            self.root.after(0, lambda: messagebox.showerror("Conversion Error", f"An error occurred during conversion: {e}"))
            self.log(f"ERROR during NoSQL to SQL conversion: {e}")
        finally: # Always re-enable buttons
            self.root.after(0, self._stop_progress)
            self.root.after(0, lambda: self._toggle_buttons(True))
            
    def export_sql_to_csv(self):
        """Starts the SQL to CSV export in a new thread."""
        self._run_conversion_in_thread(self._worker_export_sql_to_csv)

    def _worker_export_sql_to_csv(self):
        """The actual worker for exporting SQL data to a CSV file."""
        sql_type = self.sql_type.get()
        use_custom = self.use_custom_query.get()
        conn = None

        # --- Input Validation ---
        if sql_type == "SQLite" and not self.sqlite_path.get():
            self.root.after(0, lambda: messagebox.showwarning("Input Missing", "Please select a SQLite database file."))
            return # Added return here
        if sql_type == "PostgreSQL" and (not self.pg_conn or self.pg_conn.closed): # Changed from elif to if
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to PostgreSQL first."))
            return # Added return here
        if sql_type == "MySQL" and (not self.mysql_conn or not self.mysql_conn.is_connected()):
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MySQL first."))
            return
        if sql_type == "SQL Server" and not self.mssql_conn: # Changed from elif to if
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to SQL Server first."))
            return

        try:
            # --- Get Query ---
            query = ""
            source_name = ""
            if use_custom:
                query = self.custom_query_text.get("1.0", tk.END).strip()
                if not query:
                    raise ValueError("Custom query is empty.")
                source_name = "custom_query"
            else:
                table_name = self.combo_sql_tables.get()
                if not table_name:
                    raise ValueError("No table selected.")
                query = f"SELECT * FROM {self._quote_sql_identifier(table_name)}"
                source_name = table_name

            # --- Ask for output file path ---
            confirm_event = threading.Event()
            file_path_var = tk.StringVar()
            def ask_path():
                path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")], initialfile=f"{source_name}.csv")
                file_path_var.set(path)
                confirm_event.set()
            self.root.after(0, ask_path)
            confirm_event.wait()
            output_csv_path = file_path_var.get()

            if not output_csv_path:
                self.log("Export cancelled by user.")
                return

            self.log(f"Starting export: {sql_type} source to {output_csv_path}...")
            self.root.after(0, lambda: self._update_progress(25))

            # --- Read Data ---
            if sql_type == "SQLite":
                conn = sqlite3.connect(self.sqlite_path.get())
            elif sql_type == "PostgreSQL":
                conn = self.pg_conn
            elif sql_type == "MySQL":
                conn = self.mysql_conn
            else: # SQL Server
                conn = self.mssql_conn
            df = pd.read_sql(query, conn)
            if sql_type == "SQLite":
                conn.close()
            self.log(f"Read {len(df)} rows from {sql_type}.")
            self.root.after(0, lambda: self._update_progress(60))

            # --- Write to CSV ---
            df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
            self.log(f"✅ Successfully exported {len(df)} rows to {output_csv_path}.")
            self.root.after(0, lambda: self._update_progress(100))
            self.root.after(0, lambda: messagebox.showinfo("Success", f"Data successfully exported to:\n{output_csv_path}"))

        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Export Error", f"An error occurred during export: {e}"))
            self.log(f"ERROR during SQL to CSV export: {e}")


        # --- Finalization ---
        self.root.after(0, self._stop_progress)
        self.root.after(0, lambda: self._toggle_buttons(True))

    def export_mongo_to_csv(self):
        """Starts the MongoDB to CSV export in a new thread."""
        # This is very similar to the beginning of _worker_mongo_to_sql
        # We can reuse the same logic for fetching and normalizing data
        # The only difference is the final output step.
        # For simplicity, we'll create a dedicated worker.
        self._run_conversion_in_thread(self._worker_export_mongo_to_csv)

    def _worker_export_mongo_to_csv(self):
        """The actual worker for exporting MongoDB data to a CSV file."""
        collection_name = self.combo_mongo_collections.get()
        db_name = self.mongo_db_name.get()

        # --- Input Validation ---
        if not self.mongo_client:
            self.root.after(0, lambda: messagebox.showwarning("Not Connected", "Please connect to MongoDB first."))
        elif not collection_name:
            self.root.after(0, lambda: messagebox.showwarning("Input Missing", "Please select a MongoDB collection to export."))
        else:
            try:
                # --- Ask for output file path in main thread ---
                confirm_event = threading.Event()
                file_path_var = tk.StringVar()
                def ask_path():
                    path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")], initialfile=f"{collection_name}.csv")
                    file_path_var.set(path)
                    confirm_event.set()
                self.root.after(0, ask_path)
                confirm_event.wait()
                output_csv_path = file_path_var.get()

                if not output_csv_path:
                    self.log("Export cancelled by user.")
                    return

                self.log(f"Starting export: MongoDB collection '{collection_name}' to {output_csv_path}...")
                self.root.after(0, lambda: self._update_progress(25))

                # --- Read Data from MongoDB ---
                db = self.mongo_client[db_name]
                cursor = db[collection_name].find()
                data = list(cursor)

                if not data:
                    self.log("Warning: Collection is empty. Nothing to export.")
                    self.root.after(0, lambda: messagebox.showinfo("Complete", "The MongoDB collection is empty. No data was exported."))
                    return

                self.log(f"Read {len(data)} documents from collection '{collection_name}'.")
                self.root.after(0, lambda: self._update_progress(50))

                # --- Normalize data and write to CSV ---
                df = pd.json_normalize(data, sep='_')
                if '_id' in df.columns:
                    df['_id'] = df['_id'].astype(str) # Convert ObjectId to string

                for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, list)).any():
                        def json_converter(o):
                            if hasattr(o, 'isoformat'):
                                return o.isoformat()
                            raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
                        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
                
                self.root.after(0, lambda: self._update_progress(75))
                df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
                self.log(f"✅ Successfully exported {len(df)} rows to {output_csv_path}.")
                self.root.after(0, lambda: self._update_progress(100))
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Data successfully exported to:\n{output_csv_path}"))

            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Export Error", f"An error occurred during export: {e}"))
                self.log(f"ERROR during MongoDB to CSV export: {e}")

        # --- Finalization ---
        self.root.after(0, self._stop_progress)
        self.root.after(0, lambda: self._toggle_buttons(True))


if __name__ == "__main__":
    app_root = ctk.CTk()
    app = SQLNoSQLConverterApp(app_root)
    app_root.mainloop()
