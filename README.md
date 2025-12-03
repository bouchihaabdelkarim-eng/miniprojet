# SQL <=> NoSQL Database Converter

A GUI desktop tool developed with Python and CustomTkinter to easily convert data between SQL databases (SQLite, PostgreSQL, MySQL, SQL Server) and NoSQL databases (MongoDB). The tool also supports exporting data to CSV files.

![App Screenshot](https://i.imgur.com/your-screenshot.png)
*Note: The image above is a placeholder. It is recommended to replace it with an actual screenshot of the application.*

---

## ✨ Key Features

- **Bidirectional Conversion**:
  - **SQL to NoSQL**: Convert full tables or custom SQL query results to MongoDB collections.
  - **NoSQL to SQL**: Convert MongoDB collections to tables in SQL databases.
- **Multi-Database Support**:
  - **SQL**: SQLite, PostgreSQL, MySQL, and Microsoft SQL Server.
  - **NoSQL**: MongoDB.
- **Export to CSV**: Export data from any source (SQL or MongoDB) to a CSV file.
- **User-Friendly UI**: A simple and clear graphical interface for managing connections and conversions.
- **Data Preview**: Ability to preview a sample of the data before performing a conversion or export.
- **Advanced Processing**:
  - Uses `threading` to run long operations without freezing the UI.
  - Flattens nested JSON data when converting from MongoDB.
  - Progress bar to show the status of ongoing operations.
- **Light/Dark/System Mode**: Supports switching between light and dark themes, or syncing with the system's appearance.

---

## 🛠️ Installation and Setup

### Prerequisites
- **Python 3.8+**
- **Git**
- **MongoDB**: The MongoDB service must be running.
- **Microsoft ODBC Driver for SQL Server** (if you plan to use SQL Server). You can download it from the official Microsoft website.

### Installation Steps

1.  **Clone the repository:**
   ```bash
   git clone <your-repository-url>
   cd <repository-folder>
   ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    ```
    -   To activate on Windows:
        ```bash
        .\venv\Scripts\activate
        ```
    -   To activate on macOS/Linux:
        ```bash
        source venv/bin/activate
        ```

3.  **Install the required libraries:**
    Create a `requirements.txt` file (if it doesn't exist) with the following content:
    ```txt
    customtkinter
    pymongo
    pandas
    psycopg2-binary
    pyodbc
    SQLAlchemy
    mysql-connector-python
    ```
    Then, run the following command:
    ```bash
    pip install -r requirements.txt
    ```

---

## 🚀 How to Use

1.  **Run the application:**
    ```bash
    python main.py
    ```

2.  **Configure the SQL Source:**
    -   Select the database type (SQLite, PostgreSQL, MySQL, SQL Server).
    -   **For SQLite**: Click "Browse" to select the database file.
    -   **For other databases**: Enter the connection details (host, port, DB name, user, and password) and click "Connect".
    -   After a successful connection, the available tables will appear in the dropdown menu.

3.  **Configure the NoSQL Source/Destination (MongoDB):**
    -   Ensure the `Connection URI` and `Database Name` are correct.
    -   Click "Connect & Refresh Collections" to connect and load the available collections.

4.  **Perform Operations:**
    -   **To Convert**: Select the desired table/collection and click the appropriate conversion button (e.g., `Convert Entire SQL DB to MongoDB` or `<< Convert MongoDB to SQL`).
    -   **To Export**: Select the source (SQL or MongoDB) and click the corresponding "Export to CSV" button.
    -   **For Custom Queries**: Enable the "Use Custom Query" option, write your SQL query, specify a target collection name, and then start the conversion.

---

## 📄 License

This project is licensed under the MIT License. See the `LICENSE` file for more details.

---

<details>
<summary><strong>النسخة العربية (Arabic Version)</strong></summary>

## محول قواعد البيانات SQL <=> NoSQL

أداة سطح مكتب بواجهة رسومية (GUI) تم تطويرها باستخدام Python و CustomTkinter لتحويل البيانات بسهولة بين قواعد بيانات SQL (SQLite, PostgreSQL, MySQL, SQL Server) وقواعد بيانات NoSQL (MongoDB). كما تدعم الأداة تصدير البيانات إلى ملفات CSV.

### ✨ الميزات الرئيسية

- **تحويل ثنائي الاتجاه**:
  - **من SQL إلى NoSQL**: تحويل الجداول الكاملة أو نتائج استعلامات SQL مخصصة إلى مجموعات (collections) في MongoDB.
  - **من NoSQL إلى SQL**: تحويل المجموعات (collections) من MongoDB إلى جداول في قواعد بيانات SQL.
- **دعم متعدد لقواعد البيانات**:
  - **SQL**: SQLite, PostgreSQL, MySQL, و Microsoft SQL Server.
  - **NoSQL**: MongoDB.
- **تصدير إلى CSV**: تصدير البيانات من أي مصدر (SQL أو MongoDB) إلى ملف CSV.
- **واجهة مستخدم سهلة**: واجهة رسومية بسيطة وواضحة لإدارة عمليات الاتصال والتحويل.
- **معاينة البيانات**: إمكانية عرض عينة من البيانات قبل إجراء عملية التحويل أو التصدير.
- **معالجة متقدمة**:
  - استخدام `threading` لتنفيذ العمليات الطويلة دون تجميد الواجهة.
  - تسوية البيانات المتداخلة (nested JSON) عند التحويل من MongoDB.
  - شريط تقدم لإظهار حالة العمليات الجارية.
- **مظهر داكن/فاتح/نظام**: يدعم التبديل بين المظهر الفاتح والداكن، أو المزامنة مع مظهر النظام.

### 🛠️ كيفية التثبيت والتشغيل

#### المتطلبات الأساسية
- **Python 3.8+**
- **Git**
- **MongoDB**: يجب أن تكون خدمة MongoDB قيد التشغيل.
- **Microsoft ODBC Driver for SQL Server** (إذا كنت ستستخدم SQL Server). يمكنك تحميله من موقع مايكروسوفت الرسمي.

#### خطوات التثبيت

1.  **استنساخ المستودع (Clone the repository):**
    ```bash
    git clone <your-repository-url>
    cd <repository-folder>
    ```

2.  **إنشاء بيئة افتراضية (مستحسن):**
    ```bash
    python -m venv venv
    ```
    -   لتفعيل البيئة على Windows:
        ```bash
        .\venv\Scripts\activate
        ```
    -   لتفعيل البيئة على macOS/Linux:
        ```bash
        source venv/bin/activate
        ```

3.  **تثبيت المكتبات المطلوبة:**
    قم بإنشاء ملف `requirements.txt` (إذا لم يكن موجودًا) بالمحتوى المذكور في النسخة الإنجليزية، ثم قم بتشغيل الأمر التالي:
    ```bash
    pip install -r requirements.txt
    ```

### 🚀 كيفية الاستخدام

1.  **تشغيل التطبيق:**
    ```bash
    python main.py
    ```

2.  **إعداد مصدر SQL:**
    -   اختر نوع قاعدة البيانات (SQLite, PostgreSQL, MySQL, SQL Server).
    -   **لـ SQLite**: اضغط "Browse" لاختيار ملف قاعدة البيانات.
    -   **للقواعد الأخرى**: أدخل تفاصيل الاتصال (المضيف، المنفذ، اسم قاعدة البيانات، المستخدم، وكلمة المرور) ثم اضغط "Connect".
    -   بعد الاتصال بنجاح، ستظهر الجداول المتاحة في القائمة المنسدلة.

3.  **إعداد وجهة/مصدر NoSQL (MongoDB):**
    -   تأكد من صحة `Connection URI` واسم قاعدة البيانات `Database Name`.
    -   اضغط "Connect & Refresh Collections" للاتصال وتحميل المجموعات المتاحة.

4.  **تنفيذ العمليات:**
    -   **للتحويل**: اختر الجدول/المجموعة المطلوبة واضغط على زر التحويل المناسب (مثل `Convert Entire SQL DB to MongoDB` أو `<< Convert MongoDB to SQL`).
    -   **للتصدير**: اختر المصدر (SQL أو MongoDB) ثم اضغط على زر التصدير إلى CSV.
    -   **للاستعلام المخصص**: فعّل خيار "Use Custom Query"، اكتب استعلام SQL، وحدد اسم المجموعة الهدف، ثم ابدأ التحويل.

</details>