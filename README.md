```python
readme_content = """# Ecommerce Web Analytics Log Pipeline (68M Records)

A Big Data Engineering & Business Intelligence project designed to process, normalize, and analyze a massive clickstream dataset of **67,501,979 rows** (~68 million user behavior logs) using **PySpark** and **PostgreSQL**.

## 📌 Project Overview
This project demonstrates an end-to-end Big Data pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold). It ingests raw, heavily denormalized web log data, standardizes schemas, enforces Third Normal Form (3NF) normalization, resolves time-varying product price fluctuations, and aggregates strategic business metrics for downstream Business Intelligence (BI) dashboards.

---

## 📂 Project Structure


```

```text
README.md written successfully.

```text
ecommerce_analytics_project/
│
├── data/                           # Data storage (Excluded from Git tracking)
│   ├── raw_data/                   # Raw clickstream log files (~68M rows)
│   ├── bronze/                     # Raw data ingested into schema-enforced Parquet format
│   ├── silver/                     # 3NF Normalized data tables (Events, Products, etc.)
│   └── gold/                       # Analytical data marts (RFM, Cohort, Sales Trends)
│
├── src/                            # Core pipeline source code
│   ├── __init__.py
│   ├── connection/                 # Database connectivity layer
│   │   ├── __init__.py
│   │   └── postgres_jdbc.py        # Spark-PostgreSQL JDBC connection configs
│   │
│   ├── utils/                      # Shared utility modules
│   │   ├── __init__.py
│   │   └── spark_helpers.py        # Optimized SparkSession initializer (RAM/Cores config)
│   │
│   ├── pipeline/                   # ETL pipeline processing stages
│   │   ├── __init__.py
│   │   ├── step1_bronze.py         # Raw ingestion & Type casting -> Bronze Parquet
│   │   ├── step2_silver.py         # 3NF Normalization & Price fluctuation management
│   │   └── step3_gold.py           # BI metric aggregation (RFM, Cohort, Trends)
│   │
│   └── export/                     # Database loading orchestration
│       ├── __init__.py
│       └── load_to_postgres.py     # Writes Gold/Silver Parquet data into PostgreSQL via JDBC
│
├── notebooks/                      # R&D and Exploratory Data Analysis (EDA)
│   ├── 01_eda_raw_data.ipynb       # Assessing null values, data distribution, and data types
│   ├── 02_test_normalization.ipynb # Prototyping 3NF splitting & Window functions for pricing
│   └── 03_test_bi_queries.ipynb    # Validating analytical logic (RFM, Cohort) on samples
│
├── config/                         # Centralized configuration parameters
│   └── db_config.json              # PostgreSQL credentials, host, port, and driver options
│
├── jars/                           # Database driver binaries
│   └── postgresql-42.x.x.jar       # PostgreSQL JDBC Driver jar file
│
├── .gitignore                      # Prevents committing heavy data directories to VCS
├── requirements.txt                # Python project dependencies
└── README.md                       # Project documentation and setup guide

```

---

## 🛠️ Tech Stack & Architecture

* **Data Processing:** PySpark (Spark SQL & DataFrame API) for distributed parallel computing.
* **Storage Layer:** Local File System structured in highly optimized **Apache Parquet** columnar format.
* **Target Database:** **PostgreSQL** relational database for operational querying and downstream reporting.
* **BI & Visualizations:** Matplotlib/Seaborn (Python-native reporting) or direct connection via Power BI/Tableau to PostgreSQL.

---

## 🗄️ Data Modeling & Normalization (Silver Layer)

The raw clickstream dataset is completely flat and contains severe data redundancies. The pipeline normalizes the schema into **Third Normal Form (3NF)**:

### Raw Schema:

`[event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session]`

### Target 3NF Relational Model:

1. **`users` Table:** `user_id` (PK)
2. **`categories` Table:** `category_id` (PK), `category_code`, `sub_category`, `product_type` (parsed paths)
3. **`products` Table:** `product_id` (PK), `brand`, `category_id` (FK)
4. **`events` Table:** `event_id` (Generated PK), `event_time`, `event_type`, `product_id` (FK), `user_id` (FK), `user_session`, `captured_price`

> ⚠️ **Price Fluctuation Handling:** Product prices change dynamically over time. To handle this without violating 3NF, the structural characteristics of the product (`brand`, `category_id`) are decoupled into the `products` table, while the historical snapshot price at the exact moment of transaction/interaction is maintained natively within the transactional `events` table (or tracked using an SCD Type 2 pattern if historical base price mapping is needed).

---

## 📊 Gold Layer: Strategic Business Intelligence Workloads

Once normalized, PySpark processes the Silver layer to compute four production-grade analytical models:

### 1. Sales & Interaction Trend Analysis

* Evaluates correlation between user interaction spikes (`view`, `cart`) and actual conversions (`purchase`).
* Tracks lead time bottlenecks (e.g., assessing if consumers window-shop 2-3 days prior to peak purchasing windows).

### 2. Weekly Cohort Retention Matrix

* Isolates distinct user cohorts based on their first initial purchase week.
* Calculates weekly engagement and transaction decay (`Weeks After`) to compute precise user lifetime values and friction boundaries.

### 3. Market Preference & Brand Share

* Aggregates metrics comparing market leaders (e.g., tracking why certain brands dominate Gross Merchandise Value (GMV) while competitors dominate total volume/order density).

### 4. RFM Segmentation Model

* Dynamically assigns scores across three key components:
* **Recency (R):** Days elapsed since the customer's last purchase.
* **Frequency (F):** Total volume of completed purchase transactions.
* **Monetary (M):** Cumulative lifetime capital spend.


* Segments users into actionable categories: *Champions (VIP)*, *Loyal Customers*, *At Risk*, and *Lost*.

---

## 🚀 Installation & Getting Started

### 1. Prerequisites

Ensure you have the following frameworks installed locally:

* Java Runtime Environment (JRE) / JDK 8 or 11
* Apache Spark 3.x.x
* Python 3.8+
* PostgreSQL Server Instance

### 2. Set Up the Directory and Environment

```bash
# Clone or initialize the repository layout
mkdir -p ecommerce_analytics_project/{data/{raw_data,bronze,silver,gold},src/{connection,utils,pipeline,export},notebooks,config,jars}

# Set up virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required python packages
pip install -r requirements.txt

```

### 3. Fetch the PostgreSQL JDBC Driver

1. Download the latest official JDBC driver from [PostgreSQL JDBC Driver Download](https://jdbc.postgresql.org/).
2. Place the downloaded `.jar` file inside the `jars/` directory.

### 4. Database Setup & Configuration

Update the file `config/db_config.json` with your explicit credentials:

```json
{
  "jdbc_url": "jdbc:postgresql://localhost:5432/ecommerce_db",
  "properties": {
    "user": "postgres",
    "password": "your_secure_password",
    "driver": "org.postgresql.Driver",
    "batchsize": "100000",
    "stringtype": "unspecified"
  }
}

```

---

## 🏃 Execution Workflow

Execute the end-to-end data processing pipeline sequentially:

```bash
# Step 1: Ingest raw log data and convert to schema-validated Bronze Parquet format
python3 src/pipeline/step1_bronze.py

# Step 2: Run 3NF Normalization and handle price fluctuations (Silver Layer)
python3 src/pipeline/step2_silver.py

# Step 3: Compute aggregate analytical BI models (Gold Layer)
python3 src/pipeline/step3_gold.py

# Step 4: Load final analytical insights into your local PostgreSQL database
python3 src/export/load_to_postgres.py

```

---

## 🔒 Git Security Policy

The project utilizes strict `.gitignore` patterns to prevent multi-gigabyte files or sensitive administrative database configurations from leaking into production tracking repositories. Ensure your `.gitignore` contains:

```text
data/
config/db_config.json
venv/
.ipynb_checkpoints/
*.jar

```

"""

with open("README.md", "w", encoding="utf-8") as f:
f.write(readme_content)

print("README.md written successfully.")

```
Tôi đã thiết kế và khởi tạo file tài liệu **`README.md`** chuẩn hóa cho dự án Data Engineering của bạn. Vì bạn quyết định chuyển sang sử dụng hệ quản trị cơ sở dữ liệu **PostgreSQL**, toàn bộ cấu trúc kết nối, file Driver (`.jar`), mô tả JDBC pipeline, và hướng dẫn cấu hình hệ thống trong tài liệu đều đã được đồng bộ hóa hoàn toàn theo cấu trúc của PostgreSQL.

Dưới đây là file tài liệu chi tiết cấu trúc thư mục, mô hình chuẩn hóa dữ liệu 3NF, logic xử lý biến động giá và các bài toán BI (RFM, Cohort, Trend) được viết theo quy chuẩn phân tích Big Data.

[file-tag: code-generated-file-0-1780237537974018041]

---

### 💡 Các điểm lưu ý quan trọng khi triển khai với PostgreSQL cho 68 triệu dòng log:

1. **Cấu hình JDBC Batch Size:**
   Trong file cấu hình mẫu `config/db_config.json` ở file README, tham số `"batchsize": "100000"` được thêm vào nhằm ép PySpark gom cụm dữ liệu (gồm nhiều dòng) trước khi thực hiện lệnh `INSERT` hàng loạt vào PostgreSQL. Nếu không cấu hình tham số này, Spark sẽ ghi từng dòng tuần tự hoặc chia block quá nhỏ, dễ gây nghẽn kết nối mạng (Network I/O bottleneck) và làm tràn bộ nhớ đệm của DB.

2. **Bài toán xử lý biến động giá (Price Fluctuation) trong 3NF:**
   Do giá của cùng một `product_id` có thể thay đổi theo từng khung giờ hoặc chiến dịch marketing, nếu bạn đưa cột `price` vào bảng `products` thì sẽ vi phạm dạng chuẩn 3 (vì cùng một sản phẩm sẽ sinh ra nhiều dòng trùng ID nhưng khác giá, làm mất tính duy nhất của Khóa chính - Primary Key). 
   * **Giải pháp tối ưu:** Tách thông tin cố định của sản phẩm (`brand`, `category_id`) ra bảng `products`. Giữ nguyên mức giá tại thời điểm xảy ra tương tác (`captured_price`) trực tiếp ở bảng transactional là bảng `events`.

3. **Cấu hình Tối ưu phần cứng cho PostgreSQL:**
   68 triệu dòng log khi nạp vào SQL sẽ chiếm dung lượng lưu trữ tương đối lớn và tạo áp lực lên RAM khi thực hiện các phép toán `JOIN` hoặc `INDEX`. Bạn nên cài đặt hoặc điều chỉnh các tham số cấu hình trong file `postgresql.conf` của PostgreSQL để tối ưu hiệu năng ghi:
   * Tăng `max_wal_size` và `checkpoint_completion_target` để tránh việc PostgreSQL phải ghi dữ liệu xuống đĩa cứng (Flush disk) quá liên tục trong quá trình PySpark đẩy data thông qua JDBC driver.

Bạn có thể mở file `README.md` vừa được tạo trong thư mục dự án của mình để bắt đầu viết mã nguồn cho các file ETL theo đúng sơ đồ trên. Nếu bạn cần hỗ trợ viết tiếp mã nguồn PySpark cho **Step 1 (Ingest Raw -> Bronze Parquet)** hoặc **Step 2 (Phân rã dữ liệu chuẩn hóa 3NF)**, hãy cứ nói cho mình biết nhé!

```
