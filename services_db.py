def init_db(*, get_db_connection, hash_password):
    conn = get_db_connection()
    c = conn.cursor()
    expected_cols = ["id", "platform", "client", "product", "sales", "company", "start_date", "end_date", "seconds", "spots", "amount_net", "updated_at"]
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
    exists = c.fetchone() is not None
    if exists:
        table_info = c.execute("PRAGMA table_info(orders)").fetchall()
        current_cols = [row[1] for row in table_info]
        required_core = [x for x in expected_cols if x != "contract_id"]
        has_core = all(col in current_cols for col in required_core)
        if not has_core:
            if current_cols != expected_cols:
                c.execute("DROP TABLE IF EXISTS orders")
                conn.commit()
                exists = False
        else:
            if "contract_id" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN contract_id TEXT")
                conn.commit()
            if "seconds_type" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN seconds_type TEXT")
                conn.commit()
            if "project_amount_net" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN project_amount_net REAL")
                conn.commit()
            if "split_amount" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN split_amount REAL")
                conn.commit()
            if "hourly_schedule_json" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN hourly_schedule_json TEXT")
                conn.commit()
            if "play_time_window" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN play_time_window TEXT")
                conn.commit()
            if "special_time_window" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN special_time_window INTEGER")
                conn.commit()
            if "region" not in current_cols:
                c.execute("ALTER TABLE orders ADD COLUMN region TEXT")
                conn.commit()
    if not exists:
        c.execute(
            """
            CREATE TABLE orders (
                id TEXT PRIMARY KEY,
                platform TEXT, client TEXT, product TEXT, sales TEXT, company TEXT,
                start_date TEXT, end_date TEXT, seconds INTEGER, spots INTEGER, amount_net REAL,
                updated_at TIMESTAMP, contract_id TEXT, seconds_type TEXT, project_amount_net REAL, split_amount REAL,
                hourly_schedule_json TEXT,
                play_time_window TEXT,
                special_time_window INTEGER,
                region TEXT
            )
        """
        )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS platform_settings (
            platform TEXT PRIMARY KEY, store_count INTEGER, daily_hours INTEGER
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ad_flight_segments (
            segment_id TEXT PRIMARY KEY, source_order_id TEXT, platform TEXT, channel TEXT, region TEXT,
            company TEXT, sales TEXT, client TEXT, product TEXT, seconds INTEGER, spots INTEGER,
            start_date DATE, end_date DATE, duration_days INTEGER, store_count INTEGER,
            total_spots INTEGER, total_store_seconds INTEGER, seconds_type TEXT, created_at TIMESTAMP, updated_at TIMESTAMP
        )
    """
    )
    try:
        table_info = c.execute("PRAGMA table_info(ad_flight_segments)").fetchall()
        current_cols = [row[1] for row in table_info]
        if "media_platform" not in current_cols:
            c.execute("ALTER TABLE ad_flight_segments ADD COLUMN media_platform TEXT")
            conn.commit()
    except Exception:
        pass
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS platform_monthly_capacity (
            media_platform TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL, daily_available_seconds INTEGER NOT NULL,
            PRIMARY KEY (media_platform, year, month)
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS platform_monthly_purchase (
            media_platform TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
            purchased_seconds INTEGER NOT NULL, purchase_price REAL NOT NULL,
            PRIMARY KEY (media_platform, year, month)
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ragic_import_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL, phase TEXT NOT NULL, ragic_id TEXT, order_no TEXT, file_token TEXT,
            imported_orders INTEGER DEFAULT 0, message TEXT
        )
    """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orders_contract_id ON orders(contract_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orders_platform ON orders(platform)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orders_client ON orders(client)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orders_sales ON orders(sales)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_segments_source_order_id ON ad_flight_segments(source_order_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_segments_media_platform ON ad_flight_segments(media_platform)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_segments_date_range ON ad_flight_segments(start_date, end_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ragic_logs_batch_id ON ragic_import_logs(batch_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ragic_logs_created_at ON ragic_import_logs(created_at)")
    conn.commit()
    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        _hash = hash_password("admin123")
        c.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)", ("admin", _hash, "行政主管"))
        conn.commit()
    conn.close()
