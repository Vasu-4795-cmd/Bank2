import sqlite3
from datetime import datetime
import hashlib
import os

# Database file path
DB_FILE = 'bank_app.db'

class BankBackend:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.conn = None
        self.init_db()

    def init_db(self):
        """Initialize the database and create tables if they don't exist."""
        # Create DB file if it doesn’t exist
        if not os.path.exists(self.db_path):
            open(self.db_path, 'w').close()

        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()

        # Create 'customer' table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer (
                account_no INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                mobile_no TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                pin_hash TEXT NOT NULL,
                action_type TEXT NOT NULL CHECK(action_type IN ('Savings', 'Current')),
                balance REAL DEFAULT 0.00
            )
        ''')

        # Create 'transactions' table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_no INTEGER NOT NULL,
                action_type TEXT NOT NULL CHECK(action_type IN ('Credit', 'Debit')),
                amount REAL NOT NULL,
                balance_after REAL NOT NULL,
                date_time TEXT NOT NULL,
                FOREIGN KEY (account_no) REFERENCES customer(account_no) ON DELETE CASCADE
            )
        ''')

        self.conn.commit()
        cursor.close()

    def create_account(self, name, mobile_no, email, pin, action_type='Savings'):
        """Create a new account. Returns account_no on success, None on failure."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)

        cursor = self.conn.cursor()
        try:
            pin_hash = hashlib.sha256(str(pin).encode()).hexdigest()
            cursor.execute('''
                INSERT INTO customer (name, mobile_no, email, pin_hash, action_type, balance)
                VALUES (?, ?, ?, ?, ?, 0.00)
            ''', (name, mobile_no, email, pin_hash, action_type))
            account_no = cursor.lastrowid
            self.conn.commit()
            return account_no
        except sqlite3.IntegrityError as e:
            print(f"[ERROR] Account creation failed: {e}")
            self.conn.rollback()
            return None
        finally:
            cursor.close()

    def authenticate(self, account_no, pin):
        """Authenticate a user using account number and PIN."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute('SELECT pin_hash FROM customer WHERE account_no = ?', (account_no,))
        result = cursor.fetchone()
        cursor.close()
        if not result:
            return False
        expected_hash = hashlib.sha256(str(pin).encode()).hexdigest()
        return result[0] == expected_hash

    def get_balance(self, account_no):
        """Return current balance or None if account not found."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute('SELECT balance FROM customer WHERE account_no = ?', (account_no,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None

    def deposit(self, account_no, amount):
        """Deposit funds into account."""
        if amount <= 0:
            return None
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        try:
            cursor.execute('SELECT balance FROM customer WHERE account_no = ?', (account_no,))
            result = cursor.fetchone()
            if not result:
                return None
            new_balance = result[0] + amount
            cursor.execute('UPDATE customer SET balance = ? WHERE account_no = ?', (new_balance, account_no))
            date_time = datetime.now().isoformat()
            cursor.execute('''
                INSERT INTO transactions (account_no, action_type, amount, balance_after, date_time)
                VALUES (?, 'Credit', ?, ?, ?)
            ''', (account_no, amount, new_balance, date_time))
            self.conn.commit()
            return new_balance
        except Exception as e:
            print(f"[ERROR] Deposit failed: {e}")
            self.conn.rollback()
            return None
        finally:
            cursor.close()

    def withdraw(self, account_no, amount):
        """Withdraw funds if balance is sufficient."""
        if amount <= 0:
            return None
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        try:
            cursor.execute('SELECT balance FROM customer WHERE account_no = ?', (account_no,))
            result = cursor.fetchone()
            if not result or result[0] < amount:
                return None
            new_balance = result[0] - amount
            cursor.execute('UPDATE customer SET balance = ? WHERE account_no = ?', (new_balance, account_no))
            date_time = datetime.now().isoformat()
            cursor.execute('''
                INSERT INTO transactions (account_no, action_type, amount, balance_after, date_time)
                VALUES (?, 'Debit', ?, ?, ?)
            ''', (account_no, amount, new_balance, date_time))
            self.conn.commit()
            return new_balance
        except Exception as e:
            print(f"[ERROR] Withdrawal failed: {e}")
            self.conn.rollback()
            return None
        finally:
            cursor.close()

    def get_transaction_history(self, account_no, limit=10):
        """Return recent transactions as a list of dicts."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, action_type, amount, balance_after, date_time
            FROM transactions
            WHERE account_no = ?
            ORDER BY date_time DESC
            LIMIT ?
        ''', (account_no, limit))
        rows = cursor.fetchall()
        cursor.close()
        return [
            {'id': row[0], 'action_type': row[1], 'amount': row[2], 'balance_after': row[3], 'date_time': row[4]}
            for row in rows
        ]

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None


# Example usage
if __name__ == "__main__":
    backend = BankBackend()

    print("\n--- Creating Account ---")
    account_no = backend.create_account("John Doe", "1234567890", "john@example.com", 1234)
    print("Account created:", account_no)

    print("\n--- Authentication ---")
    print("Authenticated:", backend.authenticate(account_no, 1234))

    print("\n--- Deposit ---")
    print("New Balance:", backend.deposit(account_no, 1000))

    print("\n--- Withdraw ---")
    print("New Balance:", backend.withdraw(account_no, 200))

    print("\n--- Balance Check ---")
    print("Current Balance:", backend.get_balance(account_no))

    print("\n--- Transaction History ---")
    for t in backend.get_transaction_history(account_no):
        print(t)

    backend.close()
