import sqlite3
import logging
import asyncio
import time
import random
import smtplib
import ssl
import csv
import socket
from io import BytesIO
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters, CallbackQueryHandler
from telegram.constants import ParseMode
import threading
import hashlib
import json
import os
import shutil
import re

# ==================== CONFIGURATION ====================
BOT_TOKEN = "8610001220:AAG4OQ5ypsdr_pvWDImAtzmQGRgL0FNebLI"
BOT_NAME = "GOJO Farmer"
OWNER_ID = 8360602913
ADMIN_IDS = [8360602913]
DB_FILE = "bot_database.db"
OWNER_USERNAME = "@Gojo_farmer_support"
DEFAULT_PAYOUT_CHANNEL = "@gojofarmers"

# Constants
MAX_ACTIVE_TASKS = 1
TASK_EXPIRY_HOURS = 24
MAX_BULK_TASKS = 50
USD_TO_ETB_RATE = 180.0
MESSAGE_EXPIRY_HOURS = 72

# Email SMTP Server
EMAIL_SMTP_SERVER = "smtp.gmail.com"

current_email_index = 0
email_lock = threading.Lock()
message_deletion_queue = []

# ==================== OTP MANAGEMENT SYSTEM ====================
otp_storage = {}
used_otps = set()
admin_generated_otps = {}

# Permission keywords
PERMISSIONS = {
    'add': '📦 Bulk Add Tasks',
    'pending': '⏳ Pending Tasks',  # Has approve/reject buttons
    'pending_approval': '📋 Pending Approval List',  # VIEW ONLY - no approve/reject
    'completed': '✅ Completed Tasks',
    'payout': '💰 Pending Payouts',
    'manage': '👥 Manage Users',
    'setting': '⚙️ Admin Settings',
    'backup': '💾 Backup Data',
    'channel': '📢 Manage Channels',
    'taskinfo': '🔍 Task Info',
    'dashboard': '📊 Dashboard',
    'statistics': '📈 Statistics',
    'broadcast': '📢 Broadcast',
    'email': '📧 Email Accounts',
    'contact': '📞 Set Contact Admin',
    'referral': '🎯 Referral Settings',
    'export': '📥 Export Tasks',
    'delete': '🗑️ Delete Tasks',
    'otp': '🔐 OTP',
    'payout_channel': '📢 Payout Channel'
}

DEFAULT_ADMIN_PERMISSIONS = ['dashboard', 'statistics', 'broadcast']

DEFAULT_MILESTONES = [
    {'referrals': 10, 'bonus': 10.00},
    {'referrals': 30, 'bonus': 30.00},
    {'referrals': 50, 'bonus': 50.00},
    {'referrals': 100, 'bonus': 70.00}
]

# ==================== HELPER FUNCTIONS ====================
def get_usd_to_etb_rate() -> float:
    return USD_TO_ETB_RATE

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def get_payout_channel() -> str:
    channel = get_system_setting('payout_channel', DEFAULT_PAYOUT_CHANNEL)
    return channel

def set_payout_channel(channel: str):
    update_system_setting('payout_channel', channel)

def get_next_email_account():
    global current_email_index
    
    rows = Database.fetchall('SELECT email, password FROM email_accounts WHERE is_active = 1')
    
    if not rows:
        return None
    
    accounts = [dict(row) for row in rows]
    
    with email_lock:
        account = accounts[current_email_index % len(accounts)]
        current_email_index = (current_email_index + 1) % len(accounts)
        return account

def generate_secure_otp() -> str:
    return f"{random.randint(100000, 999999)}"

def is_otp_required() -> bool:
    value = get_system_setting('otp_required', 'true')
    return value.lower() == 'true'

def set_otp_required(enabled: bool):
    update_system_setting('otp_required', 'true' if enabled else 'false')

def send_otp_email(recipient_email: str, otp: str, task_name: str = "", is_admin_generated: bool = False) -> bool:
    max_accounts = Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0] or 0
    
    if max_accounts == 0:
        print("❌ No email accounts configured!")
        return False
    
    if not recipient_email or '@' not in recipient_email:
        print(f"❌ Invalid recipient email: {recipient_email}")
        return False
    
    failed_accounts = []
    
    for attempt in range(max_accounts * 2):
        try:
            account = get_next_email_account()
            if not account or account['email'] in failed_accounts:
                continue
            
            print(f"📧 Attempting to send OTP via {account['email']}")
            
            msg = MIMEMultipart()
            msg['From'] = account['email']
            msg['To'] = recipient_email
            
            if is_admin_generated:
                msg['Subject'] = f"🔐 Admin Generated OTP - {BOT_NAME}"
            else:
                msg['Subject'] = f"Task Completion Verification - {BOT_NAME}"
            
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="max-width: 500px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 10px;">
                    <h2 style="color: #4CAF50;">🔐 {'Admin Generated ' if is_admin_generated else ''}Verification Code</h2>
                    <p>Hello,</p>
                    {f'<p>You have a task for <b>{BOT_NAME}</b>.</p>' if not is_admin_generated else '<p>An admin has generated a verification code for you.</p>'}
                    {f'<p><b>Task Name:</b> {task_name}</p>' if task_name else ''}
                    <p>Your verification code is:</p>
                    <div style="background-color: #f0f0f0; padding: 15px; text-align: center; font-size: 32px; font-weight: bold; letter-spacing: 5px;">
                        {otp}
                    </div>
                    <p>This code will expire in <b>5 minutes</b>.</p>
                    <p>Enter this code in the bot to {'complete your task' if not is_admin_generated else 'verify your account'}.</p>
                    <p><b>⚠️ This code can only be used once!</b></p>
                    <hr>
                    <p style="color: #888; font-size: 12px;">{BOT_NAME} Bot</p>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html'))
            
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, 465, context=context, timeout=30) as server:
                server.login(account['email'], account['password'])
                server.send_message(msg)
            
            print(f"✅ OTP sent via {account['email']} to {recipient_email}")
            return True
            
        except smtplib.SMTPAuthenticationError:
            print(f"❌ Auth failed for {account['email']}")
            failed_accounts.append(account['email'])
            Database.execute('UPDATE email_accounts SET is_active = 0 WHERE email = ?', (account['email'],))
            continue
        except socket.timeout:
            print(f"❌ Timeout with {account['email']}")
            failed_accounts.append(account['email'])
            continue
        except Exception as e:
            print(f"❌ Error with {account['email']}: {e}")
            failed_accounts.append(account['email'])
            continue
    
    print(f"❌ All email accounts failed")
    return False

def store_task_otp(user_id: int, task_id: int, email: str, otp: str):
    now = time.time()
    otp_storage[user_id] = {
        "otp": otp,
        "task_id": task_id,
        "email": email,
        "expires": now + 300,
        "attempts": 0,
        "resend_count": 0,
        "max_resend": 3,
        "used": False,
        "created_by_admin": False
    }

def can_resend_otp(user_id: int) -> Tuple[bool, int, str]:
    if user_id not in otp_storage:
        return False, 0, "No active OTP request found."
    
    data = otp_storage[user_id]
    
    if data.get("used", False):
        return False, 0, "This OTP has already been used."
    
    remaining = data["max_resend"] - data["resend_count"]
    
    if data["resend_count"] >= data["max_resend"]:
        return False, 0, f"❌ Maximum resend limit ({data['max_resend']}) reached."
    
    return True, remaining, f"📨 You have {remaining} resend(s) remaining."

def resend_otp(user_id: int) -> Tuple[bool, str]:
    if user_id not in otp_storage:
        return False, "No active OTP request found."
    
    data = otp_storage[user_id]
    
    if data.get("used", False):
        return False, "This OTP has already been used."
    
    if data["resend_count"] >= data["max_resend"]:
        return False, f"❌ Maximum resend limit ({data['max_resend']}) reached."
    
    new_otp = generate_secure_otp()
    
    data["otp"] = new_otp
    data["expires"] = time.time() + 300
    data["resend_count"] += 1
    data["attempts"] = 0
    data["used"] = False
    
    task = Database.fetchone('SELECT name FROM tasks WHERE task_id = ?', (data["task_id"],))
    task_name = task['name'] if task else ""
    
    if send_otp_email(data["email"], new_otp, task_name):
        remaining = data["max_resend"] - data["resend_count"]
        return True, f"✅ New code sent! {remaining} resend(s) remaining."
    else:
        return False, "❌ Failed to send email. Please try again."

def verify_task_otp(user_id: int, entered_otp: str) -> Tuple[bool, int, str]:
    if user_id not in otp_storage:
        return False, None, "❌ No verification code requested."
    
    data = otp_storage[user_id]
    
    if data.get("used", False):
        del otp_storage[user_id]
        return False, None, "❌ This code has already been used!"
    
    if time.time() > data["expires"]:
        del otp_storage[user_id]
        return False, None, "❌ Code expired. Please request a new one."
    
    if data["attempts"] >= 3:
        task_id = data["task_id"]
        del otp_storage[user_id]
        return False, task_id, "❌ Too many failed attempts. Task cancelled."
    
    data["attempts"] += 1
    
    if data["otp"] == entered_otp:
        data["used"] = True
        task_id = data["task_id"]
        del otp_storage[user_id]
        return True, task_id, "✅ Code verified! Task submitted for review."
    
    remaining = 3 - data["attempts"]
    return False, None, f"❌ Invalid code. {remaining} attempt(s) remaining."

def cancel_otp_task(user_id: int) -> Tuple[bool, int, str]:
    if user_id not in otp_storage:
        return False, None, "No active OTP request found."
    
    data = otp_storage[user_id]
    task_id = data["task_id"]
    del otp_storage[user_id]
    return True, task_id, "❌ Task cancelled."

def cancel_otp_task_in_db(task_id: int, user_id: int) -> bool:
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM tasks 
    WHERE task_id = ? AND assigned_to = ? AND status = 'pending_otp'
    ''', (task_id, user_id))
    
    if not cursor.fetchone():
        return False
    
    cursor.execute('''
    UPDATE tasks 
    SET status = 'available', assigned_to = NULL, assigned_time = NULL, expiry_time = NULL 
    WHERE task_id = ?
    ''', (task_id,))
    
    cursor.execute('''
    UPDATE users 
    SET active_tasks_count = active_tasks_count - 1 
    WHERE user_id = ? AND active_tasks_count > 0
    ''', (user_id,))
    
    conn.commit()
    return True

def generate_task_id() -> str:
    while True:
        random_part = random.randint(0, 99999)
        task_id = f"18{random_part:05d}"
        
        existing = Database.fetchone(
            'SELECT task_id FROM tasks WHERE unique_task_id = ?',
            (task_id,)
        )
        if not existing:
            return task_id

async def post_payout_to_channel(context: ContextTypes.DEFAULT_TYPE, user_id: int, amount: float, method: str, details: str, admin_name: str = "", image_path: str = None):
    channel = get_payout_channel()
    
    if not channel:
        print("No payout channel configured")
        return
    
    user = get_user(user_id)
    if not user:
        return
    
    if method == 'telebirr':
        method_icon = "📱"
        method_name = "Telebirr"
    elif method == 'binance':
        method_icon = "🪙"
        method_name = "Binance"
    else:
        method_icon = "🏦"
        method_name = "CBE"
    
    message = (
        f"<b>✅ PAYOUT APPROVED!</b>\n\n"
        f"<b>👤 User:</b> {user['first_name']} (@{user['username'] or 'N/A'})\n"
        f"<b>🆔 User ID:</b> <code>{user_id}</code>\n"
        f"<b>💰 Amount:</b> <code>ETB {amount:,.2f}</code>\n"
        f"<b>{method_icon} Method:</b> {method_name}\n"
        f"<b>📝 Details:</b> <code>{details}</code>\n"
        f"<b>👑 Processed by:</b> {admin_name}\n"
        f"<b>⏰ Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"<i>🎉 Congratulations! Your payout has been sent.</i>"
    )
    
    try:
        if image_path and os.path.exists(image_path):
            with open(image_path, 'rb') as photo:
                await context.bot.send_photo(
                    chat_id=channel,
                    photo=photo,
                    caption=message,
                    parse_mode=ParseMode.HTML
                )
            os.remove(image_path)
        else:
            await context.bot.send_message(
                chat_id=channel,
                text=message,
                parse_mode=ParseMode.HTML
            )
        print(f"✅ Payout post sent to {channel}")
    except Exception as e:
        print(f"Error posting to channel: {e}")

async def queue_message_for_deletion(chat_id: int, message_id: int):
    message_deletion_queue.append((chat_id, message_id, time.time()))

async def delete_old_messages(context: ContextTypes.DEFAULT_TYPE):
    now = time.time()
    to_delete = []
    
    for item in message_deletion_queue:
        if now - item[2] > MESSAGE_EXPIRY_HOURS * 3600:
            to_delete.append(item)
    
    for chat_id, message_id, _ in to_delete:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            message_deletion_queue.remove((chat_id, message_id, _))
        except Exception as e:
            print(f"Error deleting message: {e}")

# ==================== DATABASE SETUP ====================
class Database:
    _connections = {}
    
    @staticmethod
    def get_connection():
        thread_id = threading.get_ident()
        if thread_id not in Database._connections:
            Database._connections[thread_id] = sqlite3.connect(DB_FILE, check_same_thread=False)
            Database._connections[thread_id].row_factory = sqlite3.Row
        return Database._connections[thread_id]
    
    @staticmethod
    def execute(query, params=()):
        conn = Database.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return cursor
    
    @staticmethod
    def fetchone(query, params=()):
        cursor = Database.execute(query, params)
        return cursor.fetchone()
    
    @staticmethod
    def fetchall(query, params=()):
        cursor = Database.execute(query, params)
        return cursor.fetchall()

def init_database():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        balance REAL DEFAULT 0,
        hold_balance REAL DEFAULT 0,
        tasks_completed INTEGER DEFAULT 0,
        referral_id TEXT UNIQUE,
        referred_by INTEGER,
        referral_count INTEGER DEFAULT 0,
        total_earned REAL DEFAULT 0,
        registered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_welcome_shown TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_admin BOOLEAN DEFAULT 0,
        telebirr_name TEXT,
        telebirr_phone TEXT,
        binance_id TEXT,
        cbe_name TEXT,
        cbe_account TEXT,
        default_payment_method TEXT DEFAULT 'telebirr',
        active_tasks_count INTEGER DEFAULT 0,
        referral_links_used TEXT DEFAULT '',
        channels_joined TEXT DEFAULT '[]',
        mandatory_channels TEXT DEFAULT '[]',
        referred_users TEXT DEFAULT '',
        referred_earnings REAL DEFAULT 0
    )
    ''')
    
    cursor.execute("PRAGMA table_info(users)")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    if 'admin_permissions' not in existing_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN admin_permissions TEXT DEFAULT 'dashboard,statistics,broadcast'")
    
    if 'referred_users' not in existing_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN referred_users TEXT DEFAULT ''")
    
    if 'referred_earnings' not in existing_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN referred_earnings REAL DEFAULT 0")
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tasks (
        task_id INTEGER PRIMARY KEY AUTOINCREMENT,
        unique_task_id TEXT UNIQUE,
        name TEXT NOT NULL,
        father_name TEXT,
        address TEXT NOT NULL,
        password TEXT NOT NULL,
        reward REAL DEFAULT 0.25,
        status TEXT DEFAULT 'available',
        assigned_to INTEGER,
        assigned_time TIMESTAMP,
        completed_time TIMESTAMP,
        expiry_time TIMESTAMP,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(address)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        trans_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        trans_type TEXT,
        details TEXT,
        task_id INTEGER,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (task_id) REFERENCES tasks(task_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payouts (
        payout_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        status TEXT DEFAULT 'pending',
        payout_method TEXT,
        payout_details TEXT,
        new_payout_details TEXT,
        request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed_time TIMESTAMP,
        processed_by INTEGER,
        image_path TEXT,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (processed_by) REFERENCES users(user_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bonus_settings (
        setting_id INTEGER PRIMARY KEY AUTOINCREMENT,
        min_withdrawal REAL DEFAULT 20.00,
        referral_bonus REAL DEFAULT 2.00,
        referral_percentage REAL DEFAULT 5.00,
        task_reward REAL DEFAULT 0.25,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS system_settings (
        setting_key TEXT PRIMARY KEY,
        setting_value TEXT,
        description TEXT,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payment_methods (
        method_id INTEGER PRIMARY KEY AUTOINCREMENT,
        method_name TEXT UNIQUE,
        is_active BOOLEAN DEFAULT 1,
        min_amount REAL DEFAULT 0,
        max_amount REAL DEFAULT 1000,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channels (
        channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_username TEXT UNIQUE,
        channel_name TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        added_by INTEGER,
        FOREIGN KEY (added_by) REFERENCES users(user_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_channels (
        user_id INTEGER,
        channel_username TEXT,
        joined BOOLEAN DEFAULT 0,
        verified_date TIMESTAMP,
        PRIMARY KEY (user_id, channel_username),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS email_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE,
        password TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        added_by INTEGER,
        FOREIGN KEY (added_by) REFERENCES users(user_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS milestone_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        is_enabled BOOLEAN DEFAULT 1,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by INTEGER,
        FOREIGN KEY (updated_by) REFERENCES users(user_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS milestones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrals INTEGER NOT NULL,
        bonus REAL NOT NULL,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(referrals)
    )
    ''')
    
    cursor.execute('SELECT COUNT(*) FROM bonus_settings')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
        INSERT INTO bonus_settings (min_withdrawal, referral_bonus, referral_percentage, task_reward) 
        VALUES (20.00, 2.00, 5.00, 0.25)
        ''')
    
    default_methods = ['telebirr', 'binance', 'cbe']
    for method in default_methods:
        cursor.execute('''
        INSERT OR IGNORE INTO payment_methods (method_name) 
        VALUES (?)
        ''', (method,))
    
    default_settings = [
        ('bot_name', BOT_NAME, 'Bot display name'),
        ('welcome_message', f'<b>🎉 WELCOME TO {BOT_NAME.upper()}! 🎉</b>\n\n🌟 <b>Your earning journey starts here!</b> 🌟\n\n💰 <b>Earn money by completing simple tasks</b>\n👥 <b>Invite friends & earn bonuses</b>\n⚡ <b>Quick & easy withdrawals</b>\n\n✨ <b>Let\'s get started and grow together!</b> ✨', 'Welcome message'),
        ('welcome_back_message', '<b>✨ WELCOME BACK! ✨</b>\n\nWe\'re thrilled to see you again! 🌟\n\nReady to earn more? Let\'s continue your journey! 🚀', 'Welcome back message'),
        ('task_approval_message', '✅ Task approved! ETB{reward} added to your balance.', 'Default task approval message'),
        ('task_rejection_message', '❌ Task rejected. Please check requirements and try again.', 'Default task rejection message'),
        ('payout_approved_message', '✅ Payout approved! ETB{amount} sent to your account.', 'Default payout approved message'),
        ('payout_rejected_message', '❌ Payout rejected. Funds returned to balance.', 'Default payout rejected message'),
        ('task_expiry_hours', '24', 'Task expiry time in hours'),
        ('price_update_message', '🎉 Price updated! Task reward is now ETB{reward:.2f}', 'Price update notification'),
        ('referral_update_message', '🎉 Referral bonus updated! Now ETB{bonus:.2f}', 'Referral update notification'),
        ('withdrawal_update_message', '🎉 Withdrawal minimum updated! Now ETB{min:.2f}', 'Withdrawal update notification'),
        ('mandatory_channels_message', '<b>⚠️ CHANNEL JOIN REQUIRED</b>\n\nTo use this bot, you must join our channels:', 'Mandatory channels message'),
        ('new_channel_added_message', '📢 New channel added: {channel}', 'New channel notification'),
        ('contact_admin', OWNER_USERNAME, 'Contact admin username'),
        ('otp_required', 'true', 'Require OTP verification for tasks (true/false)'),
        ('payout_channel', DEFAULT_PAYOUT_CHANNEL, 'Channel for payout proof posts'),
    ]
    
    for key, value, desc in default_settings:
        cursor.execute('''
        INSERT OR IGNORE INTO system_settings (setting_key, setting_value, description) 
        VALUES (?, ?, ?)
        ''', (key, value, desc))
    
    cursor.execute('SELECT COUNT(*) FROM channels WHERE channel_username = ?', ('@ethiofarmernews',))
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
        INSERT INTO channels (channel_username, channel_name, added_by, is_active)
        VALUES (?, ?, ?, 1)
        ''', ('@ethiofarmernews', 'Ethio Farmer News', OWNER_ID))
    
    cursor.execute('SELECT COUNT(*) FROM milestone_settings')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
        INSERT INTO milestone_settings (is_enabled, updated_by)
        VALUES (1, ?)
        ''', (OWNER_ID,))
    
    for milestone in DEFAULT_MILESTONES:
        cursor.execute('''
        INSERT OR IGNORE INTO milestones (referrals, bonus)
        VALUES (?, ?)
        ''', (milestone['referrals'], milestone['bonus']))
    
    default_email_accounts = [
        {"email": "creolacole3231@gmail.com", "password": "vvewsxznbcfhajue"},
        {"email": "jwilliams7s1fa@gmail.com", "password": "nzpvdrogsyxgeopc"},
        {"email": "Karmaethio@gmail.com", "password": "xvvojpfretylfhj"},
    ]
    
    for account in default_email_accounts:
        cursor.execute('''
        INSERT OR IGNORE INTO email_accounts (email, password, is_active, added_by)
        VALUES (?, ?, 1, ?)
        ''', (account['email'], account['password'], OWNER_ID))
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_to)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_unique_id ON tasks(unique_task_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_payouts_status ON payouts(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_referral ON users(referral_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_active ON users(last_active)')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_address ON tasks(address)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_channels_active ON channels(is_active)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_channels_joined ON user_channels(user_id, joined)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_email_accounts_active ON email_accounts(is_active)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_milestones_referrals ON milestones(referrals)')
    
    conn.commit()
    conn.close()
    
    start_background_tasks()
    update_all_users_mandatory_channels()

# ==================== EXPORT TASKS FUNCTIONS ====================
def export_completed_tasks_to_csv() -> Tuple[BytesIO, int]:
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT name, father_name, address, password
    FROM tasks
    WHERE status = 'approved'
    ORDER BY completed_time DESC
    ''')
    
    rows = cursor.fetchall()
    
    output = BytesIO()
    writer = csv.writer(output)
    
    writer.writerow(['Name', 'Father Name', 'Email', 'Password'])
    
    for row in rows:
        task_dict = dict(row)
        writer.writerow([
            task_dict['name'],
            task_dict['father_name'] or '',
            f"{task_dict['address']}@gmail.com",
            task_dict['password']
        ])
    
    output.seek(0)
    return output, len(rows)

def export_failed_tasks_to_csv() -> Tuple[BytesIO, int]:
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT name, father_name, address, password
    FROM tasks
    WHERE status = 'rejected'
    ORDER BY completed_time DESC
    ''')
    
    rows = cursor.fetchall()
    
    output = BytesIO()
    writer = csv.writer(output)
    
    writer.writerow(['Name', 'Father Name', 'Email', 'Password'])
    
    for row in rows:
        task_dict = dict(row)
        writer.writerow([
            task_dict['name'],
            task_dict['father_name'] or '',
            f"{task_dict['address']}@gmail.com",
            task_dict['password']
        ])
    
    output.seek(0)
    return output, len(rows)

# ==================== DELETE TASKS FUNCTIONS ====================
def delete_single_completed_task(task_id: int, admin_id: int) -> Tuple[bool, str]:
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    task = Database.fetchone('SELECT * FROM tasks WHERE unique_task_id = ? AND status = "approved"', (task_id,))
    if not task:
        return False, "❌ Task not found or not approved!"
    
    cursor.execute('DELETE FROM tasks WHERE unique_task_id = ?', (task_id,))
    conn.commit()
    
    return True, f"✅ Task {task_id} deleted successfully!"

def delete_all_completed_tasks(admin_id: int) -> Tuple[int, str]:
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM tasks WHERE status = "approved"')
    count = cursor.fetchone()[0]
    
    if count == 0:
        return 0, "📭 No completed tasks to delete!"
    
    cursor.execute('DELETE FROM tasks WHERE status = "approved"')
    conn.commit()
    
    return count, f"✅ Deleted {count} completed tasks successfully!"

def delete_all_failed_tasks(admin_id: int) -> Tuple[int, str]:
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM tasks WHERE status = "rejected"')
    count = cursor.fetchone()[0]
    
    if count == 0:
        return 0, "📭 No failed tasks to delete!"
    
    cursor.execute('DELETE FROM tasks WHERE status = "rejected"')
    conn.commit()
    
    return count, f"✅ Deleted {count} failed tasks successfully!"

# ==================== PENDING APPROVAL TASKS FUNCTIONS ====================
def get_all_pending_approval_tasks(page: int = 1, per_page: int = 20):
    offset = (page - 1) * per_page
    
    rows = Database.fetchall('''
    SELECT t.task_id, t.unique_task_id, t.name, t.father_name, t.address, 
           t.password, t.reward, t.completed_time, t.assigned_to,
           u.username, u.first_name
    FROM tasks t
    LEFT JOIN users u ON t.assigned_to = u.user_id
    WHERE t.status = 'pending'
    ORDER BY t.completed_time DESC
    LIMIT ? OFFSET ?
    ''', (per_page, offset))
    
    total_rows = Database.fetchone('SELECT COUNT(*) as count FROM tasks WHERE status = "pending"')
    total = total_rows[0] if total_rows else 0
    total_pages = (total + per_page - 1) // per_page
    
    return [dict(row) for row in rows], page, total_pages, total

# ==================== EMAIL ACCOUNT MANAGEMENT ====================
def get_email_accounts() -> List[Dict]:
    rows = Database.fetchall('SELECT * FROM email_accounts ORDER BY created_date DESC')
    return [dict(row) for row in rows] if rows else []

def add_email_account(email: str, password: str, admin_id: int) -> Tuple[bool, str]:
    if not email or not password:
        return False, "Email and password are required!"
    
    if not email.endswith('@gmail.com'):
        return False, "Only Gmail accounts are supported!"
    
    existing = Database.fetchone('SELECT id FROM email_accounts WHERE email = ?', (email,))
    if existing:
        return False, "Email account already exists!"
    
    Database.execute('''
    INSERT INTO email_accounts (email, password, is_active, added_by)
    VALUES (?, ?, 1, ?)
    ''', (email, password, admin_id))
    
    return True, f"✅ Email account {email} added successfully!"

def remove_email_account(email: str, requester_id: int) -> Tuple[bool, str]:
    if not is_owner(requester_id):
        return False, "❌ Only the owner can remove email accounts!"
    
    active_count = Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0]
    if active_count <= 1:
        return False, "❌ Cannot remove the last active email account!"
    
    Database.execute('DELETE FROM email_accounts WHERE email = ?', (email,))
    return True, f"✅ Email account {email} removed successfully!"

def toggle_email_account(email: str, is_active: bool, requester_id: int) -> Tuple[bool, str]:
    if not is_owner(requester_id):
        return False, "❌ Only the owner can toggle email accounts!"
    
    if not is_active:
        active_count = Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0]
        if active_count <= 1:
            return False, "❌ Cannot disable the last active email account!"
    
    Database.execute('UPDATE email_accounts SET is_active = ? WHERE email = ?', (1 if is_active else 0, email))
    status = "activated" if is_active else "deactivated"
    return True, f"✅ Email account {email} {status}!"

# ==================== MILESTONE MANAGEMENT ====================
def get_milestone_settings() -> Dict:
    row = Database.fetchone('SELECT * FROM milestone_settings ORDER BY id DESC LIMIT 1')
    return {'is_enabled': row['is_enabled'] if row else 1}

def update_milestone_settings(is_enabled: bool, admin_id: int):
    Database.execute('''
    INSERT INTO milestone_settings (is_enabled, updated_by, updated_date)
    VALUES (?, ?, CURRENT_TIMESTAMP)
    ''', (1 if is_enabled else 0, admin_id))

def get_milestones() -> List[Dict]:
    rows = Database.fetchall('SELECT * FROM milestones ORDER BY referrals ASC')
    return [dict(row) for row in rows] if rows else DEFAULT_MILESTONES.copy()

def update_milestone(referrals: int, bonus: float, admin_id: int) -> Tuple[bool, str]:
    existing = Database.fetchone('SELECT id FROM milestones WHERE referrals = ?', (referrals,))
    if existing:
        Database.execute('''
        UPDATE milestones SET bonus = ? WHERE referrals = ?
        ''', (bonus, referrals))
        return True, f"✅ Milestone for {referrals} referrals updated to ETB{bonus:.2f}"
    else:
        Database.execute('''
        INSERT INTO milestones (referrals, bonus) VALUES (?, ?)
        ''', (referrals, bonus))
        return True, f"✅ New milestone added: {referrals} referrals = ETB{bonus:.2f}"

def delete_milestone(referrals: int, admin_id: int) -> Tuple[bool, str]:
    existing = Database.fetchone('SELECT id FROM milestones WHERE referrals = ?', (referrals,))
    if not existing:
        return False, f"❌ Milestone for {referrals} referrals not found!"
    
    Database.execute('DELETE FROM milestones WHERE referrals = ?', (referrals,))
    return True, f"✅ Milestone for {referrals} referrals removed!"

async def broadcast_milestone_update(context, is_enabled: bool):
    if is_enabled:
        milestones = get_milestones()
        if milestones:
            message = "<b>🎯 REFERRAL MILESTONE BONUSES ENABLED!</b>\n\n"
            message += "You can now earn extra bonuses when you reach referral milestones:\n\n"
            for m in milestones:
                message += f"• {m['referrals']} referrals → ETB{m['bonus']:.2f}\n"
            message += "\nInvite more friends and earn extra rewards! 🚀"
            await broadcast_message(context, message)

# ==================== DATABASE FUNCTIONS ====================
def get_user(user_id: int):
    row = Database.fetchone('SELECT * FROM users WHERE user_id = ?', (user_id,))
    return dict(row) if row else None

def create_user(user_id: int, username: str, first_name: str, last_name: str = "", referral_code: str = None):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    existing_user = Database.fetchone('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
    if existing_user:
        update_user_activity(user_id)
        return False, "User already exists"
    
    if referral_code:
        try:
            referrer_id = int(referral_code)
            if referrer_id == user_id:
                return False, "You cannot refer yourself!"
            
            ref_user = Database.fetchone('SELECT user_id, referred_users FROM users WHERE user_id = ?', (referrer_id,))
            if not ref_user:
                referral_code = None
        except ValueError:
            referral_code = None
    
    referral_id = hashlib.md5(f"{user_id}{time.time()}".encode()).hexdigest()[:8]
    
    referred_by = None
    referral_applied = False
    referral_bonus_given = False
    
    if referral_code:
        try:
            referrer_id = int(referral_code)
            ref_user = Database.fetchone('SELECT user_id, referred_users FROM users WHERE user_id = ?', (referrer_id,))
            if ref_user:
                referrer_id_val = ref_user[0]
                referred_by = referrer_id_val
                referral_applied = True
                
                existing_user_check = Database.fetchone('SELECT referred_by FROM users WHERE user_id = ?', (user_id,))
                if existing_user_check and existing_user_check[0] is not None:
                    referral_bonus_given = False
                else:
                    referred_users_str = ref_user[1] or ''
                    referred_users_list = referred_users_str.split(',') if referred_users_str else []
                    
                    if str(user_id) not in referred_users_list:
                        settings = get_bonus_settings()
                        cursor.execute('''
                        UPDATE users SET balance = balance + ?, referral_count = referral_count + 1, referred_users = ? 
                        WHERE user_id = ?
                        ''', (settings['referral_bonus'], 
                              (referred_users_str + ',' + str(user_id) if referred_users_str else str(user_id)),
                              referrer_id_val))
                        
                        cursor.execute('''
                        INSERT INTO transactions (user_id, amount, trans_type, details) 
                        VALUES (?, ?, ?, ?)
                        ''', (referrer_id_val, settings['referral_bonus'], 'referral', f'Referral from user {user_id}'))
                        
                        referral_bonus_given = True
                    else:
                        referral_bonus_given = False
        except ValueError:
            pass
    
    mandatory_channels = get_mandatory_channels()
    
    cursor.execute('''
    INSERT INTO users 
    (user_id, username, first_name, last_name, referral_id, referred_by, referral_links_used, mandatory_channels) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, username, first_name, last_name, referral_id, referred_by, 
          referral_code if referral_applied else '', json.dumps(mandatory_channels)))
    
    conn.commit()
    update_user_activity(user_id)
    
    if referral_bonus_given:
        return True, "User created successfully with referral bonus applied"
    elif referral_applied:
        return True, "User created successfully (already referred before, no bonus)"
    else:
        return True, "User created successfully"

def update_user_activity(user_id: int):
    Database.execute(
        'UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
        (user_id,)
    )

def update_user_welcome_shown(user_id: int):
    Database.execute(
        'UPDATE users SET last_welcome_shown = CURRENT_TIMESTAMP WHERE user_id = ?',
        (user_id,)
    )

def should_show_welcome_back(user_id: int) -> bool:
    row = Database.fetchone(
        'SELECT last_welcome_shown FROM users WHERE user_id = ?',
        (user_id,)
    )
    if row:
        last_shown = datetime.fromisoformat(row[0])
        return (datetime.now() - last_shown).total_seconds() > 1800
    return True

def get_online_users_count(hours: int = 1):
    row = Database.fetchone('''
    SELECT COUNT(*) as count FROM users 
    WHERE last_active > datetime('now', ?)
    ''', (f'-{hours} hours',))
    return row[0] if row else 0

def get_bonus_settings():
    row = Database.fetchone('SELECT * FROM bonus_settings ORDER BY setting_id DESC LIMIT 1')
    return dict(row) if row else {
        'min_withdrawal': 20.00,
        'referral_bonus': 2.00,
        'referral_percentage': 5.00,
        'task_reward': 0.25
    }

def update_bonus_settings(min_withdrawal: float, referral_bonus: float, referral_percentage: float, task_reward: float):
    old_settings = get_bonus_settings()
    
    Database.execute('''
    INSERT INTO bonus_settings (min_withdrawal, referral_bonus, referral_percentage, task_reward)
    VALUES (?, ?, ?, ?)
    ''', (min_withdrawal, referral_bonus, referral_percentage, task_reward))
    
    if old_settings['task_reward'] != task_reward:
        Database.execute('''
        UPDATE tasks SET reward = ? WHERE status IN ('available', 'assigned')
        ''', (task_reward,))
    
    return old_settings

def get_system_setting(key: str, default: str = ""):
    row = Database.fetchone('SELECT setting_value FROM system_settings WHERE setting_key = ?', (key,))
    return row[0] if row else default

def update_system_setting(key: str, value: str):
    Database.execute('''
    INSERT OR REPLACE INTO system_settings (setting_key, setting_value, updated_date)
    VALUES (?, ?, CURRENT_TIMESTAMP)
    ''', (key, value))

def get_task_statistics():
    rows = Database.fetchall('''
    SELECT 
        SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END) as available_tasks,
        SUM(CASE WHEN status = 'assigned' THEN 1 ELSE 0 END) as assigned_tasks,
        SUM(CASE WHEN status = 'pending_otp' THEN 1 ELSE 0 END) as pending_otp_tasks,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_tasks,
        SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_tasks,
        SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected_tasks,
        SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) as expired_tasks,
        COUNT(*) as total_tasks
    FROM tasks
    ''')
    
    if rows and rows[0]:
        row = rows[0]
        return {
            'available_tasks': row[0] or 0,
            'assigned_tasks': row[1] or 0,
            'pending_otp_tasks': row[2] or 0,
            'pending_tasks': row[3] or 0,
            'approved_tasks': row[4] or 0,
            'rejected_tasks': row[5] or 0,
            'expired_tasks': row[6] or 0,
            'total_tasks': row[7] or 0
        }
    return {
        'available_tasks': 0,
        'assigned_tasks': 0,
        'pending_otp_tasks': 0,
        'pending_tasks': 0,
        'approved_tasks': 0,
        'rejected_tasks': 0,
        'expired_tasks': 0,
        'total_tasks': 0
    }

# ==================== PERMISSION FUNCTIONS ====================
def get_admin_permissions(user_id: int) -> List[str]:
    row = Database.fetchone('SELECT admin_permissions FROM users WHERE user_id = ?', (user_id,))
    if row and row[0]:
        return row[0].split(',')
    return DEFAULT_ADMIN_PERMISSIONS.copy()

def set_admin_permissions(user_id: int, permissions: List[str]):
    permissions_str = ','.join(permissions)
    Database.execute('UPDATE users SET admin_permissions = ? WHERE user_id = ?', (permissions_str, user_id))

def add_admin_with_permissions(user_id: int, requester_id: int, extra_permissions: List[str] = None) -> Tuple[bool, str]:
    if not is_owner(requester_id):
        return False, "❌ Only the bot owner can add admins!"
    
    user = get_user(user_id)
    if not user:
        return False, f"❌ User {user_id} not found in database!"
    
    Database.execute('UPDATE users SET is_admin = 1 WHERE user_id = ?', (user_id,))
    
    all_permissions = DEFAULT_ADMIN_PERMISSIONS.copy()
    if extra_permissions:
        for p in extra_permissions:
            if p in PERMISSIONS and p not in all_permissions:
                all_permissions.append(p)
    
    set_admin_permissions(user_id, all_permissions)
    
    perm_display = [PERMISSIONS[p] for p in all_permissions if p in PERMISSIONS]
    
    return True, f"✅ User {user_id} is now an admin with permissions: {', '.join(perm_display)}"

def remove_admin(user_id: int, requester_id: int) -> Tuple[bool, str]:
    if not is_owner(requester_id):
        return False, "❌ Only the bot owner can remove admins!"
    
    if user_id == OWNER_ID:
        return False, "❌ Cannot remove the owner!"
    
    Database.execute('UPDATE users SET is_admin = 0 WHERE user_id = ?', (user_id,))
    return True, f"✅ User {user_id} is no longer an admin!"

def is_admin(user_id: int):
    user = get_user(user_id)
    return user and user.get('is_admin', 0) == 1

def get_admin_menu_by_permissions(user_id: int):
    permissions = get_admin_permissions(user_id)
    
    available_buttons = []
    
    if 'dashboard' in permissions:
        available_buttons.append("📊 Dashboard")
    if 'add' in permissions:
        available_buttons.append("📦 Bulk Add Tasks")
    if 'pending' in permissions:
        available_buttons.append("⏳ Pending Tasks")  # Has approve/reject
    if 'pending_approval' in permissions:
        available_buttons.append("📋 Pending Approval List")  # VIEW ONLY
    if 'completed' in permissions:
        available_buttons.append("✅ Completed Tasks")
    if 'payout' in permissions:
        available_buttons.append("💰 Pending Payouts")
    if 'manage' in permissions:
        available_buttons.append("👥 Manage Users")
    if 'statistics' in permissions:
        available_buttons.append("📈 Statistics")
    if 'setting' in permissions:
        available_buttons.append("⚙️ Admin Settings")
    if 'broadcast' in permissions:
        available_buttons.append("📢 Broadcast")
    if 'backup' in permissions:
        available_buttons.append("💾 Backup Data")
    if 'channel' in permissions:
        available_buttons.append("📢 Manage Channels")
    if 'taskinfo' in permissions:
        available_buttons.append("🔍 Task Info")
    if 'email' in permissions:
        available_buttons.append("📧 Email Accounts")
    if 'contact' in permissions:
        available_buttons.append("📞 Set Contact Admin")
    if 'referral' in permissions:
        available_buttons.append("🎯 Referral Settings")
    if 'export' in permissions:
        available_buttons.append("📥 Export Tasks")
    if 'delete' in permissions:
        available_buttons.append("🗑️ Delete Tasks")
    if 'otp' in permissions:
        available_buttons.append("🔐 OTP")
    if 'payout_channel' in permissions:
        available_buttons.append("📢 Payout Channel")
    
    buttons = []
    for i in range(0, len(available_buttons), 2):
        row = [available_buttons[i]]
        if i + 1 < len(available_buttons):
            row.append(available_buttons[i + 1])
        buttons.append(row)
    
    buttons.append(["🏠 User Menu"])
    
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# ==================== MANDATORY CHANNEL SYSTEM ====================
def add_channel(channel_username: str, admin_id: int) -> Tuple[bool, str]:
    if not channel_username.startswith('@'):
        return False, "Channel username must start with @"
    
    existing = Database.fetchone(
        'SELECT channel_username FROM channels WHERE channel_username = ?',
        (channel_username,)
    )
    if existing:
        return False, "Channel already exists"
    
    Database.execute('''
    INSERT INTO channels (channel_username, channel_name, added_by, is_active)
    VALUES (?, ?, ?, 1)
    ''', (channel_username, channel_username, admin_id))
    
    update_all_users_mandatory_channels()
    return True, ""

def remove_channel(channel_username: str) -> Tuple[bool, str]:
    Database.execute(
        'DELETE FROM channels WHERE channel_username = ?',
        (channel_username,)
    )
    
    update_all_users_mandatory_channels()
    return True, f"✅ Channel {channel_username} removed successfully!"

def get_mandatory_channels() -> List[str]:
    rows = Database.fetchall(
        'SELECT channel_username FROM channels WHERE is_active = 1'
    )
    return [row[0] for row in rows] if rows else []

def get_all_channels() -> List[Dict]:
    rows = Database.fetchall(
        'SELECT * FROM channels ORDER BY created_date DESC'
    )
    return [dict(row) for row in rows] if rows else []

def update_all_users_mandatory_channels():
    channels = get_mandatory_channels()
    channels_json = json.dumps(channels)
    Database.execute('UPDATE users SET mandatory_channels = ?', (channels_json,))

def mark_channel_joined(user_id: int, channel_username: str):
    Database.execute('''
    INSERT OR REPLACE INTO user_channels (user_id, channel_username, joined, verified_date)
    VALUES (?, ?, 1, CURRENT_TIMESTAMP)
    ''', (user_id, channel_username))

async def check_user_channels(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> Tuple[bool, List[str], List[str]]:
    mandatory_channels = get_mandatory_channels()
    if not mandatory_channels:
        return True, [], []
    
    actually_joined = []
    not_joined = []
    
    for channel in mandatory_channels:
        try:
            chat_member = await context.bot.get_chat_member(
                chat_id=channel,
                user_id=user_id
            )
            
            if chat_member.status not in ['left', 'kicked']:
                actually_joined.append(channel)
                mark_channel_joined(user_id, channel)
            else:
                not_joined.append(channel)
                
        except Exception:
            not_joined.append(channel)
    
    return len(not_joined) == 0, actually_joined, not_joined

def get_channel_join_url(channel_username: str) -> str:
    if channel_username.startswith('@'):
        return f"https://t.me/{channel_username[1:]}"
    return f"https://t.me/{channel_username}"

async def show_channel_requirement(update: Update, context: ContextTypes.DEFAULT_TYPE, missing_channels: List[str]):
    message = update.message or update.callback_query.message
    
    response = (
        f"<b>⚠️ CHANNEL JOIN REQUIRED</b>\n\n"
        f"To use {BOT_NAME} bot, you must join our channels:\n\n"
    )
    
    keyboard = []
    for channel in missing_channels:
        channel_name = channel.replace('@', '')
        response += f"• {channel}\n"
        keyboard.append([
            InlineKeyboardButton(
                f"📢 Join {channel}",
                url=get_channel_join_url(channel)
            )
        ])
    
    response += "\nAfter joining all channels, click the button below to verify."
    
    keyboard.append([
        InlineKeyboardButton("✅ Verify Joins", callback_data="verify_channels")
    ])
    
    await message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ==================== TASK MANAGEMENT ====================
def get_task_expiry_hours():
    try:
        return int(get_system_setting('task_expiry_hours', '24'))
    except:
        return 24

def get_available_task(user_id: int):
    expire_old_tasks()
    
    user = get_user(user_id)
    if user and user.get('active_tasks_count', 0) >= MAX_ACTIVE_TASKS:
        return None
    
    row = Database.fetchone('''
    SELECT * FROM tasks 
    WHERE status = 'available' 
    ORDER BY task_id 
    LIMIT 1
    ''')
    
    if row:
        task = dict(row)
        expiry_hours = get_task_expiry_hours()
        expiry_time = datetime.now() + timedelta(hours=expiry_hours)
        
        conn = Database.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        UPDATE tasks 
        SET status = 'assigned', assigned_to = ?, assigned_time = CURRENT_TIMESTAMP, expiry_time = ?
        WHERE task_id = ?
        ''', (user_id, expiry_time, task['task_id']))
        
        cursor.execute('''
        UPDATE users 
        SET active_tasks_count = active_tasks_count + 1 
        WHERE user_id = ?
        ''', (user_id,))
        
        conn.commit()
        
        task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ?', (task['task_id'],))
        return dict(task) if task else None
    
    return None

def expire_old_tasks():
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT task_id, assigned_to FROM tasks 
    WHERE status = 'assigned' 
    AND expiry_time < CURRENT_TIMESTAMP
    ''')
    
    expired_tasks = cursor.fetchall()
    
    for task in expired_tasks:
        task_id = task[0]
        user_id = task[1]
        
        cursor.execute('UPDATE tasks SET status = "expired" WHERE task_id = ?', (task_id,))
        
        if user_id:
            cursor.execute('''
            UPDATE users 
            SET active_tasks_count = active_tasks_count - 1 
            WHERE user_id = ? AND active_tasks_count > 0
            ''', (user_id,))
    
    conn.commit()

def delete_expired_tasks():
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    DELETE FROM tasks 
    WHERE status = 'expired' 
    AND expiry_time < datetime('now', '-24 hours')
    ''')
    
    deleted_count = cursor.rowcount
    conn.commit()
    
    if deleted_count > 0:
        print(f"Deleted {deleted_count} expired tasks older than 24 hours")
    
    return deleted_count

def mark_task_for_completion(task_id: int, user_id: int):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM tasks 
    WHERE task_id = ? AND assigned_to = ? AND status = 'assigned'
    ''', (task_id, user_id))
    
    task = cursor.fetchone()
    if not task:
        return False, None, None
    
    task_dict = dict(task)
    
    if is_otp_required():
        email = f"{task_dict['address']}@gmail.com"
        
        cursor.execute('''
        UPDATE tasks 
        SET status = 'pending_otp', completed_time = CURRENT_TIMESTAMP 
        WHERE task_id = ?
        ''', (task_id,))
        
        conn.commit()
        return True, 'otp_required', email
    else:
        reward = task_dict['reward']
        
        cursor.execute('''
        UPDATE tasks 
        SET status = 'pending', completed_time = CURRENT_TIMESTAMP 
        WHERE task_id = ?
        ''', (task_id,))
        
        cursor.execute('''
        UPDATE users 
        SET active_tasks_count = active_tasks_count - 1 
        WHERE user_id = ? AND active_tasks_count > 0
        ''', (user_id,))
        
        cursor.execute('''
        UPDATE users 
        SET hold_balance = hold_balance + ? 
        WHERE user_id = ?
        ''', (reward, user_id))
        
        cursor.execute('''
        INSERT INTO transactions (user_id, amount, trans_type, task_id, details)
        VALUES (?, ?, ?, ?, ?)
        ''', (user_id, reward, 'task_pending', task_id, f'Task #{task_id} pending approval'))
        
        conn.commit()
        return True, 'direct_submit', None

def submit_task_after_otp(task_id: int, user_id: int):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM tasks 
    WHERE task_id = ? AND assigned_to = ? AND status = 'pending_otp'
    ''', (task_id, user_id))
    
    task = cursor.fetchone()
    if not task:
        return False
    
    task_dict = dict(task)
    reward = task_dict['reward']
    
    cursor.execute('''
    UPDATE tasks 
    SET status = 'pending' 
    WHERE task_id = ?
    ''', (task_id,))
    
    cursor.execute('''
    UPDATE users 
    SET active_tasks_count = active_tasks_count - 1 
    WHERE user_id = ? AND active_tasks_count > 0
    ''', (user_id,))
    
    cursor.execute('''
    UPDATE users 
    SET hold_balance = hold_balance + ? 
    WHERE user_id = ?
    ''', (reward, user_id))
    
    cursor.execute('''
    INSERT INTO transactions (user_id, amount, trans_type, task_id, details)
    VALUES (?, ?, ?, ?, ?)
    ''', (user_id, reward, 'task_pending', task_id, f'Task #{task_id} pending approval'))
    
    conn.commit()
    return True

def cancel_task_assignment(task_id: int, user_id: int, return_to_available: bool = True):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM tasks 
    WHERE task_id = ? AND assigned_to = ?
    ''', (task_id, user_id))
    
    task = cursor.fetchone()
    if not task:
        return False
    
    if return_to_available:
        cursor.execute('''
        UPDATE tasks 
        SET status = 'available', assigned_to = NULL, assigned_time = NULL, expiry_time = NULL 
        WHERE task_id = ?
        ''', (task_id,))
    else:
        cursor.execute('''
        UPDATE tasks 
        SET status = 'expired', assigned_to = NULL, assigned_time = NULL, expiry_time = NULL 
        WHERE task_id = ?
        ''', (task_id,))
    
    cursor.execute('''
    UPDATE users 
    SET active_tasks_count = active_tasks_count - 1 
    WHERE user_id = ? AND active_tasks_count > 0
    ''', (user_id,))
    
    conn.commit()
    return True

def get_user_active_tasks(user_id: int):
    rows = Database.fetchall('''
    SELECT * FROM tasks 
    WHERE assigned_to = ? AND status IN ('assigned', 'pending_otp')
    ORDER BY assigned_time DESC
    ''', (user_id,))
    
    return [dict(row) for row in rows]

def get_user_completed_tasks(user_id: int):
    rows = Database.fetchall('''
    SELECT task_id, name, address, reward, status, completed_time
    FROM tasks 
    WHERE assigned_to = ? AND status IN ('approved', 'pending', 'rejected')
    ORDER BY completed_time DESC
    ''', (user_id,))
    
    return [dict(row) for row in rows]

def create_task(name: str, father_name: str, address: str, password: str):
    existing_task = Database.fetchone('SELECT task_id FROM tasks WHERE address = ?', (address,))
    if existing_task:
        return None, None, f"Email {address}@gmail.com already exists!"
    
    if not name or not name.strip():
        return None, None, "❌ Name cannot be empty!"
    
    if not address or not address.strip():
        return None, None, "❌ Email address cannot be empty!"
    
    if not password or not password.strip():
        return None, None, "❌ Password cannot be empty!"
    
    address = address.strip().lower()
    if address.endswith('@gmail.com'):
        address = address.replace('@gmail.com', '')
    
    if not re.match(r'^[a-z0-9._-]+$', address):
        return None, None, "❌ Email address can only contain letters, numbers, dots, underscores, and hyphens!"
    
    settings = get_bonus_settings()
    reward = settings['task_reward']
    unique_id = generate_task_id()
    
    try:
        cursor = Database.execute('''
        INSERT INTO tasks (unique_task_id, name, father_name, address, password, reward, status)
        VALUES (?, ?, ?, ?, ?, ?, 'available')
        ''', (unique_id, name.strip(), father_name.strip() if father_name else '', address, password.strip(), reward))
        
        return cursor.lastrowid, unique_id, f"✅ Task created successfully with ID: {unique_id}"
    except sqlite3.IntegrityError as e:
        return None, None, f"❌ Error creating task: {str(e)}"

def create_bulk_tasks(tasks_data: List[Dict]) -> Tuple[int, List[str]]:
    settings = get_bonus_settings()
    reward = settings['task_reward']
    created = 0
    errors = []
    
    for idx, task_data in enumerate(tasks_data):
        try:
            name = task_data.get('name', '').strip()
            father_name = task_data.get('father_name', '').strip() if task_data.get('father_name') else ''
            address = task_data.get('address', '').strip()
            password = task_data.get('password', '').strip()
            
            if not name:
                errors.append(f"Task {idx+1}: Name is required")
                continue
            
            if not address:
                errors.append(f"Task {idx+1}: Email address is required")
                continue
            
            if not password:
                errors.append(f"Task {idx+1}: Password is required")
                continue
            
            if address.endswith('@gmail.com'):
                address = address.replace('@gmail.com', '')
            
            if not re.match(r'^[a-z0-9._-]+$', address):
                errors.append(f"Task {idx+1}: Invalid email format: {address}")
                continue
            
            existing = Database.fetchone('SELECT task_id FROM tasks WHERE address = ?', (address,))
            if existing:
                errors.append(f"Task {idx+1}: Email {address}@gmail.com already exists")
                continue
            
            unique_id = generate_task_id()
            
            Database.execute('''
            INSERT INTO tasks (unique_task_id, name, father_name, address, password, reward, status)
            VALUES (?, ?, ?, ?, ?, ?, 'available')
            ''', (unique_id, name, father_name, address, password, reward))
            
            created += 1
            
        except Exception as e:
            errors.append(f"Task {idx+1}: {str(e)}")
    
    return created, errors

def get_pending_tasks():
    rows = Database.fetchall('''
    SELECT t.*, u.username, u.user_id as user_id 
    FROM tasks t
    LEFT JOIN users u ON t.assigned_to = u.user_id
    WHERE t.status = 'pending'
    ORDER BY t.completed_time ASC
    ''')
    
    return [dict(row) for row in rows]

def approve_task(task_id: int, admin_id: int, custom_message: str = None, image_path: str = None):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
    if not task:
        return False
    
    task = dict(task)
    user_id = task['assigned_to']
    reward = task['reward']
    
    cursor.execute('UPDATE tasks SET status = "approved" WHERE task_id = ?', (task_id,))
    
    cursor.execute('''
    UPDATE users 
    SET balance = balance + ?, 
        hold_balance = hold_balance - ?,
        tasks_completed = tasks_completed + 1,
        total_earned = total_earned + ?
    WHERE user_id = ?
    ''', (reward, reward, reward, user_id))
    
    cursor.execute('''
    INSERT INTO transactions (user_id, amount, trans_type, task_id, details)
    VALUES (?, ?, ?, ?, ?)
    ''', (user_id, reward, 'task_completion', task_id, f'Task #{task_id} approved'))
    
    user = get_user(user_id)
    if user and user.get('referred_by'):
        referrer_id = user['referred_by']
        settings = get_bonus_settings()
        percentage = settings.get('referral_percentage', 5.0)
        
        if percentage > 0:
            referrer_earning = reward * (percentage / 100)
            
            cursor.execute('''
            UPDATE users 
            SET balance = balance + ?, referred_earnings = referred_earnings + ?
            WHERE user_id = ?
            ''', (referrer_earning, referrer_earning, referrer_id))
            
            cursor.execute('''
            INSERT INTO transactions (user_id, amount, trans_type, details)
            VALUES (?, ?, ?, ?)
            ''', (referrer_id, referrer_earning, 'referral_earning', 
                   f'{percentage}% of referred user {user_id} task reward: ETB{reward:.2f}'))
    
    conn.commit()
    return True

def reject_task(task_id: int, admin_id: int, custom_message: str = None):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
    if not task:
        return False
    
    task = dict(task)
    user_id = task['assigned_to']
    reward = task['reward']
    
    cursor.execute('''
    UPDATE tasks 
    SET status = 'rejected'
    WHERE task_id = ?
    ''', (task_id,))
    
    cursor.execute('''
    UPDATE users 
    SET hold_balance = hold_balance - ? 
    WHERE user_id = ?
    ''', (reward, user_id))
    
    cursor.execute('''
    UPDATE users 
    SET active_tasks_count = active_tasks_count - 1 
    WHERE user_id = ? AND active_tasks_count > 0
    ''', (user_id,))
    
    conn.commit()
    return True

def get_task_by_unique_id(unique_id: str):
    row = Database.fetchone('''
    SELECT t.*, u.username, u.user_id as completed_by_user_id
    FROM tasks t
    LEFT JOIN users u ON t.assigned_to = u.user_id
    WHERE t.unique_task_id = ? AND t.status = 'approved'
    ''', (unique_id,))
    
    return dict(row) if row else None

def get_approved_tasks_paginated(page: int = 1, per_page: int = 10):
    offset = (page - 1) * per_page
    
    rows = Database.fetchall('''
    SELECT t.unique_task_id, t.address, t.completed_time
    FROM tasks t
    WHERE t.status = 'approved'
    ORDER BY t.completed_time DESC
    LIMIT ? OFFSET ?
    ''', (per_page, offset))
    
    total_rows = Database.fetchone('SELECT COUNT(*) as count FROM tasks WHERE status = "approved"')
    total = total_rows[0] if total_rows else 0
    total_pages = (total + per_page - 1) // per_page
    
    return [dict(row) for row in rows], page, total_pages, total

# ==================== PAYMENT MANAGEMENT ====================
def save_payment_method(user_id: int, method: str, **kwargs):
    if method == 'telebirr':
        Database.execute('''
        UPDATE users 
        SET telebirr_name = ?, telebirr_phone = ?, default_payment_method = 'telebirr'
        WHERE user_id = ?
        ''', (kwargs.get('name'), kwargs.get('phone'), user_id))
    elif method == 'binance':
        Database.execute('''
        UPDATE users 
        SET binance_id = ?, default_payment_method = 'binance'
        WHERE user_id = ?
        ''', (kwargs.get('binance_id'), user_id))
    elif method == 'cbe':
        Database.execute('''
        UPDATE users 
        SET cbe_name = ?, cbe_account = ?, default_payment_method = 'cbe'
        WHERE user_id = ?
        ''', (kwargs.get('name'), kwargs.get('account'), user_id))

def get_payment_methods(user_id: int):
    row = Database.fetchone('''
    SELECT telebirr_name, telebirr_phone, binance_id, cbe_name, cbe_account, default_payment_method 
    FROM users WHERE user_id = ?
    ''', (user_id,))
    
    if row:
        return dict(row)
    return None

def get_active_payment_methods():
    rows = Database.fetchall('SELECT * FROM payment_methods WHERE is_active = 1')
    return [dict(row) for row in rows]

def request_payout(user_id: int, amount: float, method: str, details: str, new_details: str = None):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    settings = get_bonus_settings()
    min_withdrawal = settings['min_withdrawal']
    
    if amount < min_withdrawal:
        return False, f"Minimum withdrawal is ETB{min_withdrawal:.2f}"
    
    user = get_user(user_id)
    if not user or user['balance'] < amount:
        return False, "Insufficient balance"
    
    cursor.execute('''
    INSERT INTO payouts (user_id, amount, payout_method, payout_details, new_payout_details, status)
    VALUES (?, ?, ?, ?, ?, 'pending')
    ''', (user_id, amount, method, details, new_details))
    
    cursor.execute('''
    UPDATE users 
    SET balance = balance - ?, hold_balance = hold_balance + ?
    WHERE user_id = ?
    ''', (amount, amount, user_id))
    
    cursor.execute('''
    INSERT INTO transactions (user_id, amount, trans_type, details)
    VALUES (?, ?, ?, ?)
    ''', (user_id, -amount, 'withdrawal_request', f'Payout request via {method}'))
    
    conn.commit()
    return True, "✅ Payout request submitted successfully!"

def get_pending_payouts():
    rows = Database.fetchall('''
    SELECT p.*, u.username, u.first_name, u.telebirr_name, u.telebirr_phone, u.binance_id, u.cbe_name, u.cbe_account
    FROM payouts p
    JOIN users u ON p.user_id = u.user_id
    WHERE p.status = 'pending'
    ORDER BY p.request_time ASC
    ''')
    
    return [dict(row) for row in rows]

def process_payout(payout_id: int, approve: bool, admin_id: int, image_path: str = None):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    payout = Database.fetchone('SELECT * FROM payouts WHERE payout_id = ?', (payout_id,))
    if not payout:
        return False
    
    payout = dict(payout)
    user_id = payout['user_id']
    amount = payout['amount']
    payout_method = payout['payout_method']
    payout_details = payout['payout_details']
    
    if approve:
        cursor.execute('''
        UPDATE payouts 
        SET status = 'approved', processed_time = CURRENT_TIMESTAMP, processed_by = ?, image_path = ?
        WHERE payout_id = ?
        ''', (admin_id, image_path, payout_id))
        
        cursor.execute('''
        UPDATE users 
        SET hold_balance = hold_balance - ?
        WHERE user_id = ?
        ''', (amount, user_id))
        
        cursor.execute('''
        INSERT INTO transactions (user_id, amount, trans_type, details)
        VALUES (?, ?, ?, ?)
        ''', (user_id, -amount, 'withdrawal_approved', f'Payout #{payout_id} approved'))
        
    else:
        cursor.execute('''
        UPDATE payouts 
        SET status = 'rejected', processed_time = CURRENT_TIMESTAMP, processed_by = ?
        WHERE payout_id = ?
        ''', (admin_id, payout_id))
        
        cursor.execute('''
        UPDATE users 
        SET balance = balance + ?, hold_balance = hold_balance - ?
        WHERE user_id = ?
        ''', (amount, amount, user_id))
        
        cursor.execute('''
        INSERT INTO transactions (user_id, amount, trans_type, details)
        VALUES (?, ?, ?, ?)
        ''', (user_id, amount, 'withdrawal_rejected', f'Payout #{payout_id} rejected - funds returned'))
    
    conn.commit()
    return True

def adjust_user_balance(user_id: int, amount: float, adjust_type: str, reason: str, admin_id: int):
    conn = Database.get_connection()
    cursor = conn.cursor()
    
    user = get_user(user_id)
    if not user:
        return False
    
    if adjust_type == 'add':
        cursor.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
        trans_type = 'admin_addition'
        trans_amount = amount
    elif adjust_type == 'subtract':
        if user['balance'] < amount:
            return False
        cursor.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (amount, user_id))
        trans_type = 'admin_deduction'
        trans_amount = -amount
    else:
        return False
    
    cursor.execute('''
    INSERT INTO transactions (user_id, amount, trans_type, details)
    VALUES (?, ?, ?, ?)
    ''', (user_id, trans_amount, trans_type, f'Admin adjustment: {reason}'))
    
    conn.commit()
    return True

# ==================== USER MANAGEMENT ====================
def get_all_users(limit: int = 100):
    rows = Database.fetchall('''
    SELECT user_id, username, first_name, balance, hold_balance, tasks_completed, 
           referral_count, total_earned, registered_date, last_active, is_admin
    FROM users 
    ORDER BY registered_date DESC
    LIMIT ?
    ''', (limit,))
    
    return [dict(row) for row in rows]

def get_recent_users(limit: int = 10):
    rows = Database.fetchall('''
    SELECT user_id, username, first_name, registered_date, is_admin
    FROM users 
    ORDER BY registered_date DESC
    LIMIT ?
    ''', (limit,))
    
    return [dict(row) for row in rows]

def search_users(query: str):
    if query.isdigit():
        rows = Database.fetchall('''
        SELECT user_id, username, first_name, balance, tasks_completed, is_admin
        FROM users 
        WHERE user_id = ?
        LIMIT 1
        ''', (int(query),))
        return [dict(row) for row in rows] if rows else []
    
    rows = Database.fetchall('''
    SELECT user_id, username, first_name, balance, tasks_completed, is_admin
    FROM users 
    WHERE username LIKE ? OR first_name LIKE ? OR telebirr_phone LIKE ?
    LIMIT 20
    ''', (f'%{query}%', f'%{query}%', f'%{query}%'))
    
    return [dict(row) for row in rows]

def get_user_statistics(user_id: int):
    user = get_user(user_id)
    if not user:
        return None
    
    tasks = Database.fetchall('''
    SELECT status, COUNT(*) as count
    FROM tasks 
    WHERE assigned_to = ?
    GROUP BY status
    ''', (user_id,))
    
    transactions = Database.fetchall('''
    SELECT trans_type, SUM(amount) as total
    FROM transactions 
    WHERE user_id = ?
    GROUP BY trans_type
    ''', (user_id,))
    
    payouts = Database.fetchall('''
    SELECT amount, status, request_time
    FROM payouts 
    WHERE user_id = ?
    ORDER BY request_time DESC
    LIMIT 10
    ''', (user_id,))
    
    return {
        'user': user,
        'tasks': [dict(row) for row in tasks],
        'transactions': {row['trans_type']: row['total'] for row in transactions},
        'payouts': [dict(row) for row in payouts],
        'active_tasks': user.get('active_tasks_count', 0)
    }

# ==================== BROADCAST SYSTEM ====================
async def broadcast_message(context, message: str, user_ids: List[int] = None):
    if user_ids is None:
        rows = Database.fetchall('SELECT user_id FROM users')
        user_ids = [row['user_id'] for row in rows]
    
    sent = 0
    failed = 0
    
    for user_id in user_ids:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=message,
                parse_mode=ParseMode.HTML
            )
            sent += 1
            await asyncio.sleep(0.1)
        except Exception:
            failed += 1
    
    return sent, failed

# ==================== NOTIFICATION SYSTEM ====================
async def send_user_notification(context, user_id: int, message: str, image_path: str = None):
    try:
        if image_path and os.path.exists(image_path):
            with open(image_path, 'rb') as photo:
                sent_msg = await context.bot.send_photo(
                    chat_id=user_id,
                    photo=photo,
                    caption=message,
                    parse_mode=ParseMode.HTML
                )
                await queue_message_for_deletion(user_id, sent_msg.message_id)
        else:
            sent_msg = await context.bot.send_message(
                chat_id=user_id,
                text=message,
                parse_mode=ParseMode.HTML
            )
            await queue_message_for_deletion(user_id, sent_msg.message_id)
        return True
    except Exception as e:
        print(f"Error sending notification to {user_id}: {e}")
        return False

# ==================== KEYBOARD DEFINITIONS ====================
def get_main_menu(user_id: int):
    admin_status = is_admin(user_id)
    
    buttons = [
        ["📋 Take Task", "📝 My Tasks"],
        ["💰 My Balance", "👥 My Referrals"],
        ["💸 Request Payout", "⚙️ Settings"]
    ]
    
    if admin_status:
        buttons.append(["👑 Admin Panel"])
    
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_settings_menu():
    buttons = [
        ["🔧 Payment Methods", "📞 Contact Admin"],
        ["📊 Account Info", "🔄 Change Payment Method"],
        ["🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_payment_methods_menu():
    buttons = [
        ["📱 Setup Telebirr", "🪙 Setup Binance"],
        ["🏦 Setup CBE", "📋 View Saved Methods"],
        ["🗑️ Clear Methods", "🔙 Back to Settings"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_task_action_menu():
    buttons = [
        ["✅ Done", "❌ Cancel Task"],
        ["🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_task_confirmation_menu():
    buttons = [
        ["✅ Confirm Done", "❌ Go Back"],
        ["🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_cancel_confirmation_menu():
    buttons = [
        ["✅ Confirm Cancel", "❌ Keep Task"],
        ["🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_otp_action_menu(resend_remaining: int = 3):
    buttons = [
        [f"📨 Resend Code ({resend_remaining} left)"],
        ["❌ Cancel Task"],
        ["🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_active_task_warning_menu():
    buttons = [
        ["📋 View My Active Task"],
        ["❌ Cancel My Active Task"],
        ["🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_payout_methods_menu(has_telebirr: bool, has_binance: bool, has_cbe: bool):
    buttons = []
    
    if has_telebirr:
        buttons.append(["📱 Use Saved Telebirr"])
    if has_binance:
        buttons.append(["🪙 Use Saved Binance"])
    if has_cbe:
        buttons.append(["🏦 Use Saved CBE"])
    
    buttons.append(["❌ Cancel"])
    
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_user_management_menu():
    buttons = [
        ["📋 List Recent Users", "🔍 Search User by ID"],
        ["📊 View Online Users", "🏠 Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_user_detail_menu(user_id: int):
    buttons = [
        [f"💰 Add Balance #{user_id}", f"📉 Subtract Balance #{user_id}"],
        [f"📨 Message User #{user_id}", f"📊 View Details #{user_id}"],
        ["🔙 Back to Users", "🏠 Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_broadcast_menu():
    buttons = [
        ["📢 Broadcast to All", "📨 Broadcast to User"],
        ["🔙 Back to Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_task_review_menu(task_id: int):
    buttons = [
        [f"✅ Approve #{task_id}", f"❌ Reject #{task_id}"],
        ["⏭️ Next Task", "🏠 Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_payout_approval_options():
    buttons = [
        ["✅ Approve with Default Message"],
        ["📸 Approve with Image + Default Message"],
        ["❌ Cancel Approval"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_payout_rejection_options():
    buttons = [
        ["❌ Reject with Default Message"],
        ["❌ Cancel Rejection"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_channels_menu():
    channels = get_mandatory_channels()
    channels_list = "\n".join([f"• {ch}" for ch in channels]) if channels else "No channels added"
    
    buttons = [
        ["➕ Add Channel", "➖ Remove Channel"],
        ["📋 View All Channels", "🔙 Back to Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_payment_methods_menu():
    buttons = [
        ["📋 View Active Methods", "➕ Add Method"],
        ["➖ Remove Method", "🔙 Back to Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_email_accounts_menu():
    buttons = [
        ["➕ Add Email Account", "➖ Remove Email Account"],
        ["✅ Enable Account", "❌ Disable Account"],
        ["📋 View All Accounts", "🔙 Back to Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_milestone_menu():
    buttons = [
        ["✅ Enable Milestones", "❌ Disable Milestones"],
        ["📋 View Milestones", "✏️ Edit Milestone"],
        ["➕ Add Milestone", "🗑️ Remove Milestone"],
        ["🔙 Back to Admin Settings"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_referral_menu():
    buttons = [
        ["💰 Edit Referral Bonus", "📊 Edit Referral Percentage"],
        ["🎯 Milestone Bonuses", "📢 Broadcast Referral Update"],
        ["🔙 Back to Admin Settings"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_delete_tasks_menu():
    buttons = [
        ["🗑️ Delete Single Completed Task"],
        ["🗑️ Delete All Completed Tasks"],
        ["🗑️ Delete All Failed Tasks"],
        ["🔙 Back to Admin Settings"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_export_menu():
    buttons = [
        ["📥 Export Completed Tasks"],
        ["📥 Export Failed Tasks"],
        ["🔙 Back to Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_settings_menu():
    buttons = [
        ["💰 Adjust Rewards", "⏰ Set Expiry Hours"],
        ["📋 Payment Methods", "📝 Update Messages"],
        ["📢 Manage Channels", "🔍 Task Info"],
        ["📧 Email Accounts", "🗑️ Delete Tasks"],
        ["🎯 Milestone Bonuses", "📞 Set Contact Admin"],
        ["🎯 Referral Settings", "➕ Add Admin"],
        ["➖ Remove Admin", "📋 Admin List"],
        ["🔐 OTP", "📢 Payout Channel"],
        ["🏠 Admin Menu"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_otp_menu():
    current_status = is_otp_required()
    status_text = "✅ ENABLED" if current_status else "❌ DISABLED"
    buttons = [
        [f"🔄 Toggle OTP (Currently {status_text})"],
        ["🔑 Generate OTP for User"],
        ["🔙 Back to Admin Settings"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_payout_channel_menu():
    current_channel = get_payout_channel()
    buttons = [
        [f"📢 Current Channel: {current_channel}"],
        ["✏️ Change Payout Channel"],
        ["🔙 Back to Admin Settings"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# ==================== OTP HANDLERS ====================
async def handle_otp_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'otp' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to manage OTP!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    await update.message.reply_text(
        "🔐 OTP Management\n\n"
        "Select an option:",
        reply_markup=get_otp_menu()
    )

async def handle_otp_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'otp' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to toggle OTP!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    current_status = is_otp_required()
    new_status = not current_status
    set_otp_required(new_status)
    
    status_text = "ENABLED" if new_status else "DISABLED"
    
    await update.message.reply_text(
        f"🔐 OTP Verification has been {status_text}!\n\n"
        f"When OTP is ENABLED: Users must enter verification code to complete tasks\n"
        f"When OTP is DISABLED: Users complete tasks directly without code\n\n"
        f"Current Status: {'✅ ON' if new_status else '❌ OFF'}",
        reply_markup=get_otp_menu()
    )

async def handle_generate_otp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'otp' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to generate OTP!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    context.user_data['awaiting_generate_otp_user'] = True
    await update.message.reply_text(
        "🔑 Generate OTP for User\n\n"
        "Enter the user ID to generate a verification code:\n"
        "Example: 8360602913\n\n"
        "This OTP will be sent to the user's email and can only be used once.",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_generate_otp_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    try:
        target_user_id = int(message.text.strip())
    except ValueError:
        await message.reply_text(
            "❌ Invalid user ID! Please enter a valid number.",
            reply_markup=get_otp_menu()
        )
        del context.user_data['awaiting_generate_otp_user']
        return
    
    target_user = get_user(target_user_id)
    if not target_user:
        await message.reply_text(
            f"❌ User {target_user_id} not found!",
            reply_markup=get_otp_menu()
        )
        del context.user_data['awaiting_generate_otp_user']
        return
    
    user_email = None
    
    task = Database.fetchone('''
    SELECT address FROM tasks WHERE assigned_to = ? AND status IN ('approved', 'pending')
    LIMIT 1
    ''', (target_user_id,))
    
    if task:
        user_email = f"{task['address']}@gmail.com"
    
    if not user_email:
        context.user_data['temp_target_user_id'] = target_user_id
        context.user_data['awaiting_generate_otp_email'] = True
        await message.reply_text(
            f"👤 User: {target_user['first_name']} (@{target_user['username'] or 'N/A'})\n\n"
            f"Enter the email address to send the OTP to:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
    
    otp = generate_secure_otp()
    
    if send_otp_email(user_email, otp, is_admin_generated=True, task_name="Admin Verification"):
        admin_generated_otps[f"{user.id}_{target_user_id}_{int(time.time())}"] = {
            "otp": otp,
            "user_id": target_user_id,
            "email": user_email,
            "expires": time.time() + 300,
            "used": False,
            "admin_id": user.id,
            "created_at": time.time()
        }
        
        await message.reply_text(
            f"✅ OTP sent successfully!\n\n"
            f"👤 User: {target_user['first_name']} (@{target_user['username'] or 'N/A'})\n"
            f"📧 Email: {user_email}\n"
            f"🔑 OTP: <code>{otp}</code>\n"
            f"⏰ Expires in 5 minutes\n"
            f"🔒 One-time use only\n\n"
            f"💡 You can also share this code with the user if they didn't receive the email.",
            parse_mode=ParseMode.HTML,
            reply_markup=get_otp_menu()
        )
        
        await send_user_notification(
            context, target_user_id,
            f"🔐 <b>Admin Generated Verification Code</b>\n\n"
            f"An admin has generated a verification code for you:\n\n"
            f"<code>{otp}</code>\n\n"
            f"⏰ This code expires in 5 minutes\n"
            f"🔒 This code can only be used once.\n\n"
            f"Enter this code when prompted."
        )
    else:
        await message.reply_text(
            "❌ Failed to send OTP email!\n\n"
            "Please check email account configuration.",
            reply_markup=get_otp_menu()
        )
    
    del context.user_data['awaiting_generate_otp_user']

async def handle_generate_otp_email_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    user_email = message.text.strip()
    
    if not user_email.endswith('@gmail.com'):
        await message.reply_text(
            "❌ Only Gmail addresses are supported!\n\n"
            "Please enter a valid Gmail address:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
    
    target_user_id = context.user_data.get('temp_target_user_id')
    target_user = get_user(target_user_id)
    
    if not target_user:
        await message.reply_text(
            "❌ User not found!",
            reply_markup=get_otp_menu()
        )
        del context.user_data['awaiting_generate_otp_email']
        del context.user_data['temp_target_user_id']
        return
    
    otp = generate_secure_otp()
    
    if send_otp_email(user_email, otp, is_admin_generated=True, task_name="Admin Verification"):
        admin_generated_otps[f"{user.id}_{target_user_id}_{int(time.time())}"] = {
            "otp": otp,
            "user_id": target_user_id,
            "email": user_email,
            "expires": time.time() + 300,
            "used": False,
            "admin_id": user.id,
            "created_at": time.time()
        }
        
        await message.reply_text(
            f"✅ OTP sent successfully!\n\n"
            f"👤 User: {target_user['first_name']} (@{target_user['username'] or 'N/A'})\n"
            f"📧 Email: {user_email}\n"
            f"🔑 OTP: <code>{otp}</code>\n"
            f"⏰ Expires in 5 minutes\n"
            f"🔒 One-time use only",
            parse_mode=ParseMode.HTML,
            reply_markup=get_otp_menu()
        )
        
        await send_user_notification(
            context, target_user_id,
            f"🔐 <b>Admin Generated Verification Code</b>\n\n"
            f"An admin has generated a verification code for you:\n\n"
            f"<code>{otp}</code>\n\n"
            f"⏰ This code expires in 5 minutes\n"
            f"🔒 This code can only be used once."
        )
    else:
        await message.reply_text(
            "❌ Failed to send OTP email!\n\n"
            "Please check email account configuration.",
            reply_markup=get_otp_menu()
        )
    
    del context.user_data['awaiting_generate_otp_email']
    del context.user_data['temp_target_user_id']

# ==================== PAYOUT CHANNEL HANDLER ====================
async def handle_payout_channel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'payout_channel' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to manage payout channel!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    await update.message.reply_text(
        "📢 Payout Channel Management\n\n"
        "Select an option:",
        reply_markup=get_payout_channel_menu()
    )

async def handle_change_payout_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'payout_channel' not in get_admin_permissions(user.id):
        return
    
    context.user_data['awaiting_payout_channel'] = True
    await update.message.reply_text(
        "✏️ Change Payout Channel\n\n"
        "Enter the new channel username or ID:\n"
        "Example: @gojofarmers or -1001234567890\n\n"
        "⚠️ Make sure the bot is an admin in that channel!",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_payout_channel_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    channel = message.text.strip()
    
    if 'awaiting_payout_channel' in context.user_data:
        set_payout_channel(channel)
        
        await message.reply_text(
            f"✅ Payout channel updated to: {channel}\n\n"
            f"All future payout confirmations will be posted there.",
            reply_markup=get_payout_channel_menu()
        )
        
        try:
            await context.bot.send_message(
                chat_id=channel,
                text=f"✅ Test message from {BOT_NAME}\n\nPayout channel configured successfully!"
            )
            await message.reply_text(f"✅ Test message sent to {channel} successfully!")
        except Exception as e:
            await message.reply_text(
                f"⚠️ Could not send test message to {channel}!\n"
                f"Error: {str(e)}\n\n"
                f"Please make sure the bot is an admin in that channel."
            )
        
        del context.user_data['awaiting_payout_channel']

# ==================== PENDING APPROVAL LIST - VIEW ONLY ====================
async def handle_pending_approval_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all pending approval tasks - VIEW ONLY, no approve/reject buttons"""
    user = update.effective_user
    
    if not is_admin(user.id) or 'pending_approval' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You are not authorized!",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    page = context.user_data.get('pending_approval_page', 1)
    tasks, current_page, total_pages, total = get_all_pending_approval_tasks(page)
    
    if not tasks:
        await update.message.reply_text(
            "📭 No pending approval tasks found.\n\nAll tasks have been reviewed or no tasks submitted yet.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    response = f"<b>📋 PENDING APPROVAL LIST (VIEW ONLY)</b>\n"
    response += f"📊 Page {current_page}/{total_pages} | Total: {total} tasks waiting for review\n\n"
    response += f"<i>💡 This is a view-only list. Use '⏳ Pending Tasks' to approve/reject.</i>\n\n"
    response += "<code>" + "="*50 + "</code>\n\n"
    
    for i, task in enumerate(tasks, 1):
        email = f"{task['address']}@gmail.com"
        completed_time = task['completed_time'].split()[0] if task['completed_time'] else 'Unknown'
        
        response += f"<b>#{i}</b>\n"
        response += f"┌ <b>Task ID:</b> <code>{task['unique_task_id']}</code>\n"
        response += f"├ <b>Name:</b> <code>{task['name']}</code>\n"
        if task['father_name']:
            response += f"├ <b>Father Name:</b> <code>{task['father_name']}</code>\n"
        response += f"├ <b>Email:</b> <code>{email}</code>\n"
        response += f"├ <b>Password:</b> <code>{task['password']}</code>\n"
        response += f"├ <b>Reward:</b> ETB{task['reward']:.2f}\n"
        response += f"├ <b>User ID:</b> <code>{task['assigned_to']}</code>\n"
        response += f"├ <b>Username:</b> @{task['username'] or 'N/A'}\n"
        response += f"└ <b>Submitted:</b> {completed_time}\n\n"
    
    keyboard = []
    nav_row = []
    if current_page > 1:
        nav_row.append(InlineKeyboardButton("◀️ Previous", callback_data=f"pending_approval_page_{current_page-1}"))
    if current_page < total_pages:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"pending_approval_page_{current_page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="refresh_pending_approval")])
    keyboard.append([InlineKeyboardButton("🏠 Admin Menu", callback_data="back_to_admin")])
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

# ==================== TASK HANDLERS ====================
async def handle_take_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    active_tasks = get_user_active_tasks(user.id)
    
    if active_tasks:
        active_task = active_tasks[0]
        context.user_data['active_task_id'] = active_task['task_id']
        
        await message.reply_text(
            f"⚠️ <b>You have an active task!</b>\n\n"
            f"📧 Email: <code>{active_task['address']}</code>@gmail.com\n"
            f"💰 Reward: ETB{active_task['reward']:.2f}\n\n"
            f"What would you like to do?",
            parse_mode=ParseMode.HTML,
            reply_markup=get_active_task_warning_menu()
        )
        return
    
    await take_new_task(update, context)

async def take_new_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    user_data = get_user(user.id)
    if user_data and user_data.get('active_tasks_count', 0) >= MAX_ACTIVE_TASKS:
        await message.reply_text(
            f"⚠️ You can have maximum {MAX_ACTIVE_TASKS} active task at once.\n"
            f"Complete or cancel your current task first.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    task = get_available_task(user.id)
    
    if not task:
        await message.reply_text(
            "📭 No tasks available at the moment.\n"
            "Please check back later!",
            reply_markup=get_main_menu(user.id)
        )
        return
        
    context.user_data['current_task'] = task['task_id']
    expiry_hours = get_task_expiry_hours()
    settings = get_bonus_settings()
    
    otp_status = "🔐 OTP Verification: " + ("ON" if is_otp_required() else "OFF")
    
    task_msg = (
        f"📋 Task Assigned!\n\n"
        f"Register account using the specified data and earn ETB{settings['task_reward']:.2f}\n\n"
        f"👤 Name: <code>{task['name']}</code>\n"
    )
    
    if task['father_name'] and task['father_name'].strip():
        task_msg += f"👨 Father Name: <code>{task['father_name']}</code>\n"
    
    task_msg += (
        f"📧 Email: <code>{task['address']}</code>@gmail.com\n"
        f"🔐 Password: <code>{task['password']}</code>\n\n"
        f"⚠️ Be sure to use the exact data above.\n\n"
        f"⏰ This task expires in {expiry_hours} hours.\n"
        f"{otp_status}\n\n"
        f"💡 Click on the email username to copy it!"
    )
    
    sent_msg = await message.reply_text(
        task_msg,
        parse_mode=ParseMode.HTML,
        reply_markup=get_task_action_menu()
    )
    context.user_data['task_message_id'] = sent_msg.message_id
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_view_active_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('active_task_id') or context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
    
    if not task:
        await message.reply_text(
            "Task not found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    task = dict(task)
    
    task_msg = (
        f"📋 Your Active Task\n\n"
        f"👤 Name: <code>{task['name']}</code>\n"
    )
    
    if task['father_name'] and task['father_name'].strip():
        task_msg += f"👨 Father Name: <code>{task['father_name']}</code>\n"
    
    task_msg += (
        f"📧 Email: <code>{task['address']}</code>@gmail.com\n"
        f"🔐 Password: <code>{task['password']}</code>\n\n"
        f"💰 Reward: ETB{task['reward']:.2f}\n\n"
        f"⚠️ Be sure to use the exact data above.\n\n"
        f"What would you like to do?"
    )
    
    sent_msg = await message.reply_text(
        task_msg,
        parse_mode=ParseMode.HTML,
        reply_markup=get_task_action_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)
    
    context.user_data['current_task'] = task_id

async def handle_cancel_active_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('active_task_id') or context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    context.user_data['cancelling_task'] = task_id
    
    sent_msg = await message.reply_text(
        f"Are you sure you want to cancel this task?\n\n"
        f"❌ The task will be returned to available tasks\n"
        f"❌ You will lose this task assignment\n"
        f"✅ You can take it again later",
        reply_markup=get_cancel_confirmation_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_confirm_task_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('cancelling_task') or context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    success = cancel_task_assignment(task_id, user.id, return_to_available=True)
    
    if success:
        if 'task_message_id' in context.user_data:
            try:
                await context.bot.delete_message(
                    chat_id=user.id,
                    message_id=context.user_data['task_message_id']
                )
            except:
                pass
            del context.user_data['task_message_id']
        
        response = "❌ Task cancelled and returned to available tasks.\n\nYou can now take another task!"
    else:
        response = "❌ Failed to cancel task."
    
    sent_msg = await message.reply_text(
        response,
        reply_markup=get_main_menu(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)
    
    context.user_data.pop('current_task', None)
    context.user_data.pop('active_task_id', None)
    context.user_data.pop('cancelling_task', None)
    context.user_data.pop('awaiting_task_otp', None)

async def handle_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    success, result_type, email = mark_task_for_completion(task_id, user.id)
    
    if not success:
        await message.reply_text(
            "❌ Failed to process task. Please try again.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    if result_type == 'direct_submit':
        if 'task_message_id' in context.user_data:
            try:
                await context.bot.delete_message(
                    chat_id=user.id,
                    message_id=context.user_data['task_message_id']
                )
            except:
                pass
            del context.user_data['task_message_id']
        
        settings = get_bonus_settings()
        reward = settings['task_reward']
        
        sent_msg = await message.reply_text(
            f"✅ Task submitted for admin approval!\n\n"
            f"The task has been submitted for admin review.\n"
            f"⏰ You'll be notified when it's approved.\n"
            f"💰 Reward: ETB{reward:.2f} (pending)\n\n"
            f"✅ You can now take another task!",
            reply_markup=get_main_menu(user.id)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        
        context.user_data.pop('current_task', None)
        context.user_data.pop('active_task_id', None)
        
    else:
        otp = generate_secure_otp()
        
        sent_msg = await message.reply_text(
            f"✉️ Sending verification code to <code>{email}</code>...\n\n"
            f"Please check your inbox (or spam folder).\n"
            f"Enter the 6-digit code to verify task completion.\n\n"
            f"💡 You can resend the code up to 3 times if needed.",
            parse_mode=ParseMode.HTML
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        
        task = Database.fetchone('SELECT name FROM tasks WHERE task_id = ?', (task_id,))
        task_name = task['name'] if task else ""
        
        if send_otp_email(email, otp, task_name):
            store_task_otp(user.id, task_id, email, otp)
            context.user_data['awaiting_task_otp'] = True
            context.user_data['otp_task_id'] = task_id
            
            sent_msg = await message.reply_text(
                "🔐 <b>Verification Code Sent!</b>\n\n"
                "Please enter the 6-digit code you received.\n"
                f"You have 3 attempts, code expires in 5 minutes.\n\n"
                f"📨 You can resend the code up to 3 times.",
                parse_mode=ParseMode.HTML,
                reply_markup=get_otp_action_menu(3)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
        else:
            Database.execute('''
            UPDATE tasks SET status = 'assigned' WHERE task_id = ?
            ''', (task_id,))
            await message.reply_text(
                "❌ Failed to send verification email.\n\n"
                "Please try again or contact support.",
                reply_markup=get_task_action_menu()
            )

async def handle_otp_resend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    can_resend, remaining, msg = can_resend_otp(user.id)
    
    if not can_resend:
        sent_msg = await message.reply_text(
            msg,
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel Task"]], resize_keyboard=True)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        return
    
    success, result = resend_otp(user.id)
    
    if success:
        _, remaining, _ = can_resend_otp(user.id)
        sent_msg = await message.reply_text(
            f"✅ {result}\n\n"
            f"Enter the 6-digit code to verify task completion.",
            reply_markup=get_otp_action_menu(remaining)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
    else:
        sent_msg = await message.reply_text(
            f"❌ {result}",
            reply_markup=get_otp_action_menu(remaining if 'remaining' in locals() else 0)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_otp_cancel_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    success, task_id, msg = cancel_otp_task(user.id)
    
    if success and task_id:
        cancel_otp_task_in_db(task_id, user.id)
        
        context.user_data.pop('current_task', None)
        context.user_data.pop('active_task_id', None)
        context.user_data.pop('awaiting_task_otp', None)
        context.user_data.pop('otp_task_id', None)
        
        sent_msg = await message.reply_text(
            f"❌ {msg}\n\nYou can now take another task.",
            reply_markup=get_main_menu(user.id)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
    else:
        sent_msg = await message.reply_text(
            msg or "No active OTP request found.",
            reply_markup=get_main_menu(user.id)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_task_otp_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    otp = message.text.strip()
    
    # Check admin-generated OTPs
    for key, data in admin_generated_otps.items():
        if (data["otp"] == otp and 
            data["user_id"] == user.id and 
            not data["used"] and 
            time.time() < data["expires"]):
            
            data["used"] = True
            
            if 'awaiting_task_otp' in context.user_data:
                task_id = context.user_data.get('otp_task_id')
                if task_id and submit_task_after_otp(task_id, user.id):
                    if 'task_message_id' in context.user_data:
                        try:
                            await context.bot.delete_message(
                                chat_id=user.id,
                                message_id=context.user_data['task_message_id']
                            )
                        except:
                            pass
                        del context.user_data['task_message_id']
                    
                    settings = get_bonus_settings()
                    reward = settings['task_reward']
                    
                    sent_msg = await message.reply_text(
                        f"✅ Admin code verified! Task submitted for approval.\n\n"
                        f"The task has been submitted for admin review.\n"
                        f"💰 Reward: ETB{reward:.2f} (pending)\n\n"
                        f"✅ You can now take another task!",
                        reply_markup=get_main_menu(user.id)
                    )
                    await queue_message_for_deletion(user.id, sent_msg.message_id)
                    
                    context.user_data.pop('current_task', None)
                    context.user_data.pop('active_task_id', None)
                    context.user_data.pop('awaiting_task_otp', None)
                    context.user_data.pop('otp_task_id', None)
                else:
                    await message.reply_text(
                        "❌ Failed to submit task. Please contact support.",
                        reply_markup=get_main_menu(user.id)
                    )
            else:
                await message.reply_text(
                    "✅ Admin code verified! You can now proceed.",
                    reply_markup=get_main_menu(user.id)
                )
            return
    
    if not otp.isdigit() or len(otp) != 6:
        sent_msg = await message.reply_text(
            "❌ Invalid code!\n\n"
            "Please enter the 6-digit verification code:",
            reply_markup=get_otp_action_menu()
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        return
    
    success, task_id, result = verify_task_otp(user.id, otp)
    
    if success and task_id:
        if submit_task_after_otp(task_id, user.id):
            if 'task_message_id' in context.user_data:
                try:
                    await context.bot.delete_message(
                        chat_id=user.id,
                        message_id=context.user_data['task_message_id']
                    )
                except:
                    pass
                del context.user_data['task_message_id']
            
            settings = get_bonus_settings()
            reward = settings['task_reward']
            
            sent_msg = await message.reply_text(
                f"✅ {result}\n\n"
                f"The task has been submitted for admin approval.\n"
                f"⏰ You'll be notified when it's approved.\n"
                f"💰 Reward: ETB{reward:.2f} (pending)\n\n"
                f"✅ You can now take another task!",
                reply_markup=get_main_menu(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
            
            context.user_data.pop('current_task', None)
            context.user_data.pop('active_task_id', None)
            context.user_data.pop('awaiting_task_otp', None)
            context.user_data.pop('otp_task_id', None)
        else:
            await message.reply_text(
                "❌ Failed to submit task. Please contact support.",
                reply_markup=get_main_menu(user.id)
            )
    elif task_id and not success and "Too many failed attempts" in result:
        cancel_otp_task_in_db(task_id, user.id)
        context.user_data.pop('current_task', None)
        context.user_data.pop('active_task_id', None)
        context.user_data.pop('awaiting_task_otp', None)
        context.user_data.pop('otp_task_id', None)
        
        sent_msg = await message.reply_text(
            f"❌ {result}\n\nTask has been cancelled.",
            reply_markup=get_main_menu(user.id)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
    else:
        sent_msg = await message.reply_text(
            result,
            reply_markup=get_otp_action_menu()
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_cancel_during_otp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    if user.id in otp_storage:
        data = otp_storage[user.id]
        task_id = data["task_id"]
        
        if cancel_otp_task_in_db(task_id, user.id):
            del otp_storage[user.id]
            context.user_data.pop('current_task', None)
            context.user_data.pop('active_task_id', None)
            context.user_data.pop('awaiting_task_otp', None)
            context.user_data.pop('otp_task_id', None)
            
            if 'task_message_id' in context.user_data:
                try:
                    await context.bot.delete_message(
                        chat_id=user.id,
                        message_id=context.user_data['task_message_id']
                    )
                except:
                    pass
                del context.user_data['task_message_id']
            
            sent_msg = await message.reply_text(
                "❌ Task cancelled.\n\nYou can now take another task.",
                reply_markup=get_main_menu(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
        else:
            sent_msg = await message.reply_text(
                "❌ Failed to cancel task.",
                reply_markup=get_main_menu(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
    else:
        await handle_task_cancel(update, context)

async def handle_task_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    success = cancel_task_assignment(task_id, user.id, return_to_available=True)
    
    if success:
        if 'task_message_id' in context.user_data:
            try:
                await context.bot.delete_message(
                    chat_id=user.id,
                    message_id=context.user_data['task_message_id']
                )
            except:
                pass
            del context.user_data['task_message_id']
        
        response = "↩️ Task returned to available tasks.\nYou can take it again later."
    else:
        response = "❌ Failed to return task."
        
    context.user_data.pop('current_task', None)
    context.user_data.pop('active_task_id', None)
    
    sent_msg = await message.reply_text(
        response,
        reply_markup=get_main_menu(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_task_back_to_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
    if not task:
        await message.reply_text(
            "Task not found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    task = dict(task)
    
    task_msg = (
        f"📋 Task\n\n"
        f"Register account using the specified data and earn ETB{task['reward']:.2f}\n\n"
        f"👤 Name: <code>{task['name']}</code>\n"
    )
    
    if task['father_name'] and task['father_name'].strip():
        task_msg += f"👨 Father Name: <code>{task['father_name']}</code>\n"
    
    task_msg += (
        f"📧 Email: <code>{task['address']}</code>@gmail.com\n"
        f"🔐 Password: <code>{task['password']}</code>\n\n"
        f"⚠️ Be sure to use the exact data above, otherwise the task will not be approved.\n\n"
        f"💡 Click on the email username to copy it!"
    )
    
    sent_msg = await message.reply_text(
        task_msg,
        parse_mode=ParseMode.HTML,
        reply_markup=get_task_action_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_keep_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
    if not task:
        await message.reply_text(
            "Task not found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    task = dict(task)
    
    task_msg = (
        f"📋 Task (Kept)\n\n"
        f"Register account using the specified data and earn ETB{task['reward']:.2f}\n\n"
        f"👤 Name: <code>{task['name']}</code>\n"
    )
    
    if task['father_name'] and task['father_name'].strip():
        task_msg += f"👨 Father Name: <code>{task['father_name']}</code>\n"
    
    task_msg += (
        f"📧 Email: <code>{task['address']}</code>@gmail.com\n"
        f"🔐 Password: <code>{task['password']}</code>\n\n"
        f"⚠️ Be sure to use the exact data above, otherwise the task will not be approved.\n\n"
        f"💡 Click on the email username to copy it!"
    )
    
    sent_msg = await message.reply_text(
        task_msg,
        parse_mode=ParseMode.HTML,
        reply_markup=get_task_action_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_confirm_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    await handle_task_done(update, context)

async def handle_task_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    task_id = context.user_data.get('current_task')
    
    if not task_id:
        await message.reply_text(
            "No active task found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    context.user_data['cancelling_task'] = task_id
    
    sent_msg = await message.reply_text(
        f"Are you sure you want to cancel this task?\n\n"
        f"❌ The task will be returned to available tasks\n"
        f"❌ You will lose this task assignment\n"
        f"✅ You can take it again later",
        reply_markup=get_cancel_confirmation_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_my_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    active_tasks = get_user_active_tasks(user.id)
    completed_tasks = get_user_completed_tasks(user.id)
    
    if not active_tasks and not completed_tasks:
        await message.reply_text(
            "📭 You don't have any tasks yet.\n"
            "Start earning by clicking 'Take Task'!",
            reply_markup=get_main_menu(user.id)
        )
        return
        
    pending_tasks = [t for t in completed_tasks if t['status'] == 'pending']
    approved_tasks = [t for t in completed_tasks if t['status'] == 'approved']
    rejected_tasks = [t for t in completed_tasks if t['status'] == 'rejected']
    
    total_earned = sum(t['reward'] for t in approved_tasks)
    pending_earnings = sum(t['reward'] for t in pending_tasks)
    
    response = (
        f"📝 My Tasks\n\n"
        f"⏳ Active Tasks: {len(active_tasks)}/{MAX_ACTIVE_TASKS}\n"
        f"✅ Approved: {len(approved_tasks)} tasks\n"
        f"⏳ Pending: {len(pending_tasks)} tasks\n"
        f"❌ Rejected: {len(rejected_tasks)} tasks\n\n"
        f"💰 Total Earned: ETB{total_earned:.2f}\n"
        f"⏰ Pending: ETB{pending_earnings:.2f}\n\n"
    )
    
    if active_tasks:
        response += "📋 <b>Your Active Task:</b>\n"
        for task in active_tasks:
            status_text = "⏳ Waiting for OTP" if task['status'] == 'pending_otp' else "📋 Assigned"
            response += f"• 📧 <code>{task['address']}</code>@gmail.com - ETB{task['reward']:.2f} ({status_text})\n"
            
            if task['status'] == 'assigned':
                keyboard = [
                    [
                        InlineKeyboardButton(f"✅ Complete", callback_data=f"complete_task_{task['task_id']}"),
                        InlineKeyboardButton(f"❌ Cancel", callback_data=f"cancel_task_{task['task_id']}")
                    ],
                    [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
                ]
                sent_msg = await message.reply_text(
                    response,
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                await queue_message_for_deletion(user.id, sent_msg.message_id)
                return
            elif task['status'] == 'pending_otp':
                response += "\n⚠️ You have a task waiting for OTP verification.\n"
                response += "Please enter the code sent to your email to complete the task."
                sent_msg = await message.reply_text(
                    response,
                    parse_mode=ParseMode.HTML,
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
                await queue_message_for_deletion(user.id, sent_msg.message_id)
                return
            
    if pending_tasks:
        response += "\n⏳ <b>Pending Approval:</b>\n"
        for task in pending_tasks[:5]:
            response += f"• 📧 <code>{task['address']}</code>@gmail.com - ETB{task['reward']:.2f}\n"
            
    if approved_tasks:
        response += "\n✅ <b>Approved Tasks:</b>\n"
        for task in approved_tasks[:5]:
            response += f"• 📧 <code>{task['address']}</code>@gmail.com - ETB{task['reward']:.2f}\n"
    
    if len(approved_tasks) > 5:
        response += f"\n... and {len(approved_tasks) - 5} more approved tasks\n"
    
    if not active_tasks:
        response += "\n✅ You have no active tasks! Click 'Take Task' to start earning."
    
    response += "\n\n💡 Click on the email username to copy it!"
    
    sent_msg = await message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_my_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    user_data = get_user(user.id)
    
    if not user_data:
        await message.reply_text(
            "User not found.",
            reply_markup=get_main_menu(user.id)
        )
        return
        
    settings = get_bonus_settings()
    
    response = (
        f"💰 My Balance\n\n"
        f"💵 Available Balance: ETB{user_data['balance']:.2f}\n"
        f"⏳ Hold Balance: ETB{user_data['hold_balance']:.2f}\n"
        f"📊 Total Earned: ETB{user_data['total_earned']:.2f}\n\n"
        f"✅ Tasks Completed: {user_data['tasks_completed']}\n"
        f"👥 Referrals: {user_data['referral_count']}\n"
        f"📈 Referral Earnings: ETB{user_data['referred_earnings']:.2f}\n\n"
        f"💸 Minimum Withdrawal: ETB{settings['min_withdrawal']:.2f}\n\n"
        f"Complete more tasks to earn more! 🚀"
    )
    
    sent_msg = await message.reply_text(
        response,
        reply_markup=get_main_menu(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_my_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    user_data = get_user(user.id)
    
    if not user_data:
        await message.reply_text(
            "User not found.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    referral_code = str(user.id)
    referral_count = user_data['referral_count']
    
    settings = get_bonus_settings()
    
    bot_username = (await context.bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start={referral_code}"
    
    response = (
        f"👥 My Referrals\n\n"
        f"🔗 Your Referral Link:\n<code>{referral_link}</code>\n\n"
        f"📊 Referral Stats:\n"
        f"✅ Total Referrals: {referral_count}\n"
    )
    
    milestone_settings = get_milestone_settings()
    if milestone_settings['is_enabled']:
        milestones = get_milestones()
        if milestones:
            next_milestone = None
            progress = 0
            
            for milestone in milestones:
                if referral_count < milestone['referrals']:
                    next_milestone = milestone
                    progress = (referral_count / milestone['referrals']) * 100
                    break
            
            if not next_milestone and milestones:
                next_milestone = milestones[-1]
                progress = 100
            
            response += f"🎯 Next Milestone: {next_milestone['referrals']} referrals\n"
            response += f"📈 Progress: {progress:.1f}%\n"
            response += f"{'█' * int(progress/10)}{'░' * (10 - int(progress/10))}\n\n"
            response += "🎁 Milestone Bonuses:\n"
            for milestone in milestones:
                status = "✅ Achieved" if referral_count >= milestone['referrals'] else "⏳ Pending"
                response += f"  {milestone['referrals']} referrals → ETB{milestone['bonus']:.2f} {status}\n"
            response += "\n"
    
    response += f"💰 Earn ETB{settings['referral_bonus']:.2f} for each successful referral!\n"
    if settings.get('referral_percentage', 5.0) > 0:
        response += f"📊 Plus {settings['referral_percentage']}% of your referrals' task earnings!\n\n"
    else:
        response += "\n"
    response += f"💡 Click on the link above to copy it!"
    
    sent_msg = await message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

# ==================== PAYOUT HANDLERS ====================
async def handle_request_payout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    user_data = get_user(user.id)
    settings = get_bonus_settings()
    
    methods = get_payment_methods(user.id)
    has_telebirr = methods and methods.get('telebirr_name') and methods.get('telebirr_phone')
    has_binance = methods and methods.get('binance_id')
    has_cbe = methods and methods.get('cbe_account')
    
    if not has_telebirr and not has_binance and not has_cbe:
        await message.reply_text(
            f"⚠️ <b>No Payment Method Saved!</b>\n\n"
            f"Before requesting a payout, please save a payment method first.\n\n"
            f"📱 <b>Available Methods:</b>\n"
            f"• Telebirr (Name + Phone)\n"
            f"• Binance ID (Converted to USD)\n"
            f"• CBE (Name + Account Number)\n\n"
            f"Click <b>Settings</b> → <b>Payment Methods</b> to add your preferred method.\n\n"
            f"💰 Minimum Withdrawal: ETB{settings['min_withdrawal']:.2f}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu(user.id)
        )
        return
    
    if user_data['balance'] < settings['min_withdrawal']:
        await message.reply_text(
            f"❌ <b>Insufficient Balance</b>\n\n"
            f"Your balance: <b>ETB{user_data['balance']:.2f}</b>\n"
            f"Minimum withdrawal: <b>ETB{settings['min_withdrawal']:.2f}</b>\n\n"
            f"📊 Need: ETB{settings['min_withdrawal'] - user_data['balance']:.2f} more\n\n"
            f"Complete more tasks to reach the minimum! 🚀",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu(user.id)
        )
        return
    
    await message.reply_text(
        f"💸 <b>Request Payout</b>\n\n"
        f"💰 Available Balance: <b>ETB{user_data['balance']:.2f}</b>\n"
        f"📊 Minimum: <b>ETB{settings['min_withdrawal']:.2f}</b>\n\n"
        f"Select your saved payment method:",
        parse_mode=ParseMode.HTML,
        reply_markup=get_payout_methods_menu(has_telebirr, has_binance, has_cbe)
    )

async def handle_use_saved_payout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    methods = get_payment_methods(user.id)
    text = message.text
    
    if "Telebirr" in text:
        if not methods or not methods.get('telebirr_name'):
            await message.reply_text(
                "❌ No saved Telebirr details found.",
                reply_markup=get_main_menu(user.id)
            )
            return
            
        context.user_data['payout_method'] = 'telebirr'
        context.user_data['payout_details'] = f"{methods['telebirr_name']} - {methods['telebirr_phone']}"
        
    elif "Binance" in text:
        if not methods or not methods.get('binance_id'):
            await message.reply_text(
                "❌ No saved Binance ID found.",
                reply_markup=get_main_menu(user.id)
            )
            return
            
        context.user_data['payout_method'] = 'binance'
        context.user_data['payout_details'] = f"Binance ID: {methods['binance_id']}"
        
    elif "CBE" in text:
        if not methods or not methods.get('cbe_account'):
            await message.reply_text(
                "❌ No saved CBE account found.",
                reply_markup=get_main_menu(user.id)
            )
            return
            
        context.user_data['payout_method'] = 'cbe'
        context.user_data['payout_details'] = f"{methods['cbe_name']} - {methods['cbe_account']}"
    else:
        return
        
    context.user_data['awaiting_payout_amount'] = True
    settings = get_bonus_settings()
    
    await message.reply_text(
        f"✅ Using saved {context.user_data['payout_method'].upper()} method\n\n"
        f"Enter amount to withdraw (minimum ETB{settings['min_withdrawal']:.2f}):",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

# ==================== SETTINGS HANDLERS ====================
async def handle_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚙️ Settings\n\n"
        "Select an option:",
        reply_markup=get_settings_menu()
    )

async def handle_payment_methods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔧 Payment Methods\n\n"
        "Select an option:",
        reply_markup=get_payment_methods_menu()
    )

async def handle_contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    contact_admin = get_system_setting('contact_admin', OWNER_USERNAME)
    await update.message.reply_text(
        f"📞 Contact Admin\n\n"
        f"For support or questions:\n\n"
        f"👑 Admin: {contact_admin}\n"
        f"⏰ Response time: Usually within hours\n\n"
        f"Please describe your issue clearly when contacting.",
        reply_markup=get_settings_menu()
    )

async def handle_account_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_data = get_user(user.id)
    
    if not user_data:
        await update.message.reply_text(
            "User not found.",
            reply_markup=get_settings_menu()
        )
        return
        
    methods = get_payment_methods(user.id)
    
    response = (
        f"📊 Account Information\n\n"
        f"👤 User ID: <code>{user.id}</code>\n"
        f"📛 Username: @{user_data['username'] or 'N/A'}\n"
        f"👋 Name: <code>{user_data['first_name']} {user_data['last_name'] or ''}</code>\n"
        f"📅 Registered: {user_data['registered_date'].split()[0]}\n"
        f"🕒 Last Active: {user_data['last_active'].split()[0]}\n\n"
    )
    
    if methods:
        response += "💳 Payment Methods:\n"
        if methods.get('telebirr_name'):
            response += f"📱 Telebirr: <code>{methods['telebirr_name']} ({methods['telebirr_phone']})</code>\n"
        if methods.get('binance_id'):
            response += f"🪙 Binance ID: <code>{methods['binance_id']}</code>\n"
        if methods.get('cbe_account'):
            response += f"🏦 CBE: <code>{methods['cbe_name']} ({methods['cbe_account']})</code>\n"
        response += f"⚙️ Default: {methods['default_payment_method'].upper()}\n"
    else:
        response += "💳 Payment Methods: Not setup\n"
        
    response += "\n💡 Click on payment details to copy them!"
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_settings_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_change_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    methods = get_payment_methods(user.id)
    
    if not methods:
        await update.message.reply_text(
            "No payment methods saved yet.\n"
            "Please setup payment methods first.",
            reply_markup=get_settings_menu()
        )
        return
        
    keyboard = []
    if methods.get('telebirr_name'):
        keyboard.append([InlineKeyboardButton("📱 Set Telebirr as Default", callback_data="set_default_telebirr")])
    if methods.get('binance_id'):
        keyboard.append([InlineKeyboardButton("🪙 Set Binance as Default", callback_data="set_default_binance")])
    if methods.get('cbe_account'):
        keyboard.append([InlineKeyboardButton("🏦 Set CBE as Default", callback_data="set_default_cbe")])
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    await update.message.reply_text(
        "🔄 Change Default Payment Method\n\n"
        "Select which method to use as default:",
        reply_markup=reply_markup
    )

async def handle_view_saved_methods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    methods = get_payment_methods(user.id)
    
    if not methods or (not methods.get('telebirr_name') and not methods.get('binance_id') and not methods.get('cbe_account')):
        await update.message.reply_text(
            "📭 No payment methods saved.\n"
            "Please setup payment methods first.",
            reply_markup=get_payment_methods_menu()
        )
        return
        
    response = "📋 Saved Payment Methods\n\n"
    
    if methods.get('telebirr_name'):
        response += (
            f"📱 Telebirr:\n"
            f"  👤 Name: <code>{methods['telebirr_name']}</code>\n"
            f"  📞 Phone: <code>{methods['telebirr_phone']}</code>\n\n"
        )
        
    if methods.get('binance_id'):
        response += (
            f"🪙 Binance:\n"
            f"  🔗 ID: <code>{methods['binance_id']}</code>\n\n"
        )
        
    if methods.get('cbe_account'):
        response += (
            f"🏦 CBE:\n"
            f"  👤 Name: <code>{methods['cbe_name']}</code>\n"
            f"  🔗 Account: <code>{methods['cbe_account']}</code>\n\n"
        )
        
    response += f"⚙️ Default Method: {methods['default_payment_method'].upper()}\n\n"
    response += f"💡 Click on the data above to copy it!"
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_payment_methods_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_clear_methods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    Database.execute('''
    UPDATE users 
    SET telebirr_name = NULL, telebirr_phone = NULL, binance_id = NULL, cbe_name = NULL, cbe_account = NULL
    WHERE user_id = ?
    ''', (user.id,))
    
    await update.message.reply_text(
        "✅ All payment methods cleared.",
        reply_markup=get_payment_methods_menu()
    )

# ==================== ADMIN HANDLERS ====================
async def handle_admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text(
            "❌ You are not authorized to use this command!",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    stats = get_task_statistics()
    settings = get_bonus_settings()
    milestone_settings = get_milestone_settings()
    
    total_users = Database.fetchone('SELECT COUNT(*) as count FROM users')[0]
    online_users = get_online_users_count(1)
    
    pending_payouts = Database.fetchone('SELECT COUNT(*) as count FROM payouts WHERE status = "pending"')[0]
    total_payouts = Database.fetchone('SELECT SUM(amount) as total FROM payouts WHERE status = "approved"')[0] or 0
    
    expiry_hours = get_task_expiry_hours()
    email_accounts = Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0] or 0
    
    otp_status = "✅ ON" if is_otp_required() else "❌ OFF"
    payout_channel = get_payout_channel()
    
    response = (
        f"👑 Admin Dashboard\n\n"
        f"📊 User Statistics:\n"
        f"👥 Total Users: {total_users}\n"
        f"✅ Online Users (1h): {online_users}\n\n"
        f"📋 Task Statistics:\n"
        f"📭 Available: {stats.get('available_tasks', 0)}\n"
        f"⏳ Assigned: {stats.get('assigned_tasks', 0)}\n"
        f"⏳ Waiting OTP: {stats.get('pending_otp_tasks', 0)}\n"
        f"⏳ Pending Review: {stats.get('pending_tasks', 0)}\n"
        f"✅ Approved: {stats.get('approved_tasks', 0)}\n"
        f"❌ Rejected: {stats.get('rejected_tasks', 0)}\n"
        f"💀 Expired: {stats.get('expired_tasks', 0)}\n"
        f"📊 Total: {stats.get('total_tasks', 0)}\n\n"
        f"💰 Financial Statistics:\n"
        f"💸 Pending Payouts: {pending_payouts}\n"
        f"✅ Total Paid: ETB{total_payouts:.2f}\n\n"
        f"📧 Email Accounts Active: {email_accounts}\n"
        f"🎯 Milestone Bonuses: {'✅ Enabled' if milestone_settings['is_enabled'] else '❌ Disabled'}\n"
        f"🔐 OTP Verification: {otp_status}\n"
        f"📢 Payout Channel: {payout_channel}\n\n"
        f"⚙️ Current Settings:\n"
        f"💰 Min Withdrawal: ETB{settings['min_withdrawal']:.2f}\n"
        f"👥 Referral Bonus: ETB{settings['referral_bonus']:.2f}\n"
        f"📊 Referral Percentage: {settings['referral_percentage']}%\n"
        f"✅ Task Reward: ETB{settings['task_reward']:.2f}\n"
        f"⏰ Task Expiry: {expiry_hours} hours\n\n"
        f"🕒 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    
    sent_msg = await update.message.reply_text(
        response,
        reply_markup=get_admin_menu_by_permissions(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_pending_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show pending tasks with approve/reject buttons"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text(
            "❌ You are not authorized to use this command!",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    tasks = get_pending_tasks()
    
    if not tasks:
        await update.message.reply_text(
            "📭 No pending tasks for review.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
        
    context.user_data['pending_tasks'] = [t['task_id'] for t in tasks]
    context.user_data['pending_index'] = 0
    
    await show_pending_task_details(update, context, 0)

async def show_pending_task_details(update: Update, context: ContextTypes.DEFAULT_TYPE, index: int):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    if 'pending_tasks' not in context.user_data or index >= len(context.user_data['pending_tasks']):
        await update.message.reply_text(
            "No more pending tasks.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
        
    task_id = context.user_data['pending_tasks'][index]
    task = Database.fetchone('''
    SELECT t.*, u.username, u.user_id 
    FROM tasks t
    LEFT JOIN users u ON t.assigned_to = u.user_id
    WHERE t.task_id = ?
    ''', (task_id,))
    
    if not task:
        await update.message.reply_text(
            "Task not found.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
        
    task = dict(task)
    
    response = (
        f"⏳ Pending Task Review\n\n"
        f"📋 Task ID: #{task['task_id']}\n"
        f"👤 Completed By: @{task['username']} (ID: <code>{task['assigned_to']}</code>)\n"
        f"⏰ Completed: {task['completed_time']}\n\n"
        f"📝 Task Details:\n"
        f"👤 Name: <code>{task['name']}</code>\n"
    )
    
    if task['father_name'] and task['father_name'].strip():
        response += f"👨 Father Name: <code>{task['father_name']}</code>\n"
    
    response += (
        f"📧 Email: <code>{task['address']}</code>@gmail.com\n"
        f"🔐 Password: <code>{task['password']}</code>\n\n"
        f"💰 Reward: ETB{task['reward']:.2f}\n\n"
        f"Approve or reject this task:"
    )
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_task_review_menu(task['task_id'])
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_admin_task_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if "Approve" in text:
        task_id = int(text.split('#')[1])
        await handle_task_approve(update, context, task_id)
    else:
        task_id = int(text.split('#')[1])
        await handle_task_reject(update, context, task_id)

async def handle_task_approve(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    user = update.effective_user
    
    default_message = get_system_setting("task_approval_message", 
                                       "✅ Task approved! ETB{reward} added to your balance.")
    
    task = Database.fetchone('SELECT assigned_to, reward FROM tasks WHERE task_id = ?', (task_id,))
    if task:
        task_dict = dict(task)
        reward = task_dict['reward']
        message = default_message.replace("ETB{reward}", f"ETB{reward:.2f}")
        
        success = approve_task(task_id, user.id)
        if success:
            await send_user_notification(context, task_dict['assigned_to'], message)
            sent_msg = await update.message.reply_text(
                f"✅ Task #{task_id} approved!",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
        else:
            await update.message.reply_text(
                f"❌ Failed to approve task #{task_id}",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )

async def handle_task_reject(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    user = update.effective_user
    
    default_message = get_system_setting("task_rejection_message",
                                       "❌ Task rejected. Please check requirements and try again.")
    
    task = Database.fetchone('SELECT assigned_to FROM tasks WHERE task_id = ?', (task_id,))
    if task:
        task_dict = dict(task)
        
        success = reject_task(task_id, user.id)
        if success:
            await send_user_notification(context, task_dict['assigned_to'], default_message)
            sent_msg = await update.message.reply_text(
                f"❌ Task #{task_id} rejected!",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
        else:
            await update.message.reply_text(
                f"❌ Failed to reject task #{task_id}",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )

async def handle_next_pending_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    if 'pending_index' not in context.user_data:
        context.user_data['pending_index'] = 0
    else:
        context.user_data['pending_index'] += 1
        
    await show_pending_task_details(update, context, context.user_data['pending_index'])

async def handle_manage_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text(
            "❌ You are not authorized to use this command!",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    await update.message.reply_text(
        "👥 User Management\n\n"
        "Select an option:",
        reply_markup=get_admin_user_management_menu()
    )

async def handle_list_recent_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    users = get_recent_users(10)
    
    if not users:
        await update.message.reply_text(
            "📭 No users found.",
            reply_markup=get_admin_user_management_menu()
        )
        return
        
    response = "📋 Recent Users (Last 10)\n\n"
    
    for i, user_data in enumerate(users, 1):
        admin_status_icon = "👑" if user_data['is_admin'] else "👤"
        response += (
            f"{i}. {admin_status_icon} ID: <code>{user_data['user_id']}</code>\n"
            f"   👤 @{user_data['username'] or 'N/A'}\n"
            f"   📛 <code>{user_data['first_name']}</code>\n"
            f"   📅 Joined: {user_data['registered_date'].split()[0]}\n\n"
        )
        
    response += "Click on user ID buttons below to manage:\n💡 Click on data to copy!"
    
    keyboard = []
    for user_data in users:
        keyboard.append([
            InlineKeyboardButton(
                f"👤 {user_data['user_id']} - {user_data['first_name']}",
                callback_data=f"manage_user_{user_data['user_id']}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton("🔙 Back", callback_data="back_to_user_management")
    ])
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_view_online_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    online_1h = get_online_users_count(1)
    online_24h = get_online_users_count(24)
    total_users = Database.fetchone('SELECT COUNT(*) as count FROM users')[0]
    
    response = (
        f"📊 Online Users\n\n"
        f"👥 Total Users: {total_users}\n"
        f"✅ Online (Last 1 hour): {online_1h}\n"
        f"📅 Active (Last 24 hours): {online_24h}\n\n"
        f"🕒 Updated: {datetime.now().strftime('%H:%M')}"
    )
    
    sent_msg = await update.message.reply_text(
        response,
        reply_markup=get_admin_user_management_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_admin_view_user_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    text = update.message.text
    try:
        user_id = int(text.split('#')[1])
    except:
        await update.message.reply_text(
            "Invalid user ID.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
        
    stats = get_user_statistics(user_id)
    
    if not stats:
        await update.message.reply_text(
            "User not found.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
        
    user_data = stats['user']
    transactions = stats['transactions']
    
    response = (
        f"📊 User Details\n\n"
        f"👤 User ID: <code>{user_id}</code>\n"
        f"📛 Username: @{user_data['username'] or 'N/A'}\n"
        f"👋 Name: <code>{user_data['first_name']} {user_data['last_name'] or ''}</code>\n"
        f"📅 Registered: {user_data['registered_date'].split()[0]}\n"
        f"🕒 Last Active: {user_data['last_active'].split()[0]}\n"
        f"👑 Admin Status: {'Yes' if user_data['is_admin'] else 'No'}\n\n"
        f"💰 Financial:\n"
        f"💵 Balance: ETB{user_data['balance']:.2f}\n"
        f"⏳ Hold Balance: ETB{user_data['hold_balance']:.2f}\n"
        f"📊 Total Earned: ETB{user_data['total_earned']:.2f}\n"
        f"📈 Referral Earnings: ETB{user_data['referred_earnings']:.2f}\n\n"
        f"📋 Tasks:\n"
        f"✅ Completed: {user_data['tasks_completed']}\n"
        f"⏳ Active: {stats.get('active_tasks', 0)}\n\n"
        f"👥 Referrals:\n"
        f"📊 Count: {user_data['referral_count']}\n\n"
    )
    
    if transactions:
        response += "💸 Transaction Summary:\n"
        for trans_type, total in transactions.items():
            response += f"  {trans_type}: ETB{total:.2f}\n"
    
    sent_msg = await update.message.reply_text(
        response,
        reply_markup=get_admin_user_detail_menu(user_id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_admin_balance_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    text = update.message.text
    
    if "Add" in text:
        user_id = int(text.split('#')[1])
        action = 'add'
    else:
        user_id = int(text.split('#')[1])
        action = 'subtract'
    
    user_data = get_user(user_id)
    if not user_data:
        await update.message.reply_text(
            "User not found.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    context.user_data['balance_user_id'] = user_id
    context.user_data['balance_action'] = action
    
    await update.message.reply_text(
        f"Adjust Balance for User #{user_id}\n"
        f"👤 @{user_data['username'] or 'N/A'}\n"
        f"💰 Current Balance: ETB{user_data['balance']:.2f}\n\n"
        f"Enter amount to {action}:\n"
        f"(Also enter reason after amount, separated by comma)\n\n"
        f"Example: 10.00, Bonus for good work",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )
    
    context.user_data['awaiting_balance_amount'] = True

async def handle_admin_message_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    text = update.message.text
    try:
        user_id = int(text.split('#')[1])
    except:
        await update.message.reply_text(
            "Invalid user ID.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
        
    user_data = get_user(user_id)
    if not user_data:
        await update.message.reply_text(
            "User not found.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    context.user_data['message_user_id'] = user_id
    context.user_data['awaiting_user_message'] = True
    
    await update.message.reply_text(
        f"📨 Message User #{user_id}\n"
        f"👤 @{user_data['username'] or 'N/A'}\n\n"
        f"Enter message to send:",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text(
            "❌ You are not authorized to use this command!",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    total_users = Database.fetchone('SELECT COUNT(*) as count FROM users')[0]
    active_today = Database.fetchone('''
    SELECT COUNT(*) as count FROM users 
    WHERE DATE(last_active) = DATE('now')
    ''')[0]
    
    total_tasks = Database.fetchone('SELECT COUNT(*) as count FROM tasks')[0]
    completed_tasks = Database.fetchone('SELECT COUNT(*) as count FROM tasks WHERE status = "approved"')[0]
    
    total_earned = Database.fetchone('SELECT SUM(amount) as total FROM transactions WHERE trans_type = "task_completion"')[0] or 0
    total_paid = Database.fetchone('SELECT SUM(amount) as total FROM payouts WHERE status = "approved"')[0] or 0
    total_referral_earnings = Database.fetchone('SELECT SUM(amount) as total FROM transactions WHERE trans_type = "referral_earning"')[0] or 0
    
    task_stats = get_task_statistics()
    email_accounts = Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0] or 0
    
    otp_status = "✅ ON" if is_otp_required() else "❌ OFF"
    
    response = (
        f"📈 System Statistics\n\n"
        f"👥 Users:\n"
        f"📊 Total Users: {total_users}\n"
        f"✅ Active Today: {active_today}\n\n"
        f"📋 Tasks:\n"
        f"📊 Total Tasks: {total_tasks}\n"
        f"✅ Completed: {completed_tasks}\n"
        f"📈 Completion Rate: {(completed_tasks/total_tasks*100 if total_tasks > 0 else 0):.1f}%\n\n"
        f"📊 Task Status:\n"
        f"📭 Available: {task_stats.get('available_tasks', 0)}\n"
        f"⏳ Assigned: {task_stats.get('assigned_tasks', 0)}\n"
        f"⏳ Waiting OTP: {task_stats.get('pending_otp_tasks', 0)}\n"
        f"⏳ Pending: {task_stats.get('pending_tasks', 0)}\n"
        f"✅ Approved: {task_stats.get('approved_tasks', 0)}\n"
        f"❌ Rejected: {task_stats.get('rejected_tasks', 0)}\n"
        f"💀 Expired: {task_stats.get('expired_tasks', 0)}\n\n"
        f"💰 Financial:\n"
        f"💵 Total Earned by Users: ETB{total_earned:.2f}\n"
        f"📈 Referral Earnings Paid: ETB{total_referral_earnings:.2f}\n"
        f"💸 Total Paid Out: ETB{total_paid:.2f}\n"
        f"💼 System Balance: ETB{total_earned - total_paid:.2f}\n\n"
        f"📧 Email Accounts Active: {email_accounts}\n"
        f"🔐 OTP Verification: {otp_status}\n\n"
        f"🕒 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    
    sent_msg = await update.message.reply_text(
        response,
        reply_markup=get_admin_menu_by_permissions(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_update_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    response = (
        "📝 Update System Messages\n\n"
        "Current Messages:\n\n"
        f"✅ Task Approval: {get_system_setting('task_approval_message')}\n\n"
        f"❌ Task Rejection: {get_system_setting('task_rejection_message')}\n\n"
        f"💰 Payout Approved: {get_system_setting('payout_approved_message')}\n\n"
        f"❌ Payout Rejected: {get_system_setting('payout_rejected_message')}\n\n"
        "To update a message, use format:\n"
        "message_type,new_message\n\n"
        "Available message types:\n"
        "- approval (task approval)\n"
        "- rejection (task rejection)\n"
        "- payout_approval (payout approved)\n"
        "- payout_rejection (payout rejected)\n\n"
        "Example:\n"
        "approval,✅ Great job! Your task is approved and ETB{reward} added."
    )
    
    context.user_data['awaiting_message_update'] = True
    
    await update.message.reply_text(
        response,
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    await update.message.reply_text(
        "📢 Broadcast System\n\n"
        "Select an option:",
        reply_markup=get_admin_broadcast_menu()
    )

async def handle_backup_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text(
            "❌ You are not authorized to use this command!",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    backup_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if os.path.exists('bot.py'):
        try:
            with open('bot.py', 'rb') as bot_file:
                await context.bot.send_document(
                    chat_id=user.id,
                    document=bot_file,
                    filename=f"bot_{backup_time}.py",
                    caption=f"💾 Bot Code Backup - {backup_time}\n\nLatest version of bot.py"
                )
        except Exception as e:
            await update.message.reply_text(f"❌ Error sending bot.py: {str(e)}")
    else:
        await update.message.reply_text("❌ bot.py file not found!")
    
    backup_file = f"backup_{backup_time}.db"
    shutil.copy2(DB_FILE, backup_file)
    
    try:
        with open(backup_file, 'rb') as db_file:
            await context.bot.send_document(
                chat_id=user.id,
                document=db_file,
                filename=backup_file,
                caption=f"💾 Database Backup - {backup_time}"
            )
        os.remove(backup_file)
    except Exception as e:
        await update.message.reply_text(f"❌ Error sending database: {str(e)}")
    
    json_file = f"backup_{backup_time}.json"
    backup_data = {
        'backup_time': backup_time,
        'users_count': Database.fetchone('SELECT COUNT(*) as count FROM users')[0],
        'tasks_count': Database.fetchone('SELECT COUNT(*) as count FROM tasks')[0],
        'payouts_count': Database.fetchone('SELECT COUNT(*) as count FROM payouts')[0],
        'transactions_count': Database.fetchone('SELECT COUNT(*) as count FROM transactions')[0],
        'email_accounts_count': Database.fetchone('SELECT COUNT(*) FROM email_accounts')[0]
    }
    
    with open(json_file, 'w') as f:
        json.dump(backup_data, f, default=str)
    
    try:
        with open(json_file, 'rb') as json_f:
            await context.bot.send_document(
                chat_id=user.id,
                document=json_f,
                filename=json_file,
                caption=f"📊 Backup Summary - {backup_time}"
            )
        os.remove(json_file)
    except Exception as e:
        await update.message.reply_text(f"❌ Error sending JSON: {str(e)}")
    
    await update.message.reply_text(
        f"✅ Backup Complete!\n\n"
        f"📁 Files sent:\n"
        f"• bot_{backup_time}.py (Bot Code)\n"
        f"• {backup_file} (Database)\n"
        f"• {json_file} (Summary)\n\n"
        f"💡 All files have been sent to this chat.",
        reply_markup=get_admin_menu_by_permissions(user.id)
    )

async def handle_admin_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    await update.message.reply_text(
        "⚙️ Admin Settings\n\n"
        "Select an option:",
        reply_markup=get_admin_settings_menu()
    )

# ==================== EXPORT TASKS HANDLERS ====================
async def handle_export_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'export' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to export tasks!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    await update.message.reply_text(
        "📥 Export Tasks\n\n"
        "Select an option:",
        reply_markup=get_admin_export_menu()
    )

async def handle_export_completed_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'export' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to export tasks!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    csv_data, count = export_completed_tasks_to_csv()
    
    if count == 0:
        await update.message.reply_text(
            "📭 No completed tasks to export!",
            reply_markup=get_admin_export_menu()
        )
        return
    
    filename = f"completed_tasks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    await context.bot.send_document(
        chat_id=user.id,
        document=csv_data,
        filename=filename,
        caption=f"✅ Completed Tasks Export\n📊 Total: {count} tasks\n📅 Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await update.message.reply_text(
        f"✅ Exported {count} completed tasks!",
        reply_markup=get_admin_export_menu()
    )

async def handle_export_failed_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'export' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to export tasks!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    csv_data, count = export_failed_tasks_to_csv()
    
    if count == 0:
        await update.message.reply_text(
            "📭 No failed tasks to export!",
            reply_markup=get_admin_export_menu()
        )
        return
    
    filename = f"failed_tasks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    await context.bot.send_document(
        chat_id=user.id,
        document=csv_data,
        filename=filename,
        caption=f"❌ Failed Tasks Export\n📊 Total: {count} tasks\n📅 Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await update.message.reply_text(
        f"✅ Exported {count} failed tasks!",
        reply_markup=get_admin_export_menu()
    )

# ==================== DELETE TASKS HANDLERS ====================
async def handle_delete_tasks_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'delete' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to delete tasks!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    await update.message.reply_text(
        "🗑️ Delete Tasks\n\n"
        "Select an option:",
        reply_markup=get_admin_delete_tasks_menu()
    )

async def handle_delete_single_completed_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'delete' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to delete tasks!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    context.user_data['awaiting_delete_single_task'] = True
    await update.message.reply_text(
        "🗑️ Delete Single Completed Task\n\n"
        "Enter the Task ID (unique_task_id) to delete:\n"
        "Example: 18012345\n\n"
        "💡 You can find Task ID in Completed Tasks list.",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_delete_all_completed_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'delete' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to delete tasks!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    count, msg = delete_all_completed_tasks(user.id)
    
    await update.message.reply_text(
        msg,
        reply_markup=get_admin_delete_tasks_menu()
    )

async def handle_delete_all_failed_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'delete' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to delete tasks!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    count, msg = delete_all_failed_tasks(user.id)
    
    await update.message.reply_text(
        msg,
        reply_markup=get_admin_delete_tasks_menu()
    )

async def handle_delete_single_task_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    task_id = message.text.strip()
    
    if 'awaiting_delete_single_task' in context.user_data:
        success, msg = delete_single_completed_task(task_id, user.id)
        
        await message.reply_text(
            msg,
            reply_markup=get_admin_delete_tasks_menu()
        )
        
        del context.user_data['awaiting_delete_single_task']
        return

# ==================== MILESTONE BONUSES HANDLERS ====================
async def handle_milestone_bonuses_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'setting' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to manage milestone bonuses!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    settings = get_milestone_settings()
    milestones = get_milestones()
    
    response = "🎯 <b>Milestone Bonuses Management</b>\n\n"
    response += f"Status: {'✅ ENABLED' if settings['is_enabled'] else '❌ DISABLED'}\n\n"
    
    if milestones:
        response += "📊 <b>Current Milestones:</b>\n"
        for m in milestones:
            response += f"• {m['referrals']} referrals → ETB{m['bonus']:.2f}\n"
    else:
        response += "📭 No milestones configured.\n"
    
    response += "\nUse buttons below to manage milestones:"
    
    await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_milestone_menu()
    )

async def handle_milestone_enable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'setting' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to do this!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    update_milestone_settings(True, user.id)
    
    await broadcast_milestone_update(context, True)
    
    await update.message.reply_text(
        "✅ Milestone bonuses have been ENABLED!\n\n"
        "📢 All users have been notified.",
        reply_markup=get_admin_milestone_menu()
    )

async def handle_milestone_disable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'setting' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to do this!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    update_milestone_settings(False, user.id)
    
    await update.message.reply_text(
        "❌ Milestone bonuses have been DISABLED!\n\n"
        "Users will no longer see milestone bonuses.",
        reply_markup=get_admin_milestone_menu()
    )

async def handle_view_milestones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'setting' not in get_admin_permissions(user.id):
        return
    
    milestones = get_milestones()
    
    if not milestones:
        await update.message.reply_text(
            "📭 No milestones configured.\n\nUse '➕ Add Milestone' to add one.",
            reply_markup=get_admin_milestone_menu()
        )
        return
    
    response = "📋 <b>Milestone Bonuses</b>\n\n"
    for m in milestones:
        response += f"• {m['referrals']} referrals → ETB{m['bonus']:.2f}\n"
    
    response += f"\n📊 Total: {len(milestones)} milestones"
    
    await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_milestone_menu()
    )

async def handle_edit_milestone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'setting' not in get_admin_permissions(user.id):
        return
    
    context.user_data['awaiting_milestone_edit'] = True
    await update.message.reply_text(
        "✏️ Edit Milestone\n\n"
        "Format: referrals,bonus\n\n"
        "Example: 10,15.00\n"
        "(This will update or create milestone for 10 referrals with 15 ETB bonus)",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_add_milestone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'setting' not in get_admin_permissions(user.id):
        return
    
    context.user_data['awaiting_milestone_add'] = True
    await update.message.reply_text(
        "➕ Add Milestone\n\n"
        "Format: referrals,bonus\n\n"
        "Example: 20,25.00\n"
        "(This will add a new milestone: 20 referrals = 25 ETB)",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_remove_milestone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'setting' not in get_admin_permissions(user.id):
        return
    
    milestones = get_milestones()
    
    if not milestones:
        await update.message.reply_text(
            "📭 No milestones to remove.",
            reply_markup=get_admin_milestone_menu()
        )
        return
    
    milestone_list = "\n".join([f"• {m['referrals']} referrals" for m in milestones])
    
    context.user_data['awaiting_milestone_remove'] = True
    await update.message.reply_text(
        f"🗑️ Remove Milestone\n\n"
        f"Current milestones:\n{milestone_list}\n\n"
        f"Enter the number of referrals to remove (e.g., 10):",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_milestone_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    text = message.text
    
    if 'awaiting_milestone_edit' in context.user_data:
        try:
            parts = text.split(',')
            if len(parts) != 2:
                raise ValueError
            referrals = int(parts[0].strip())
            bonus = float(parts[1].strip())
            
            success, msg = update_milestone(referrals, bonus, user.id)
            await message.reply_text(msg, reply_markup=get_admin_milestone_menu())
            
            del context.user_data['awaiting_milestone_edit']
        except ValueError:
            await message.reply_text(
                "❌ Invalid format! Use: referrals,bonus\n\nExample: 10,15.00",
                reply_markup=get_admin_milestone_menu()
            )
        return
    
    elif 'awaiting_milestone_add' in context.user_data:
        try:
            parts = text.split(',')
            if len(parts) != 2:
                raise ValueError
            referrals = int(parts[0].strip())
            bonus = float(parts[1].strip())
            
            existing = Database.fetchone('SELECT id FROM milestones WHERE referrals = ?', (referrals,))
            if existing:
                success, msg = update_milestone(referrals, bonus, user.id)
            else:
                Database.execute('INSERT INTO milestones (referrals, bonus) VALUES (?, ?)', (referrals, bonus))
                msg = f"✅ Milestone added: {referrals} referrals = ETB{bonus:.2f}"
            
            await message.reply_text(msg, reply_markup=get_admin_milestone_menu())
            del context.user_data['awaiting_milestone_add']
        except ValueError:
            await message.reply_text(
                "❌ Invalid format! Use: referrals,bonus\n\nExample: 20,25.00",
                reply_markup=get_admin_milestone_menu()
            )
        return
    
    elif 'awaiting_milestone_remove' in context.user_data:
        try:
            referrals = int(text.strip())
            success, msg = delete_milestone(referrals, user.id)
            await message.reply_text(msg, reply_markup=get_admin_milestone_menu())
            del context.user_data['awaiting_milestone_remove']
        except ValueError:
            await message.reply_text(
                "❌ Invalid number! Please enter a valid number of referrals.",
                reply_markup=get_admin_milestone_menu()
            )
        return

# ==================== REFERRAL SETTINGS HANDLERS ====================
async def handle_referral_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'referral' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to manage referral settings!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    settings = get_bonus_settings()
    milestone_settings = get_milestone_settings()
    
    response = (
        "🎯 <b>Referral Settings</b>\n\n"
        f"💰 Referral Bonus: ETB{settings['referral_bonus']:.2f}\n"
        f"📊 Referral Percentage: {settings['referral_percentage']}%\n"
        f"🎯 Milestone Bonuses: {'✅ ENABLED' if milestone_settings['is_enabled'] else '❌ DISABLED'}\n\n"
        "Use buttons below to manage:"
    )
    
    await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_referral_menu()
    )

async def handle_edit_referral_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'referral' not in get_admin_permissions(user.id):
        return
    
    context.user_data['awaiting_referral_bonus'] = True
    await update.message.reply_text(
        "💰 Edit Referral Bonus\n\n"
        "Enter new referral bonus amount in ETB:\n"
        "Example: 5.00\n\n"
        "💡 This is the bonus given when a user refers someone.",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_edit_referral_percentage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'referral' not in get_admin_permissions(user.id):
        return
    
    context.user_data['awaiting_referral_percentage'] = True
    await update.message.reply_text(
        "📊 Edit Referral Percentage\n\n"
        "Enter new referral percentage (0-100):\n"
        "Example: 5.00\n\n"
        "💡 This is the percentage of referred user's task earnings.",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_broadcast_referral_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'referral' not in get_admin_permissions(user.id):
        return
    
    context.user_data['awaiting_referral_broadcast'] = True
    await update.message.reply_text(
        "📢 Broadcast Referral Update\n\n"
        "Enter message to broadcast to all users about referral changes:\n"
        "(Or type 'default' to use auto-generated message)",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_referral_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    text = message.text.strip()
    
    if 'awaiting_referral_bonus' in context.user_data:
        try:
            new_bonus = float(text)
            if new_bonus < 0:
                await message.reply_text(
                    "❌ Bonus cannot be negative!",
                    reply_markup=get_admin_referral_menu()
                )
                return
            
            old_settings = get_bonus_settings()
            update_bonus_settings(
                old_settings['min_withdrawal'],
                new_bonus,
                old_settings['referral_percentage'],
                old_settings['task_reward']
            )
            
            await message.reply_text(
                f"✅ Referral bonus updated from ETB{old_settings['referral_bonus']:.2f} to ETB{new_bonus:.2f}!",
                reply_markup=get_admin_referral_menu()
            )
            
            del context.user_data['awaiting_referral_bonus']
        except ValueError:
            await message.reply_text(
                "❌ Invalid amount! Please enter a valid number.",
                reply_markup=get_admin_referral_menu()
            )
        return
    
    elif 'awaiting_referral_percentage' in context.user_data:
        try:
            new_percentage = float(text)
            if new_percentage < 0 or new_percentage > 100:
                await message.reply_text(
                    "❌ Percentage must be between 0 and 100!",
                    reply_markup=get_admin_referral_menu()
                )
                return
            
            old_settings = get_bonus_settings()
            update_bonus_settings(
                old_settings['min_withdrawal'],
                old_settings['referral_bonus'],
                new_percentage,
                old_settings['task_reward']
            )
            
            await message.reply_text(
                f"✅ Referral percentage updated from {old_settings['referral_percentage']}% to {new_percentage}%!",
                reply_markup=get_admin_referral_menu()
            )
            
            del context.user_data['awaiting_referral_percentage']
        except ValueError:
            await message.reply_text(
                "❌ Invalid percentage! Please enter a valid number.",
                reply_markup=get_admin_referral_menu()
            )
        return
    
    elif 'awaiting_referral_broadcast' in context.user_data:
        if text.lower() == 'default':
            settings = get_bonus_settings()
            milestone_settings = get_milestone_settings()
            
            message_text = (
                "<b>🎯 REFERRAL SYSTEM UPDATE</b>\n\n"
                f"💰 Referral Bonus: ETB{settings['referral_bonus']:.2f} per referral\n"
                f"📊 Referral Percentage: {settings['referral_percentage']}% of referred user's earnings\n"
            )
            
            if milestone_settings['is_enabled']:
                milestones = get_milestones()
                if milestones:
                    message_text += "\n🎁 Milestone Bonuses:\n"
                    for m in milestones:
                        message_text += f"• {m['referrals']} referrals → ETB{m['bonus']:.2f}\n"
            
            message_text += "\nInvite more friends and earn more rewards! 🚀"
        else:
            message_text = text
        
        sent, failed = await broadcast_message(context, message_text)
        
        await message.reply_text(
            f"📢 Broadcast Complete!\n\n"
            f"✅ Sent to: {sent} users\n"
            f"❌ Failed: {failed} users",
            reply_markup=get_admin_referral_menu()
        )
        
        del context.user_data['awaiting_referral_broadcast']
        return

# ==================== ADMIN LIST HANDLER ====================
async def handle_admin_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text(
            "❌ Only admins can view the admin list!",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    rows = Database.fetchall('''
    SELECT user_id, username, first_name, last_name, registered_date, is_admin, admin_permissions
    FROM users 
    WHERE is_admin = 1
    ORDER BY registered_date ASC
    ''')
    
    if not rows:
        await update.message.reply_text(
            "📋 No admins found.\n\nOnly the owner is an admin.",
            reply_markup=get_admin_settings_menu()
        )
        return
    
    response = "<b>👑 ADMIN LIST</b>\n\n"
    
    for i, admin in enumerate(rows, 1):
        admin_dict = dict(admin)
        user_id = admin_dict['user_id']
        username = admin_dict['username'] or 'N/A'
        name = admin_dict['first_name']
        if admin_dict['last_name']:
            name += f" {admin_dict['last_name']}"
        registered = admin_dict['registered_date'].split()[0]
        permissions = admin_dict['admin_permissions'] or 'dashboard,statistics,broadcast'
        
        perm_display = []
        for p in permissions.split(','):
            if p in PERMISSIONS:
                perm_display.append(PERMISSIONS[p])
            else:
                perm_display.append(p)
        
        owner_mark = " 👑 OWNER" if user_id == OWNER_ID else ""
        
        response += (
            f"{i}. <b>@{username}</b>{owner_mark}\n"
            f"   🆔 ID: <code>{user_id}</code>\n"
            f"   👤 Name: <code>{name}</code>\n"
            f"   📅 Since: {registered}\n"
            f"   🔑 Permissions: {', '.join(perm_display)}\n\n"
        )
    
    response += f"📊 Total Admins: {len(rows)}\n"
    response += "💡 Click on IDs to copy!"
    
    keyboard = []
    for admin in rows[:5]:
        admin_dict = dict(admin)
        user_id = admin_dict['user_id']
        if user_id != OWNER_ID:
            keyboard.append([
                InlineKeyboardButton(
                    f"❌ Remove {user_id}",
                    callback_data=f"remove_admin_{user_id}"
                )
            ])
    
    if keyboard:
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_admin_settings")])
        await update.message.reply_text(
            response,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            response,
            parse_mode=ParseMode.HTML,
            reply_markup=ReplyKeyboardMarkup([["🔙 Back to Admin Settings"]], resize_keyboard=True)
        )

# ==================== SET CONTACT ADMIN ====================
async def handle_set_contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'contact' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to change contact admin!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    context.user_data['awaiting_contact_admin'] = True
    await update.message.reply_text(
        "📞 Set Contact Admin\n\n"
        "Enter the new contact admin username:\n"
        "Example: @newadmin\n\n"
        "This will be shown to users when they click 'Contact Admin'.",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_contact_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    text = message.text.strip()
    
    if 'awaiting_contact_admin' in context.user_data:
        if not text.startswith('@'):
            await message.reply_text(
                "❌ Invalid username! Username must start with @\n\n"
                "Example: @newadmin",
                reply_markup=get_admin_settings_menu()
            )
            return
        
        update_system_setting('contact_admin', text)
        
        await message.reply_text(
            f"✅ Contact admin updated to {text}\n\n"
            f"Users will now see this when they click 'Contact Admin'.",
            reply_markup=get_admin_settings_menu()
        )
        
        del context.user_data['awaiting_contact_admin']

# ==================== EMAIL ACCOUNTS HANDLER ====================
async def handle_email_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'email' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to manage email accounts!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    accounts = get_email_accounts()
    
    if not accounts:
        await update.message.reply_text(
            "📭 No email accounts found.\n\nUse '➕ Add Email Account' to add one.",
            reply_markup=get_admin_email_accounts_menu()
        )
        return
    
    response = "📧 <b>Email Accounts for OTP</b>\n\n"
    for acc in accounts:
        status = "✅ Active" if acc['is_active'] else "❌ Inactive"
        response += f"• <code>{acc['email']}</code> - {status}\n"
    
    response += f"\n📊 Total: {len(accounts)} accounts"
    response += f"\n✅ Active: {Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0]}"
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_email_accounts_menu()
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_add_email_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'email' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to add email accounts!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    context.user_data['awaiting_email_account'] = True
    await update.message.reply_text(
        "➕ Add Email Account\n\n"
        "Format: email,password\n\n"
        "Example: myemail@gmail.com,app_password_here\n\n"
        "⚠️ Use Gmail App Password (not your regular password)\n"
        "🔑 Get app password from: https://myaccount.google.com/apppasswords",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_remove_email_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'email' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to remove email accounts!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    accounts = get_email_accounts()
    if not accounts:
        await update.message.reply_text(
            "📭 No email accounts to remove.",
            reply_markup=get_admin_email_accounts_menu()
        )
        return
    
    context.user_data['awaiting_remove_email'] = True
    
    account_list = "\n".join([f"• {acc['email']}" for acc in accounts])
    await update.message.reply_text(
        f"➖ Remove Email Account\n\n"
        f"Current accounts:\n{account_list}\n\n"
        f"Enter the email address to remove:\n"
        f"⚠️ Cannot remove the last active account!",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def handle_toggle_email_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id) or 'email' not in get_admin_permissions(user.id):
        await update.message.reply_text(
            "❌ You don't have permission to toggle email accounts!",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    accounts = get_email_accounts()
    
    if not accounts:
        await update.message.reply_text(
            "📭 No email accounts found.",
            reply_markup=get_admin_email_accounts_menu()
        )
        return
    
    context.user_data['awaiting_toggle_email'] = True
    
    response = "✅ Enable / ❌ Disable Email Account\n\n"
    for acc in accounts:
        status = "✅ Active" if acc['is_active'] else "❌ Inactive"
        response += f"• <code>{acc['email']}</code> - {status}\n"
    
    response += f"\n⚠️ Cannot disable the last active account!\n\n"
    response += f"Enter the email address to toggle (enable/disable):"
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

async def handle_email_account_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    text = message.text
    
    if 'awaiting_email_account' in context.user_data:
        parts = text.split(',', 1)
        if len(parts) != 2:
            await message.reply_text(
                "❌ Invalid format!\n\n"
                "Use: email,password\n\n"
                "Example: myemail@gmail.com,abcd efgh ijkl mnop",
                reply_markup=get_admin_email_accounts_menu()
            )
            return
        
        email = parts[0].strip()
        password = parts[1].strip()
        
        success, msg = add_email_account(email, password, user.id)
        
        if success:
            await message.reply_text(
                f"✅ {msg}\n\n"
                f"📧 Total active accounts: {Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0]}",
                reply_markup=get_admin_email_accounts_menu()
            )
        else:
            await message.reply_text(
                f"❌ {msg}",
                reply_markup=get_admin_email_accounts_menu()
            )
        
        del context.user_data['awaiting_email_account']
        return
    
    elif 'awaiting_remove_email' in context.user_data:
        email = text.strip()
        
        success, msg = remove_email_account(email, user.id)
        
        if success:
            await message.reply_text(
                f"✅ {msg}\n\n"
                f"📧 Remaining active accounts: {Database.fetchone('SELECT COUNT(*) FROM email_accounts WHERE is_active = 1')[0]}",
                reply_markup=get_admin_email_accounts_menu()
            )
        else:
            await message.reply_text(
                f"❌ {msg}",
                reply_markup=get_admin_email_accounts_menu()
            )
        
        del context.user_data['awaiting_remove_email']
        return
    
    elif 'awaiting_toggle_email' in context.user_data:
        email = text.strip()
        account = Database.fetchone('SELECT is_active FROM email_accounts WHERE email = ?', (email,))
        
        if not account:
            await message.reply_text(
                f"❌ Email account {email} not found!",
                reply_markup=get_admin_email_accounts_menu()
            )
            return
        
        current_status = account['is_active']
        success, msg = toggle_email_account(email, not current_status, user.id)
        
        if success:
            await message.reply_text(
                f"✅ {msg}",
                reply_markup=get_admin_email_accounts_menu()
            )
        else:
            await message.reply_text(
                f"❌ {msg}",
                reply_markup=get_admin_email_accounts_menu()
            )
        
        del context.user_data['awaiting_toggle_email']
        return

# ==================== ADMIN PAYMENT METHODS ====================
async def handle_admin_payment_methods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    await update.message.reply_text(
        "📋 Payment Methods Management\n\n"
        "Manage available payment methods for users:",
        reply_markup=get_admin_payment_methods_menu()
    )

async def handle_view_payment_methods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    methods = get_active_payment_methods()
    
    response = "📋 Active Payment Methods:\n\n"
    for method in methods:
        status = "✅ Active" if method['is_active'] else "❌ Inactive"
        response += f"• {method['method_name'].upper()} - {status}\n"
    
    response += "\nUse buttons below to add/remove methods."
    
    await update.message.reply_text(
        response,
        reply_markup=get_admin_payment_methods_menu()
    )

async def handle_add_payment_method_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    method_name = message.text.strip().lower()
    
    if 'awaiting_add_payment_method' in context.user_data:
        add_payment_method(method_name)
        del context.user_data['awaiting_add_payment_method']
        
        await message.reply_text(
            f"✅ Payment method '{method_name}' added successfully!",
            reply_markup=get_admin_payment_methods_menu()
        )
        return

async def handle_remove_payment_method_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    method_name = message.text.strip().lower()
    
    if 'awaiting_remove_payment_method' in context.user_data:
        remove_payment_method(method_name)
        del context.user_data['awaiting_remove_payment_method']
        
        await message.reply_text(
            f"✅ Payment method '{method_name}' removed successfully!",
            reply_markup=get_admin_payment_methods_menu()
        )
        return

# ==================== ADMIN CHANNELS ====================
async def handle_admin_channels_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    await update.message.reply_text(
        "📢 Channel Management\n\n"
        "Manage mandatory channels:",
        reply_markup=get_admin_channels_menu()
    )

async def handle_view_all_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    channels = get_all_channels()
    
    if not channels:
        await update.message.reply_text(
            "📭 No channels found.",
            reply_markup=get_admin_channels_menu()
        )
        return
    
    response = "📋 All Channels\n\n"
    for channel in channels:
        status = "✅ Active" if channel['is_active'] else "❌ Inactive"
        response += f"• <code>{channel['channel_username']}</code> - {status}\n"
    
    response += f"\n📊 Total: {len(channels)} channels"
    
    await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_channels_menu()
    )

async def handle_add_channel_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    channel = message.text.strip()
    
    if 'awaiting_add_channel' in context.user_data:
        success, msg = add_channel(channel, user.id)
        del context.user_data['awaiting_add_channel']
        
        if success:
            await message.reply_text(
                f"✅ Channel {channel} added successfully!",
                reply_markup=get_admin_channels_menu()
            )
        else:
            await message.reply_text(
                f"❌ {msg}",
                reply_markup=get_admin_channels_menu()
            )
        return

async def handle_remove_channel_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    channel = message.text.strip()
    
    if 'awaiting_remove_channel' in context.user_data:
        success, msg = remove_channel(channel)
        del context.user_data['awaiting_remove_channel']
        
        await message.reply_text(
            msg,
            reply_markup=get_admin_channels_menu()
        )
        return

# ==================== COMPLETED TASKS VIEW ====================
async def handle_completed_tasks_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    page = context.user_data.get('completed_tasks_page', 1)
    tasks, current_page, total_pages, total = get_approved_tasks_paginated(page)
    
    if not tasks:
        await update.message.reply_text(
            "📭 No completed tasks found.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    response = f"✅ Completed Tasks (Page {current_page}/{total_pages})\n\n"
    
    for i, task in enumerate(tasks, 1):
        email = f"{task['address']}@gmail.com"
        response += f"{i}. <code>{email}</code> - <code>{task['unique_task_id']}</code>\n"
    
    response += f"\n💡 Click on email or ID to copy!\n"
    response += f"📊 Total: {total} tasks"
    
    keyboard = []
    
    if current_page > 1:
        keyboard.append([InlineKeyboardButton("◀️ Previous Page", callback_data=f"completed_page_{current_page-1}")])
    
    if current_page < total_pages:
        keyboard.append([InlineKeyboardButton("▶️ Next Page", callback_data=f"completed_page_{current_page+1}")])
    
    keyboard.append([InlineKeyboardButton("🔍 Task Info", callback_data="task_info_search")])
    keyboard.append([InlineKeyboardButton("🏠 Admin Menu", callback_data="back_to_admin")])
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

# ==================== PENDING PAYOUTS ====================
async def handle_pending_payouts_modified(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    rows = get_pending_payouts()
    
    if not rows:
        await update.message.reply_text(
            "📭 No pending payout requests.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    response = "<b>💰 Pending Payouts</b>\n\n"
    
    for i, payout in enumerate(rows[:10], 1):
        payout_dict = dict(payout)
        
        if payout_dict['payout_method'] == 'telebirr':
            if payout_dict['new_payout_details']:
                details = payout_dict['new_payout_details']
                method_display = "📱 Telebirr (New)"
            else:
                details = f"{payout_dict['telebirr_name'] or 'N/A'} ({payout_dict['telebirr_phone'] or 'N/A'})"
                method_display = "📱 Telebirr (Saved)"
        elif payout_dict['payout_method'] == 'binance':
            if payout_dict['new_payout_details']:
                details = payout_dict['new_payout_details']
                method_display = "🪙 Binance (New)"
            else:
                details = payout_dict['binance_id'] or 'N/A'
                method_display = "🪙 Binance (Saved)"
        elif payout_dict['payout_method'] == 'cbe':
            if payout_dict['new_payout_details']:
                details = payout_dict['new_payout_details']
                method_display = "🏦 CBE (New)"
            else:
                details = f"{payout_dict['cbe_name'] or 'N/A'} ({payout_dict['cbe_account'] or 'N/A'})"
                method_display = "🏦 CBE (Saved)"
        else:
            details = payout_dict['payout_details']
            method_display = payout_dict['payout_method'].upper()
        
        response += (
            f"{i}. <b>Payout ID:</b> #{payout_dict['payout_id']}\n"
            f"   👤 <b>User ID:</b> <code>{payout_dict['user_id']}</code>\n"
            f"   👤 <b>Username:</b> @{payout_dict['username']} (<code>{payout_dict['first_name']}</code>)\n"
            f"   💰 <b>Amount:</b> ETB{payout_dict['amount']:.2f}\n"
            f"   📋 <b>Method:</b> {method_display}\n"
            f"   📝 <b>Details:</b> <code>{details}</code>\n"
            f"   ⏰ <b>Requested:</b> {payout_dict['request_time'].split()[0]}\n\n"
        )
    
    response += "Click buttons below to approve or reject:\n"
    response += "💡 <i>Click on any data to copy it!</i>"
    
    keyboard = []
    for payout in rows[:5]:
        keyboard.append([
            InlineKeyboardButton(f"✅ Approve #{payout['payout_id']}", 
                               callback_data=f"approve_payout_{payout['payout_id']}"),
            InlineKeyboardButton(f"❌ Reject #{payout['payout_id']}", 
                               callback_data=f"reject_payout_{payout['payout_id']}")
        ])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_admin")])
    
    sent_msg = await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

# ==================== PAYOUT APPROVAL HELPERS ====================
async def handle_payout_default_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    if 'approving_payout' in context.user_data:
        payout_id = context.user_data['approving_payout']
        default_message = get_system_setting("payout_approved_message",
                                           "✅ Payout approved! ETB{amount} sent to your account.")
        
        payout = Database.fetchone('SELECT user_id, amount, payout_method, payout_details FROM payouts WHERE payout_id = ?', (payout_id,))
        if payout:
            payout_dict = dict(payout)
            amount = payout_dict['amount']
            message_text = default_message.replace("ETB{amount}", f"ETB{amount:.2f}")
            
            success = process_payout(payout_id, True, user.id)
            if success:
                admin_user = get_user(user.id)
                admin_name = f"{admin_user['first_name']} (@{admin_user['username'] or 'N/A'})" if admin_user else f"Admin #{user.id}"
                
                await send_user_notification(context, payout_dict['user_id'], message_text)
                await post_payout_to_channel(context, payout_dict['user_id'], amount, payout_dict['payout_method'], payout_dict['payout_details'], admin_name=admin_name)
                sent_msg = await update.message.reply_text(
                    f"✅ Payout #{payout_id} approved! Posted to channel.",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
                await queue_message_for_deletion(user.id, sent_msg.message_id)
            else:
                await update.message.reply_text(
                    f"❌ Failed to approve payout #{payout_id}",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
        
        del context.user_data['approving_payout']

async def handle_payout_image_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    if 'approving_payout' in context.user_data:
        payout_id = context.user_data['approving_payout']
        context.user_data['awaiting_payout_image'] = True
        context.user_data['payout_image_id'] = payout_id
        
        await update.message.reply_text(
            f"📸 Send Image for Payout #{payout_id}\n\n"
            f"Please send an image (photo) as proof of payment:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return

async def handle_payout_default_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    if 'rejecting_payout' in context.user_data:
        payout_id = context.user_data['rejecting_payout']
        default_message = get_system_setting("payout_rejected_message",
                                           "❌ Payout rejected. Funds returned to balance.")
        
        payout = Database.fetchone('SELECT user_id, amount FROM payouts WHERE payout_id = ?', (payout_id,))
        if payout:
            payout_dict = dict(payout)
            
            success = process_payout(payout_id, False, user.id)
            if success:
                await send_user_notification(context, payout_dict['user_id'], default_message)
                sent_msg = await update.message.reply_text(
                    f"❌ Payout #{payout_id} rejected!",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
                await queue_message_for_deletion(user.id, sent_msg.message_id)
            else:
                await update.message.reply_text(
                    f"❌ Failed to reject payout #{payout_id}",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
        
        del context.user_data['rejecting_payout']

# ==================== PHOTO HANDLER ====================
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    if 'awaiting_payout_image' in context.user_data and is_admin(user.id):
        payout_id = context.user_data['payout_image_id']
        
        photo = message.photo[-1]
        photo_file = await photo.get_file()
        
        temp_path = f"payout_{payout_id}_{int(time.time())}.jpg"
        await photo_file.download_to_drive(temp_path)
        
        payout = Database.fetchone('SELECT user_id, amount, payout_method, payout_details FROM payouts WHERE payout_id = ?', (payout_id,))
        if not payout:
            await message.reply_text(
                "Payout not found.",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            return
            
        payout_dict = dict(payout)
        user_id = payout_dict['user_id']
        amount = payout_dict['amount']
        payout_method = payout_dict['payout_method']
        payout_details = payout_dict['payout_details']
        
        default_message = get_system_setting("payout_approved_message",
                                           "✅ Payout approved! ETB{amount} sent to your account.")
        message_text = default_message.replace("ETB{amount}", f"ETB{amount:.2f}")
        
        success = process_payout(payout_id, True, user.id, temp_path)
        if success:
            admin_user = get_user(user.id)
            admin_name = f"{admin_user['first_name']} (@{admin_user['username'] or 'N/A'})" if admin_user else f"Admin #{user.id}"
            
            await send_user_notification(context, user_id, message_text, temp_path)
            await post_payout_to_channel(context, user_id, amount, payout_method, payout_details, admin_name=admin_name, image_path=temp_path)
            await message.reply_text(
                f"✅ Payout #{payout_id} approved with image! Posted to channel.",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
        else:
            await message.reply_text(
                f"❌ Failed to approve payout #{payout_id}",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            
        for key in ['awaiting_payout_image', 'payout_image_id']:
            context.user_data.pop(key, None)
        return
    
    await message.reply_text(
        "Please use the buttons for commands.",
        reply_markup=get_main_menu(user.id)
    )

# ==================== TASK INFO SEARCH ====================
async def handle_task_info_search_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    context.user_data['awaiting_task_id_search'] = True
    
    await update.message.reply_text(
        "🔍 Task Information Search\n\n"
        "Enter Task ID (e.g., 1804258):",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
    )

async def show_task_info(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: str):
    user = update.effective_user
    
    if not is_admin(user.id):
        return
    
    task = get_task_by_unique_id(task_id)
    
    if not task:
        await update.message.reply_text(
            "❌ Task not found or not approved.",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    response = (
        f"📋 Task Information\n\n"
        f"<b>Task ID:</b> <code>{task['unique_task_id']}</code>\n"
        f"<b>Status:</b> ✅ Approved\n"
        f"<b>Reward:</b> ETB{task['reward']:.2f}\n\n"
        f"<b>Task Details:</b>\n"
        f"👤 <b>Name:</b> <code>{task['name']}</code>\n"
    )
    
    if task['father_name'] and task['father_name'].strip():
        response += f"👨 <b>Father Name:</b> <code>{task['father_name']}</code>\n"
    
    response += (
        f"📧 <b>Email:</b> <code>{task['address']}</code>@gmail.com\n"
        f"🔐 <b>Password:</b> <code>{task['password']}</code>\n\n"
        f"<b>Completed By:</b>\n"
        f"👤 <b>Username:</b> @{task['username'] or 'N/A'}\n"
        f"🆔 <b>User ID:</b> <code>{task['assigned_to']}</code>\n"
        f"📅 <b>Completed:</b> {task['completed_time']}\n\n"
        f"💡 <i>Click on the email username to copy it!</i>"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔍 Search Another", callback_data="task_info_search")],
        [InlineKeyboardButton("🏠 Admin Menu", callback_data="back_to_admin")]
    ]
    
    await update.message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ==================== CALLBACK QUERY HANDLER ====================
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = query.from_user.id
    
    if data == "verify_channels":
        channels_ok, joined, missing = await check_user_channels(context, user_id)
        
        if channels_ok:
            await query.edit_message_text(
                "✅ All channels verified! You can now use the bot.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
            )
        else:
            await show_channel_requirement(update, context, missing)
        return
    
    elif data == "admin_manage_channels":
        await query.edit_message_text(
            "Returning to channel management...",
            reply_markup=None
        )
        await query.message.reply_text(
            "📢 Channel Management\n\n"
            "Manage mandatory channels:",
            reply_markup=get_admin_channels_menu()
        )
        return
    
    elif data.startswith("pending_approval_page_"):
        page = int(data.split('_')[3])
        context.user_data['pending_approval_page'] = page
        await handle_pending_approval_list(update, context)
        return
    
    elif data == "refresh_pending_approval":
        context.user_data['pending_approval_page'] = 1
        await handle_pending_approval_list(update, context)
        return
    
    elif data.startswith("completed_page_"):
        page = int(data.split('_')[2])
        context.user_data['completed_tasks_page'] = page
        await handle_completed_tasks_new(update, context)
        return
    
    elif data == "task_info_search":
        context.user_data['awaiting_task_id_search'] = True
        
        await query.message.reply_text(
            "🔍 Task Information Search\n\n"
            "Enter Task ID (e.g., 1804258):",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
    
    elif data.startswith("complete_task_"):
        task_id = int(data.split('_')[2])
        task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ? AND assigned_to = ? AND status = "assigned"', (task_id, user_id))
        
        if not task:
            await query.answer("Task not found or not assigned to you!", show_alert=True)
            return
        
        context.user_data['current_task'] = task_id
        context.user_data['active_task_id'] = task_id
        
        await handle_task_done(update, context)
        return
    
    elif data.startswith("cancel_task_"):
        task_id = int(data.split('_')[2])
        task = Database.fetchone('SELECT * FROM tasks WHERE task_id = ? AND assigned_to = ?', (task_id, user_id))
        
        if not task:
            await query.answer("Task not found or not assigned to you!", show_alert=True)
            return
        
        context.user_data['cancelling_task'] = task_id
        context.user_data['active_task_id'] = task_id
        
        sent_msg = await query.message.reply_text(
            f"Are you sure you want to cancel this task?\n\n"
            f"❌ The task will be returned to available tasks\n"
            f"❌ You will lose this task assignment\n"
            f"✅ You can take it again later",
            reply_markup=get_cancel_confirmation_menu()
        )
        await queue_message_for_deletion(user_id, sent_msg.message_id)
        return
    
    if data == "set_default_telebirr":
        Database.execute('UPDATE users SET default_payment_method = "telebirr" WHERE user_id = ?', (user_id,))
        await query.edit_message_text(
            "✅ Telebirr set as default payment method!",
            reply_markup=None
        )
        return
        
    elif data == "set_default_binance":
        Database.execute('UPDATE users SET default_payment_method = "binance" WHERE user_id = ?', (user_id,))
        await query.edit_message_text(
            "✅ Binance set as default payment method!",
            reply_markup=None
        )
        return
        
    elif data == "set_default_cbe":
        Database.execute('UPDATE users SET default_payment_method = "cbe" WHERE user_id = ?', (user_id,))
        await query.edit_message_text(
            "✅ CBE set as default payment method!",
            reply_markup=None
        )
        return
        
    elif data.startswith("approve_payout_"):
        payout_id = int(data.split('_')[2])
        context.user_data['approving_payout'] = payout_id
        
        await query.message.reply_text(
            f"💰 Approve Payout #{payout_id}\n\n"
            f"How would you like to notify the user?",
            reply_markup=get_payout_approval_options()
        )
        return
            
    elif data.startswith("reject_payout_"):
        payout_id = int(data.split('_')[2])
        context.user_data['rejecting_payout'] = payout_id
        
        await query.message.reply_text(
            f"❌ Reject Payout #{payout_id}\n\n"
            f"How would you like to notify the user?",
            reply_markup=get_payout_rejection_options()
        )
        return
        
    elif data.startswith("remove_admin_"):
        if not is_owner(user_id):
            await query.answer("Only the owner can remove admins!", show_alert=True)
            return
            
        admin_to_remove = int(data.split('_')[2])
        if admin_to_remove == OWNER_ID:
            await query.answer("Cannot remove the owner!", show_alert=True)
            return
            
        success, msg = remove_admin(admin_to_remove, user_id)
        await query.answer(msg, show_alert=True)
        
        await handle_admin_list(update, context)
        return
        
    elif data.startswith("manage_user_"):
        user_id_to_manage = int(data.split('_')[2])
        user = get_user(user_id_to_manage)
        
        if not user:
            await query.edit_message_text(
                "User not found.",
                reply_markup=None
            )
            return
            
        stats = get_user_statistics(user_id_to_manage)
        
        response = (
            f"👤 User Management\n\n"
            f"User ID: <code>{user_id_to_manage}</code>\n"
            f"Username: @{user['username'] or 'N/A'}\n"
            f"Name: <code>{user['first_name']} {user['last_name'] or ''}</code>\n\n"
            f"💰 Balance: ETB{user['balance']:.2f}\n"
            f"⏳ Hold Balance: ETB{user['hold_balance']:.2f}\n"
            f"✅ Tasks Completed: {user['tasks_completed']}\n"
            f"👥 Referrals: {user['referral_count']}\n"
            f"📊 Total Earned: ETB{user['total_earned']:.2f}\n\n"
            f"Select action:"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("💰 Add Balance", callback_data=f"add_balance_{user_id_to_manage}"),
                InlineKeyboardButton("📉 Subtract Balance", callback_data=f"subtract_balance_{user_id_to_manage}")
            ],
            [
                InlineKeyboardButton("📨 Message User", callback_data=f"message_user_{user_id_to_manage}"),
                InlineKeyboardButton("📊 View Details", callback_data=f"view_details_{user_id_to_manage}")
            ],
            [
                InlineKeyboardButton("🔙 Back", callback_data="back_to_user_list")
            ]
        ]
        
        await query.edit_message_text(
            response,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
        
    elif data.startswith("add_balance_"):
        user_id_to_manage = int(data.split('_')[2])
        context.user_data['balance_user_id'] = user_id_to_manage
        context.user_data['balance_action'] = 'add'
        
        await query.message.reply_text(
            f"💰 Add Balance to User #{user_id_to_manage}\n\n"
            f"Enter amount to add:\n"
            f"(Also enter reason after amount, separated by comma)\n\n"
            f"Example: 10.00, Bonus for good work",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        
        context.user_data['awaiting_balance_amount'] = True
        return
        
    elif data.startswith("subtract_balance_"):
        user_id_to_manage = int(data.split('_')[2])
        context.user_data['balance_user_id'] = user_id_to_manage
        context.user_data['balance_action'] = 'subtract'
        
        await query.message.reply_text(
            f"📉 Subtract Balance from User #{user_id_to_manage}\n\n"
            f"Enter amount to subtract:\n"
            f"(Also enter reason after amount, separated by comma)\n\n"
            f"Example: 5.00, Penalty for rule violation",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        
        context.user_data['awaiting_balance_amount'] = True
        return
        
    elif data.startswith("message_user_"):
        user_id_to_manage = int(data.split('_')[2])
        context.user_data['message_user_id'] = user_id_to_manage
        context.user_data['awaiting_user_message'] = True
        
        await query.message.reply_text(
            f"📨 Message User #{user_id_to_manage}\n\n"
            f"Enter message to send:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
        
    elif data.startswith("view_details_"):
        user_id_to_manage = int(data.split('_')[2])
        stats = get_user_statistics(user_id_to_manage)
        
        if not stats:
            await query.edit_message_text(
                "User not found.",
                reply_markup=None
            )
            return
            
        user = stats['user']
        transactions = stats['transactions']
        
        response = (
            f"📊 User Details\n\n"
            f"👤 User ID: <code>{user_id_to_manage}</code>\n"
            f"📛 Username: @{user['username'] or 'N/A'}\n"
            f"👋 Name: <code>{user['first_name']} {user['last_name'] or ''}</code>\n"
            f"📅 Registered: {user['registered_date'].split()[0]}\n"
            f"🕒 Last Active: {user['last_active'].split()[0]}\n"
            f"👑 Admin Status: {'Yes' if user['is_admin'] else 'No'}\n\n"
            f"💰 Financial:\n"
            f"💵 Balance: ETB{user['balance']:.2f}\n"
            f"⏳ Hold Balance: ETB{user['hold_balance']:.2f}\n"
            f"📊 Total Earned: ETB{user['total_earned']:.2f}\n\n"
            f"📋 Tasks:\n"
            f"✅ Completed: {user['tasks_completed']}\n"
            f"⏳ Active: {stats.get('active_tasks', 0)}\n\n"
            f"👥 Referrals:\n"
            f"📊 Count: {user['referral_count']}\n\n"
        )
        
        if transactions:
            response += "💸 Transaction Summary:\n"
            for trans_type, total in transactions.items():
                response += f"  {trans_type}: ETB{total:.2f}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"manage_user_{user_id_to_manage}")]]
        
        await query.edit_message_text(
            response,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
        
    elif data == "back_to_admin":
        await query.edit_message_text(
            "Returning to admin menu...",
            reply_markup=None
        )
        await query.message.reply_text(
            "👑 ADMIN PANEL",
            reply_markup=get_admin_menu_by_permissions(user_id)
        )
        return
        
    elif data == "back_to_user_management":
        await query.edit_message_text(
            "Returning to user management...",
            reply_markup=None
        )
        await query.message.reply_text(
            "👥 User Management",
            reply_markup=get_admin_user_management_menu()
        )
        return
        
    elif data == "back_to_user_list":
        await handle_list_recent_users(query, context)
        return
        
    elif data == "back_to_completed_tasks":
        await handle_completed_tasks_new(query, context)
        return
        
    elif data == "back_to_admin_settings":
        await handle_admin_settings_menu(update, context)
        return
        
    elif data == "main_menu":
        await query.edit_message_text(
            "Returning to main menu...",
            reply_markup=None
        )
        await query.message.reply_text(
            "Main Menu:",
            reply_markup=get_main_menu(user_id)
        )
        return

# ==================== BACKGROUND TASKS ====================
def start_background_tasks():
    def expire_tasks_worker():
        while True:
            expire_old_tasks()
            delete_expired_tasks()
            time.sleep(3600)
    
    thread = threading.Thread(target=expire_tasks_worker, daemon=True)
    thread.start()

# ==================== ENSURE OWNER ADMIN ====================
def ensure_owner_admin():
    owner_id = OWNER_ID
    owner = get_user(owner_id)
    
    if not owner:
        create_user(owner_id, OWNER_USERNAME.replace('@', ''), "Owner", "", None)
        print(f"Created owner user: {owner_id}")
    
    Database.execute('UPDATE users SET is_admin = 1 WHERE user_id = ?', (owner_id,))
    
    full_permissions = list(PERMISSIONS.keys())
    permissions_str = ','.join(full_permissions)
    Database.execute('UPDATE users SET admin_permissions = ? WHERE user_id = ?', (permissions_str, owner_id))
    print(f"Owner {owner_id} set as admin with full permissions")

# ==================== MESSAGE HANDLER ====================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    text = message.text
    
    await queue_message_for_deletion(user.id, message.message_id)
    update_user_activity(user.id)
    
    if 'awaiting_task_otp' in context.user_data:
        await handle_task_otp_input(update, context)
        return
    
    if 'awaiting_generate_otp_email' in context.user_data and is_admin(user.id):
        await handle_generate_otp_email_input(update, context)
        return
    
    if 'awaiting_payout_channel' in context.user_data and is_admin(user.id):
        await handle_payout_channel_input(update, context)
        return
    
    if 'awaiting_generate_otp_user' in context.user_data and is_admin(user.id):
        await handle_generate_otp_email(update, context)
        return
    
    if 'awaiting_milestone_edit' in context.user_data or 'awaiting_milestone_add' in context.user_data or 'awaiting_milestone_remove' in context.user_data:
        await handle_milestone_input(update, context)
        return
    
    if 'awaiting_contact_admin' in context.user_data and is_admin(user.id):
        await handle_contact_admin_input(update, context)
        return
    
    if 'awaiting_referral_bonus' in context.user_data or 'awaiting_referral_percentage' in context.user_data or 'awaiting_referral_broadcast' in context.user_data:
        await handle_referral_input(update, context)
        return
    
    if 'awaiting_email_account' in context.user_data and is_admin(user.id):
        await handle_email_account_input(update, context)
        return
    
    if 'awaiting_remove_email' in context.user_data and is_admin(user.id):
        await handle_email_account_input(update, context)
        return
    
    if 'awaiting_toggle_email' in context.user_data and is_admin(user.id):
        await handle_email_account_input(update, context)
        return
    
    if 'awaiting_delete_single_task' in context.user_data and is_admin(user.id):
        await handle_delete_single_task_input(update, context)
        return
    
    if 'awaiting_add_payment_method' in context.user_data and is_admin(user.id):
        await handle_add_payment_method_input(update, context)
        return
    
    if 'awaiting_remove_payment_method' in context.user_data and is_admin(user.id):
        await handle_remove_payment_method_input(update, context)
        return
    
    if 'awaiting_add_channel' in context.user_data and is_admin(user.id):
        await handle_add_channel_input(update, context)
        return
    
    if 'awaiting_remove_channel' in context.user_data and is_admin(user.id):
        await handle_remove_channel_input(update, context)
        return
    
    if 'awaiting_message_update' in context.user_data and is_admin(user.id):
        try:
            parts = text.split(',', 1)
            if len(parts) != 2:
                await message.reply_text(
                    "❌ Invalid format! Use: message_type,new_message\n\n"
                    "Example: approval,✅ Great job!",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
                return
            
            msg_type = parts[0].strip().lower()
            new_message = parts[1].strip()
            
            if msg_type == 'approval':
                update_system_setting('task_approval_message', new_message)
                await message.reply_text(
                    f"✅ Task approval message updated!\n\nNew message: {new_message}",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
            elif msg_type == 'rejection':
                update_system_setting('task_rejection_message', new_message)
                await message.reply_text(
                    f"✅ Task rejection message updated!\n\nNew message: {new_message}",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
            elif msg_type == 'payout_approval':
                update_system_setting('payout_approved_message', new_message)
                await message.reply_text(
                    f"✅ Payout approval message updated!\n\nNew message: {new_message}",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
            elif msg_type == 'payout_rejection':
                update_system_setting('payout_rejected_message', new_message)
                await message.reply_text(
                    f"✅ Payout rejection message updated!\n\nNew message: {new_message}",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
            else:
                await message.reply_text(
                    f"❌ Unknown message type: {msg_type}\n\n"
                    f"Available types: approval, rejection, payout_approval, payout_rejection",
                    reply_markup=get_admin_menu_by_permissions(user.id)
                )
            
            del context.user_data['awaiting_message_update']
            return
            
        except Exception as e:
            await message.reply_text(
                f"❌ Error updating message: {str(e)}",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            return
    
    if 'awaiting_balance_amount' in context.user_data and is_admin(user.id):
        try:
            parts = text.split(',', 1)
            if len(parts) != 2:
                await message.reply_text(
                    "❌ Please include reason!\n"
                    "Format: amount, reason\n\n"
                    "Example: 10.00, Bonus for good work",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
                return
                
            amount = float(parts[0].strip())
            reason = parts[1].strip()
            
            user_id = context.user_data['balance_user_id']
            action = context.user_data['balance_action']
            
            success = adjust_user_balance(user_id, amount, action, reason, user.id)
            
            if success:
                response = (
                    f"✅ Balance Updated!\n\n"
                    f"User ID: <code>{user_id}</code>\n"
                    f"Action: {action.capitalize()}\n"
                    f"Amount: ETB{amount:.2f}\n"
                    f"Reason: {reason}"
                )
                
                action_text = "added to" if action == 'add' else "deducted from"
                await send_user_notification(context, user_id,
                    f"📊 Balance Update\n\n"
                    f"ETB{amount:.2f} has been {action_text} your balance.\n"
                    f"Reason: {reason}"
                )
            else:
                response = "❌ Failed to update balance"
                
            for key in ['balance_user_id', 'balance_action', 'balance_reason', 'awaiting_balance_amount']:
                context.user_data.pop(key, None)
                
            sent_msg = await message.reply_text(
                response,
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
            return
            
        except ValueError:
            await message.reply_text(
                "❌ Invalid amount!\n"
                "Please enter a valid number:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
    
    if 'awaiting_broadcast_all' in context.user_data and is_admin(user.id):
        sent, failed = await broadcast_message(context, text)
        
        del context.user_data['awaiting_broadcast_all']
        
        sent_msg = await message.reply_text(
            f"📢 Broadcast Complete!\n\n"
            f"✅ Sent to: {sent} users\n"
            f"❌ Failed: {failed} users",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        return
    
    if 'awaiting_broadcast_user' in context.user_data and is_admin(user.id):
        try:
            user_id = int(text)
            context.user_data['broadcast_user_id'] = user_id
            context.user_data['awaiting_broadcast_user_message'] = True
            del context.user_data['awaiting_broadcast_user']
            
            await message.reply_text(
                f"📨 Broadcast to User #{user_id}\n\n"
                f"Enter message to send:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
        except ValueError:
            await message.reply_text(
                "❌ Invalid user ID!\n"
                "Please enter a valid user ID:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
    
    if 'awaiting_broadcast_user_message' in context.user_data and is_admin(user.id):
        user_id = context.user_data['broadcast_user_id']
        success = await send_user_notification(context, user_id, text)
        
        del context.user_data['broadcast_user_id']
        del context.user_data['awaiting_broadcast_user_message']
        
        if success:
            response = f"✅ Message sent to user #{user_id}"
        else:
            response = f"❌ Failed to send message to user #{user_id}"
            
        sent_msg = await message.reply_text(
            response,
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        return
    
    if 'awaiting_bonus_settings' in context.user_data and is_admin(user.id):
        try:
            parts = [p.strip() for p in text.split(',')]
            if len(parts) != 4:
                raise ValueError("Need 4 values")
                
            min_withdrawal = float(parts[0])
            referral_bonus = float(parts[1])
            referral_percentage = float(parts[2])
            task_reward = float(parts[3])
            
            old_settings = get_bonus_settings()
            
            update_bonus_settings(min_withdrawal, referral_bonus, referral_percentage, task_reward)
            
            await broadcast_price_update(context, old_settings, {
                'min_withdrawal': min_withdrawal,
                'referral_bonus': referral_bonus,
                'referral_percentage': referral_percentage,
                'task_reward': task_reward
            })
            
            del context.user_data['awaiting_bonus_settings']
            
            await message.reply_text(
                f"✅ Bonus Settings Updated!\n\n"
                f"💰 Min Withdrawal: ETB{min_withdrawal:.2f}\n"
                f"👥 Referral Bonus: ETB{referral_bonus:.2f}\n"
                f"📊 Referral Percentage: {referral_percentage}%\n"
                f"✅ Task Reward: ETB{task_reward:.2f}\n\n"
                f"📢 Users have been notified of the changes.",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            return
            
        except ValueError:
            await message.reply_text(
                "❌ Invalid format!\n\n"
                "Please enter: min_withdrawal,referral_bonus,referral_percentage,task_reward\n\n"
                "Example: 20.00,2.00,5.00,0.25",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
    
    if 'awaiting_add_admin' in context.user_data and is_admin(user.id) and is_owner(user.id):
        parts = text.strip().split('+')
        user_id_to_add = int(parts[0])
        
        extra_permissions = parts[1:] if len(parts) > 1 else None
        
        success, msg = add_admin_with_permissions(user_id_to_add, user.id, extra_permissions)
        
        del context.user_data['awaiting_add_admin']
        
        if success:
            try:
                permissions = get_admin_permissions(user_id_to_add)
                perm_display = [PERMISSIONS[p] for p in permissions if p in PERMISSIONS]
                await context.bot.send_message(
                    chat_id=user_id_to_add,
                    text=f"🎉 <b>Congratulations!</b> You have been promoted to Admin!\n\n"
                         f"📋 Your permissions:\n• " + "\n• ".join(perm_display) + "\n\n"
                         f"Click <b>Start</b> or type <b>/start</b> to see your Admin Panel.\n\n"
                         f"👑 Your admin panel will appear in the main menu.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=ReplyKeyboardMarkup([["🏠 Main Menu"]], resize_keyboard=True)
                )
                msg += f"\n\n📢 Notification sent to the new admin."
            except Exception as e:
                msg += f"\n\n⚠️ Could not notify the new admin. Error: {str(e)}"
        
        await message.reply_text(
            msg,
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    if 'awaiting_remove_admin' in context.user_data and is_admin(user.id) and is_owner(user.id):
        try:
            user_id_to_remove = int(text.strip())
            success, msg = remove_admin(user_id_to_remove, user.id)
            
            del context.user_data['awaiting_remove_admin']
            
            if success:
                try:
                    await context.bot.send_message(
                        chat_id=user_id_to_remove,
                        text=f"⚠️ <b>Admin Status Removed</b>\n\n"
                             f"You are no longer an admin of {BOT_NAME} bot.\n\n"
                             f"Your admin permissions have been revoked.",
                        parse_mode=ParseMode.HTML
                    )
                    msg += f"\n\n📢 Notification sent to the removed admin."
                except Exception as e:
                    msg += f"\n\n⚠️ Could not notify the removed admin."
            
            await message.reply_text(
                msg,
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            return
            
        except ValueError:
            await message.reply_text(
                "❌ Invalid user ID! Please enter a valid numeric user ID:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
    
    if 'awaiting_bulk_tasks' in context.user_data and is_admin(user.id):
        lines = text.strip().split('\n')
        if len(lines) > MAX_BULK_TASKS:
            await message.reply_text(
                f"❌ Too many tasks! Maximum {MAX_BULK_TASKS} tasks per upload.\n"
                f"You sent {len(lines)} tasks. Please split into smaller batches.",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            del context.user_data['awaiting_bulk_tasks']
            return
        
        tasks_list = []
        errors = []
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
                
            parts = [p.strip() for p in line.split('/')]
            
            if len(parts) == 3:
                task = {
                    'name': parts[0],
                    'father_name': '',
                    'address': parts[1],
                    'password': parts[2]
                }
            elif len(parts) == 4:
                task = {
                    'name': parts[0],
                    'father_name': parts[1],
                    'address': parts[2],
                    'password': parts[3]
                }
            else:
                errors.append(f"Line {line_num}: Invalid format (use name/address/password or name/father_name/address/password)")
                continue
                
            if not task['name'] or not task['address'] or not task['password']:
                errors.append(f"Line {line_num}: Name, address and password are required")
                continue
                
            tasks_list.append(task)
        
        if not tasks_list:
            await message.reply_text(
                "❌ No valid tasks found.",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            del context.user_data['awaiting_bulk_tasks']
            return
        
        created_count, creation_errors = create_bulk_tasks(tasks_list)
        errors.extend(creation_errors)
        
        response = f"📦 Bulk Task Upload Complete\n\n"
        response += f"✅ Successfully added: {created_count} tasks\n"
        response += f"❌ Failed: {len(errors)} tasks\n"
        
        if errors:
            response += f"\nErrors:\n"
            for error in errors[:10]:
                response += f"• {error}\n"
        
        settings = get_bonus_settings()
        response += f"\n💰 Reward per task: ETB{settings['task_reward']:.2f}"
        
        del context.user_data['awaiting_bulk_tasks']
        
        await message.reply_text(
            response,
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
    
    if 'awaiting_expiry_hours' in context.user_data and is_admin(user.id):
        try:
            hours = int(text.strip())
            if hours < 1 or hours > 168:
                await message.reply_text(
                    "❌ Invalid hours! Must be between 1 and 168.",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
                return
                
            update_system_setting('task_expiry_hours', str(hours))
            del context.user_data['awaiting_expiry_hours']
            
            await message.reply_text(
                f"✅ Task Expiry Updated!\n\n"
                f"⏰ Tasks will now expire after {hours} hours.\n"
                f"🗑️ Expired tasks older than 24 hours will be automatically deleted.",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            return
            
        except ValueError:
            await message.reply_text(
                "❌ Invalid number! Please enter a valid number of hours:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
    
    if 'awaiting_user_message' in context.user_data and is_admin(user.id):
        user_id = context.user_data['message_user_id']
        message_text = text
        
        success = await send_user_notification(context, user_id, message_text)
        
        del context.user_data['message_user_id']
        del context.user_data['awaiting_user_message']
        
        if success:
            response = f"✅ Message sent to user #{user_id}"
        else:
            response = f"❌ Failed to send message to user #{user_id}"
            
        sent_msg = await message.reply_text(
            response,
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        return
    
    if 'awaiting_user_search' in context.user_data and is_admin(user.id):
        users = search_users(text)
        
        if not users:
            await message.reply_text(
                "❌ No users found.",
                reply_markup=get_admin_user_management_menu()
            )
            return
            
        response = "🔍 Search Results:\n\n"
        for i, user_data in enumerate(users[:10], 1):
            admin_status_icon = "👑" if user_data['is_admin'] else "👤"
            response += (
                f"{i}. {admin_status_icon} ID: <code>{user_data['user_id']}</code>\n"
                f"   👤 @{user_data['username'] or 'N/A'}\n"
                f"   📛 <code>{user_data['first_name']}</code>\n"
                f"   💰 Balance: ETB{user_data['balance']:.2f}\n"
                f"   ✅ Tasks: {user_data['tasks_completed']}\n\n"
            )
            
        response += "Click on user ID buttons below to manage:\n💡 Click on data to copy!"
        
        keyboard = []
        for user_data in users[:5]:
            keyboard.append([
                InlineKeyboardButton(
                    f"👤 {user_data['user_id']} - {user_data['first_name']}",
                    callback_data=f"manage_user_{user_data['user_id']}"
                )
            ])
            
        keyboard.append([
            InlineKeyboardButton("🔙 Back", callback_data="back_to_user_management")
        ])
        
        del context.user_data['awaiting_user_search']
        
        sent_msg = await message.reply_text(
            response,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await queue_message_for_deletion(user.id, sent_msg.message_id)
        return
    
    if 'awaiting_task_id_search' in context.user_data and is_admin(user.id):
        task_id = text.strip()
        del context.user_data['awaiting_task_id_search']
        
        await show_task_info(update, context, task_id)
        return
    
    if text == "❌ Cancel":
        for key in list(context.user_data.keys()):
            if key.startswith('awaiting_'):
                del context.user_data[key]
                
        await message.reply_text(
            "Action cancelled.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    admin_status = is_admin(user.id)
    in_admin_mode = context.user_data.get('admin_mode', False) if admin_status else False
    
    exempt_commands = ["❌ Cancel", "✅ Verify Join", "/start", "🏠 Main Menu", "👑 Admin Panel", 
                      "🏠 User Menu", "📢 Manage Channels", "🔍 Task Info", "✅ Verify Joins"]
    
    if not admin_status and text not in exempt_commands:
        channels_ok, joined, missing = await check_user_channels(context, user.id)
        if not channels_ok:
            await show_channel_requirement(update, context, missing)
            return
    
    if text == "🏠 Main Menu":
        await message.reply_text(
            "Main Menu:",
            reply_markup=get_main_menu(user.id)
        )
        return
        
    elif text == "📋 Take Task":
        await handle_take_task(update, context)
        return
        
    elif text == "📋 View My Active Task":
        await handle_view_active_task(update, context)
        return
        
    elif text == "❌ Cancel My Active Task":
        await handle_cancel_active_task(update, context)
        return
        
    elif text == "📨 Resend Code" or text.startswith("📨 Resend Code"):
        await handle_otp_resend(update, context)
        return
        
    elif text == "❌ Cancel Task":
        await handle_cancel_during_otp(update, context)
        return
        
    elif text == "📝 My Tasks":
        await handle_my_tasks(update, context)
        return
        
    elif text == "💰 My Balance":
        await handle_my_balance(update, context)
        return
        
    elif text == "👥 My Referrals":
        await handle_my_referrals(update, context)
        return
        
    elif text == "💸 Request Payout":
        await handle_request_payout(update, context)
        return
        
    elif text == "⚙️ Settings":
        await handle_settings(update, context)
        return
        
    elif text == "👑 Admin Panel" and admin_status:
        context.user_data['admin_mode'] = True
        await message.reply_text(
            "👑 ADMIN PANEL\nSelect an option:",
            reply_markup=get_admin_menu_by_permissions(user.id)
        )
        return
        
    elif text == "🏠 User Menu" and admin_status and in_admin_mode:
        context.user_data['admin_mode'] = False
        await message.reply_text(
            "Switched to user mode.",
            reply_markup=get_main_menu(user.id)
        )
        return
    
    elif text == "🔧 Payment Methods":
        await handle_payment_methods(update, context)
        return
        
    elif text == "📞 Contact Admin":
        await handle_contact_admin(update, context)
        return
        
    elif text == "📊 Account Info":
        await handle_account_info(update, context)
        return
        
    elif text == "🔄 Change Payment Method":
        await handle_change_payment_method(update, context)
        return
        
    elif text == "🔙 Back to Settings":
        await handle_settings(update, context)
        return
    
    elif text == "📱 Setup Telebirr":
        context.user_data['awaiting_telebirr_name'] = True
        await message.reply_text(
            "📱 Setup Telebirr\n\n"
            "Please enter your full name as registered on Telebirr:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
        
    elif text == "🪙 Setup Binance":
        context.user_data['awaiting_binance_id'] = True
        await message.reply_text(
            "🪙 Setup Binance\n\n"
            "Please enter your Binance ID or UID:\n\n"
            f"💡 Note: Payouts will be converted to USD at rate 1 USD = {get_usd_to_etb_rate():.2f} ETB",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
        
    elif text == "🏦 Setup CBE":
        context.user_data['awaiting_cbe_name'] = True
        await message.reply_text(
            "🏦 Setup CBE Account\n\n"
            "Please enter your full name as registered on CBE:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
        
    elif text == "📋 View Saved Methods":
        await handle_view_saved_methods(update, context)
        return
        
    elif text == "🗑️ Clear Methods":
        await handle_clear_methods(update, context)
        return
    
    elif text == "✅ Done":
        await handle_task_done(update, context)
        return
        
    elif text == "↩️ Back":
        await handle_task_back(update, context)
        return
        
    elif text == "✅ Confirm Done":
        await handle_confirm_task_done(update, context)
        return
        
    elif text == "❌ Go Back":
        await handle_task_back_to_task(update, context)
        return
        
    elif text == "✅ Confirm Cancel":
        await handle_confirm_task_cancel(update, context)
        return
        
    elif text == "❌ Keep Task":
        await handle_keep_task(update, context)
        return
    
    elif text.startswith("📱 Use Saved") or text.startswith("🪙 Use Saved") or text.startswith("🏦 Use Saved"):
        await handle_use_saved_payout(update, context)
        return
    
    elif in_admin_mode:
        permissions = get_admin_permissions(user.id)
        
        if text == "📊 Dashboard" and 'dashboard' in permissions:
            await handle_admin_dashboard(update, context)
            return
            
        elif text == "📦 Bulk Add Tasks" and 'add' in permissions:
            context.user_data['awaiting_bulk_tasks'] = True
            await message.reply_text(
                "📦 Bulk Add Tasks\n\n"
                "Send tasks in the following format (one per line):\n\n"
                "Format 1: name/address/password\n"
                "Format 2: name/father_name/address/password\n\n"
                f"Example:\n"
                f"Abel/abeluser101/mypass123\n"
                f"Abel/John/abeluser101/mypass123\n\n"
                f"Max {MAX_BULK_TASKS} tasks at once.\n"
                f"Each email will be address@gmail.com\n"
                f"Reward: ETB{get_bonus_settings()['task_reward']:.2f} each",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
            
        elif text == "⏳ Pending Tasks" and 'pending' in permissions:
            await handle_pending_tasks(update, context)
            return
            
        elif text == "📋 Pending Approval List" and 'pending_approval' in permissions:
            await handle_pending_approval_list(update, context)
            return
            
        elif text == "✅ Completed Tasks" and 'completed' in permissions:
            await handle_completed_tasks_new(update, context)
            return
            
        elif text == "💰 Pending Payouts" and 'payout' in permissions:
            await handle_pending_payouts_modified(update, context)
            return
            
        elif text == "👥 Manage Users" and 'manage' in permissions:
            await handle_manage_users(update, context)
            return
            
        elif text == "📈 Statistics" and 'statistics' in permissions:
            await handle_statistics(update, context)
            return
            
        elif text == "⚙️ Admin Settings" and 'setting' in permissions:
            await handle_admin_settings_menu(update, context)
            return
            
        elif text == "📢 Broadcast" and 'broadcast' in permissions:
            await handle_broadcast_menu(update, context)
            return
            
        elif text == "💾 Backup Data" and 'backup' in permissions:
            await handle_backup_data(update, context)
            return
            
        elif text == "📢 Manage Channels" and 'channel' in permissions:
            await handle_admin_channels_menu(update, context)
            return
            
        elif text == "🔍 Task Info" and 'taskinfo' in permissions:
            await handle_task_info_search_menu(update, context)
            return
            
        elif text == "📧 Email Accounts" and 'email' in permissions:
            await handle_email_accounts_menu(update, context)
            return
            
        elif text == "🎯 Referral Settings" and 'referral' in permissions:
            await handle_referral_settings_menu(update, context)
            return
            
        elif text == "🎯 Milestone Bonuses" and 'setting' in permissions:
            await handle_milestone_bonuses_menu(update, context)
            return
            
        elif text == "📥 Export Tasks" and 'export' in permissions:
            await handle_export_menu(update, context)
            return
            
        elif text == "📞 Set Contact Admin" and 'contact' in permissions:
            await handle_set_contact_admin(update, context)
            return
            
        elif text == "✅ Enable Milestones" and 'setting' in permissions:
            await handle_milestone_enable(update, context)
            return
            
        elif text == "❌ Disable Milestones" and 'setting' in permissions:
            await handle_milestone_disable(update, context)
            return
            
        elif text == "📋 View Milestones" and 'setting' in permissions:
            await handle_view_milestones(update, context)
            return
            
        elif text == "✏️ Edit Milestone" and 'setting' in permissions:
            await handle_edit_milestone(update, context)
            return
            
        elif text == "➕ Add Milestone" and 'setting' in permissions:
            await handle_add_milestone(update, context)
            return
            
        elif text == "🗑️ Remove Milestone" and 'setting' in permissions:
            await handle_remove_milestone(update, context)
            return
            
        elif text == "🗑️ Delete Tasks" and 'delete' in permissions:
            await handle_delete_tasks_menu(update, context)
            return
            
        elif text == "🗑️ Delete Single Completed Task" and 'delete' in permissions:
            await handle_delete_single_completed_task(update, context)
            return
            
        elif text == "🗑️ Delete All Completed Tasks" and 'delete' in permissions:
            await handle_delete_all_completed_tasks(update, context)
            return
            
        elif text == "🗑️ Delete All Failed Tasks" and 'delete' in permissions:
            await handle_delete_all_failed_tasks(update, context)
            return
            
        elif text == "📥 Export Completed Tasks" and 'export' in permissions:
            await handle_export_completed_tasks(update, context)
            return
            
        elif text == "📥 Export Failed Tasks" and 'export' in permissions:
            await handle_export_failed_tasks(update, context)
            return
            
        elif text == "💰 Edit Referral Bonus" and 'referral' in permissions:
            await handle_edit_referral_bonus(update, context)
            return
            
        elif text == "📊 Edit Referral Percentage" and 'referral' in permissions:
            await handle_edit_referral_percentage(update, context)
            return
            
        elif text == "📢 Broadcast Referral Update" and 'referral' in permissions:
            await handle_broadcast_referral_update(update, context)
            return
            
        elif text == "➕ Add Email Account" and 'email' in permissions:
            await handle_add_email_account(update, context)
            return
            
        elif text == "➖ Remove Email Account" and 'email' in permissions:
            await handle_remove_email_account(update, context)
            return
            
        elif text == "✅ Enable Account" and 'email' in permissions:
            await handle_toggle_email_account(update, context)
            return
            
        elif text == "❌ Disable Account" and 'email' in permissions:
            await handle_toggle_email_account(update, context)
            return
            
        elif text == "📋 View All Accounts" and 'email' in permissions:
            await handle_email_accounts_menu(update, context)
            return
            
        elif text == "🔐 OTP" and 'otp' in permissions:
            await handle_otp_menu(update, context)
            return
            
        elif text == "📢 Payout Channel" and 'payout_channel' in permissions:
            await handle_payout_channel_menu(update, context)
            return
            
        elif text == "🔄 Toggle OTP (Currently" in text and 'otp' in permissions:
            await handle_otp_toggle(update, context)
            return
            
        elif text == "🔑 Generate OTP for User" and 'otp' in permissions:
            await handle_generate_otp(update, context)
            return
            
        elif text == "✏️ Change Payout Channel" and 'payout_channel' in permissions:
            await handle_change_payout_channel(update, context)
            return
            
        elif text.startswith("✅ Approve #") or text.startswith("❌ Reject #"):
            if 'pending' in permissions:
                await handle_admin_task_action(update, context)
            return
            
        elif text.startswith("💰 Add Balance #") or text.startswith("📉 Subtract Balance #"):
            if 'manage' in permissions:
                await handle_admin_balance_action(update, context)
            return
            
        elif text.startswith("📨 Message User #"):
            if 'manage' in permissions:
                await handle_admin_message_user(update, context)
            return
            
        elif text.startswith("📊 View Details #"):
            if 'manage' in permissions:
                await handle_admin_view_user_details(update, context)
            return
            
        elif "⏭️ Next Task" in text:
            if 'pending' in permissions:
                await handle_next_pending_task(update, context)
            return
            
        elif text == "📋 List Recent Users":
            if 'manage' in permissions:
                await handle_list_recent_users(update, context)
            return
            
        elif text == "🔍 Search User by ID":
            if 'manage' in permissions:
                context.user_data['awaiting_user_search'] = True
                await message.reply_text(
                    "🔍 Search User\n\n"
                    "Enter user ID or username to search:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "📊 View Online Users":
            if 'manage' in permissions:
                await handle_view_online_users(update, context)
            return
            
        elif text == "➕ Add Admin":
            if is_owner(user.id):
                context.user_data.pop('awaiting_bonus_settings', None)
                context.user_data.pop('awaiting_expiry_hours', None)
                context.user_data.pop('awaiting_add_payment_method', None)
                context.user_data.pop('awaiting_remove_payment_method', None)
                context.user_data.pop('awaiting_add_channel', None)
                context.user_data.pop('awaiting_remove_channel', None)
                context.user_data.pop('awaiting_email_account', None)
                context.user_data.pop('awaiting_remove_email', None)
                context.user_data.pop('awaiting_toggle_email', None)
                
                context.user_data['awaiting_add_admin'] = True
                perm_list = "\n".join([f"• {k}: {v}" for k, v in PERMISSIONS.items()])
                await message.reply_text(
                    "➕ Add New Admin\n\n"
                    "Format: user_id+permission1+permission2\n\n"
                    f"Available permissions:\n{perm_list}\n\n"
                    "Examples:\n"
                    "123456789\n"
                    "123456789+add+pending+completed\n\n"
                    "Default permissions (no +): dashboard, statistics, broadcast",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "➖ Remove Admin":
            if is_owner(user.id):
                context.user_data.pop('awaiting_bonus_settings', None)
                context.user_data.pop('awaiting_expiry_hours', None)
                context.user_data.pop('awaiting_add_payment_method', None)
                context.user_data.pop('awaiting_remove_payment_method', None)
                context.user_data.pop('awaiting_add_channel', None)
                context.user_data.pop('awaiting_remove_channel', None)
                context.user_data.pop('awaiting_email_account', None)
                context.user_data.pop('awaiting_remove_email', None)
                context.user_data.pop('awaiting_toggle_email', None)
                
                context.user_data['awaiting_remove_admin'] = True
                await message.reply_text(
                    "➖ Remove Admin\n\n"
                    "Enter user ID to remove from admin:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "🔙 Back to Users":
            if 'manage' in permissions:
                await handle_manage_users(update, context)
            return
            
        elif text == "📢 Broadcast to All":
            if 'broadcast' in permissions:
                context.user_data['awaiting_broadcast_all'] = True
                await message.reply_text(
                    "📢 Broadcast to All Users\n\n"
                    "Enter the message to send to all users:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "📨 Broadcast to User":
            if 'broadcast' in permissions:
                context.user_data['awaiting_broadcast_user'] = True
                await message.reply_text(
                    "📨 Broadcast to Specific User\n\n"
                    "Enter user ID first:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "💰 Adjust Rewards":
            if 'setting' in permissions:
                context.user_data['awaiting_bonus_settings'] = True
                await message.reply_text(
                    "💰 Adjust Rewards\n\n"
                    "Enter values in format:\n"
                    "min_withdrawal,referral_bonus,referral_percentage,task_reward\n\n"
                    "Example: 20.00,2.00,5.00,0.25",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "⏰ Set Expiry Hours":
            if 'setting' in permissions:
                context.user_data['awaiting_expiry_hours'] = True
                await message.reply_text(
                    "⏰ Set Task Expiry Hours\n\n"
                    "Enter number of hours for task expiry (1-168):",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "📋 Payment Methods":
            if 'setting' in permissions:
                await handle_admin_payment_methods(update, context)
            return
            
        elif text == "📝 Update Messages":
            if 'setting' in permissions:
                await handle_update_messages(update, context)
            return
            
        elif text == "📋 View All Channels":
            if 'channel' in permissions:
                await handle_view_all_channels(update, context)
            return
            
        elif text == "➕ Add Channel":
            if 'channel' in permissions:
                context.user_data['awaiting_add_channel'] = True
                await message.reply_text(
                    "➕ Add Channel\n\n"
                    "Enter channel username (e.g., @channelname):",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "➖ Remove Channel":
            if 'channel' in permissions:
                channels = get_mandatory_channels()
                if not channels:
                    await message.reply_text(
                        "📭 No channels to remove.",
                        reply_markup=get_admin_channels_menu()
                    )
                    return
                    
                channel_list = "\n".join([f"• {ch}" for ch in channels])
                context.user_data['awaiting_remove_channel'] = True
                await message.reply_text(
                    f"➖ Remove Channel\n\n"
                    f"Current channels:\n{channel_list}\n\n"
                    f"Enter channel username to remove (e.g., @channelname):",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "✅ Approve with Default Message":
            if 'payout' in permissions and 'approving_payout' in context.user_data:
                await handle_payout_default_approve(update, context)
            return
            
        elif text == "📸 Approve with Image + Default Message":
            if 'payout' in permissions and 'approving_payout' in context.user_data:
                await handle_payout_image_approve(update, context)
            return
            
        elif text == "❌ Reject with Default Message":
            if 'payout' in permissions and 'rejecting_payout' in context.user_data:
                await handle_payout_default_reject(update, context)
            return
            
        elif text == "❌ Cancel Approval":
            context.user_data.pop('approving_task', None)
            context.user_data.pop('rejecting_task', None)
            context.user_data.pop('approving_payout', None)
            context.user_data.pop('rejecting_payout', None)
            await message.reply_text(
                "Action cancelled.",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            return
            
        elif text == "🔙 Back to Admin Menu":
            await message.reply_text(
                "👑 ADMIN PANEL",
                reply_markup=get_admin_menu_by_permissions(user.id)
            )
            return
            
        elif text == "📋 View Active Methods":
            if 'setting' in permissions:
                await handle_view_payment_methods(update, context)
            return
            
        elif text == "➕ Add Method":
            if 'setting' in permissions:
                context.user_data['awaiting_add_payment_method'] = True
                await message.reply_text(
                    "➕ Add Payment Method\n\n"
                    "Enter new payment method name:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "➖ Remove Method":
            if 'setting' in permissions:
                context.user_data['awaiting_remove_payment_method'] = True
                await message.reply_text(
                    "➖ Remove Payment Method\n\n"
                    "Enter payment method name to remove:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
            return
            
        elif text == "📋 Admin List":
            if is_owner(user.id):
                await handle_admin_list(update, context)
            return
            
        elif text == "🔙 Back to Admin Settings":
            await handle_admin_settings_menu(update, context)
            return
    
    if 'awaiting_telebirr_name' in context.user_data:
        context.user_data['telebirr_name'] = text
        context.user_data['awaiting_telebirr_phone'] = True
        del context.user_data['awaiting_telebirr_name']
        
        await message.reply_text(
            "📱 Setup Telebirr\n\n"
            "Please enter your Telebirr phone number:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
        
    if 'awaiting_telebirr_phone' in context.user_data:
        phone = text.strip()
        if not phone.isdigit() or len(phone) != 10 or not phone.startswith('09'):
            await message.reply_text(
                "❌ Invalid phone number!\n"
                "Please enter a valid 10-digit Ethiopian phone number starting with 09:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
            
        save_payment_method(user.id, 'telebirr', 
                          name=context.user_data['telebirr_name'],
                          phone=phone)
        
        del context.user_data['telebirr_name']
        del context.user_data['awaiting_telebirr_phone']
        
        await message.reply_text(
            f"✅ Telebirr setup complete!\n\n"
            f"Your Telebirr details have been saved.",
            reply_markup=get_payment_methods_menu()
        )
        return
    
    if 'awaiting_binance_id' in context.user_data:
        binance_id = text.strip()
        if len(binance_id) < 5:
            await message.reply_text(
                "❌ Invalid Binance ID!\n"
                "Please enter a valid Binance ID or UID:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
            
        context.user_data['temp_binance_id'] = binance_id
        
        keyboard = [
            [InlineKeyboardButton("✅ Confirm", callback_data="confirm_binance"),
             InlineKeyboardButton("❌ Cancel", callback_data="cancel_binance")]
        ]
        
        await message.reply_text(
            f"🪙 Confirm Binance ID\n\n"
            f"Binance ID: <code>{binance_id}</code>\n\n"
            f"Please confirm this is correct:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        del context.user_data['awaiting_binance_id']
        return
    
    if 'awaiting_cbe_name' in context.user_data:
        context.user_data['cbe_name'] = text
        context.user_data['awaiting_cbe_account'] = True
        del context.user_data['awaiting_cbe_name']
        
        await message.reply_text(
            "🏦 Setup CBE Account\n\n"
            "Please enter your CBE account number:",
            reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
        )
        return
        
    if 'awaiting_cbe_account' in context.user_data:
        cbe_account = text.strip()
        if not cbe_account.isdigit() or len(cbe_account) < 10:
            await message.reply_text(
                "❌ Invalid CBE account number!\n"
                "Please enter a valid CBE account number:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
            
        save_payment_method(user.id, 'cbe', 
                          name=context.user_data['cbe_name'],
                          account=cbe_account)
        
        del context.user_data['cbe_name']
        del context.user_data['awaiting_cbe_account']
        
        await message.reply_text(
            f"✅ CBE account setup complete!\n\n"
            f"Your CBE account has been saved.",
            reply_markup=get_payment_methods_menu()
        )
        return
    
    if 'awaiting_payout_amount' in context.user_data:
        try:
            amount = float(text)
            settings = get_bonus_settings()
            
            if amount < settings['min_withdrawal']:
                await message.reply_text(
                    f"❌ Minimum withdrawal is ETB{settings['min_withdrawal']:.2f}\n"
                    f"Your entered: ETB{amount:.2f}\n\n"
                    f"Please enter a higher amount:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
                return
                
            user_data = get_user(user.id)
            if user_data['balance'] < amount:
                await message.reply_text(
                    f"❌ Insufficient balance!\n"
                    f"Your balance: ETB{user_data['balance']:.2f}\n"
                    f"Requested: ETB{amount:.2f}\n\n"
                    f"Please enter a lower amount:",
                    reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
                )
                return
                
            new_details = context.user_data.get('new_payout_details')
            details = context.user_data.get('payout_details', '')
            method = context.user_data['payout_method']
            
            if method == 'binance':
                rate = get_usd_to_etb_rate()
                usd_amount = amount / rate
                details += f"\n💰 ETB {amount:.2f} = ${usd_amount:.2f} USD at rate {rate:.2f}"
            
            success, message_text = request_payout(
                user.id,
                amount,
                method,
                details,
                new_details
            )
            
            if success:
                response = (
                    f"✅ Payout Request Submitted!\n\n"
                    f"Amount: ETB{amount:.2f}\n"
                    f"Method: {method.upper()}\n"
                )
                
                if method == 'binance':
                    rate = get_usd_to_etb_rate()
                    usd_amount = amount / rate
                    response += f"💵 USD Amount: ${usd_amount:.2f} (at rate {rate:.2f} ETB/USD)\n"
                
                if new_details:
                    response += f"Details: {new_details}\n"
                else:
                    response += f"Details: {details}\n"
                
                response += f"\n⏰ Will be processed within 24 hours."
            else:
                response = f"❌ {message_text}"
                
            for key in ['payout_method', 'payout_details', 'new_payout_details', 'awaiting_payout_amount']:
                context.user_data.pop(key, None)
                
            sent_msg = await message.reply_text(
                response,
                reply_markup=get_main_menu(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
            return
            
        except ValueError:
            await message.reply_text(
                "❌ Invalid amount!\n"
                "Please enter a valid number:",
                reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)
            )
            return
    
    await message.reply_text(
        "I didn't understand that command.\n"
        "Please use the buttons below:",
        reply_markup=get_main_menu(user.id)
    )

# ==================== START COMMAND ====================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    referral_code = None
    if context.args:
        referral_code = context.args[0]
    
    existing_user = get_user(user.id)
    
    if existing_user:
        update_user_activity(user.id)
        
        if should_show_welcome_back(user.id):
            welcome_back_msg = get_system_setting(
                "welcome_back_message", 
                "<b>✨ WELCOME BACK! ✨</b>\n\nWe're thrilled to see you again! 🌟\n\nReady to earn more? Let's continue your journey! 🚀"
            )
            
            sent_msg = await message.reply_text(
                welcome_back_msg,
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
            update_user_welcome_shown(user.id)
        else:
            sent_msg = await message.reply_text(
                f"👋 Welcome back {user.first_name}!\n\n"
                f"Use the buttons below to continue:",
                reply_markup=get_main_menu(user.id)
            )
            await queue_message_for_deletion(user.id, sent_msg.message_id)
        
        channels_ok, joined, missing = await check_user_channels(context, user.id)
        if not channels_ok:
            await show_channel_requirement(update, context, missing)
        return
    
    success, result_msg = create_user(user.id, user.username or "", user.first_name, user.last_name or "", referral_code)
    
    welcome_msg = get_system_setting(
        "welcome_message",
        f"<b>🎉 WELCOME TO {BOT_NAME.upper()}! 🎉</b>\n\n"
        "🌟 <b>Your earning journey starts here!</b> 🌟\n\n"
        "💰 <b>Earn money by completing simple tasks</b>\n"
        "👥 <b>Invite friends & earn bonuses</b>\n"
        "⚡ <b>Quick & easy withdrawals</b>\n\n"
        "✨ <b>Let's get started and grow together!</b> ✨"
    )
    
    response = f"👋 Hello <b>{user.first_name}</b>!\n\n{welcome_msg}\n\n"
    
    if referral_code and "already referred" not in result_msg.lower():
        response += f"✅ <b>Referral applied!</b> You joined using a referral link.\n\n"
    
    response += "Use the buttons below to navigate:"
    
    sent_msg = await message.reply_text(
        response,
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)
    
    update_user_activity(user.id)
    update_user_welcome_shown(user.id)
    
    channels_ok, joined, missing = await check_user_channels(context, user.id)
    if not channels_ok:
        await show_channel_requirement(update, context, missing)

async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    update_user_activity(user.id)
    
    sent_msg = await message.reply_text(
        "🔄 Menu refreshed!\n\n"
        "Your menu has been updated.",
        reply_markup=get_main_menu(user.id)
    )
    await queue_message_for_deletion(user.id, sent_msg.message_id)

# ==================== CALLBACK QUERY HANDLER FOR BINANCE ====================
async def handle_callback_query_binance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = query.from_user.id
    
    if data == "confirm_binance":
        if 'temp_binance_id' in context.user_data:
            binance_id = context.user_data['temp_binance_id']
            save_payment_method(user_id, 'binance', binance_id=binance_id)
            del context.user_data['temp_binance_id']
            
            await query.edit_message_text(
                f"✅ Binance setup complete!\n\n"
                f"Binance ID: <code>{binance_id}</code>\n\n"
                f"Your Binance ID has been saved.\n"
                f"💡 Payouts will be converted to USD at current exchange rate.",
                parse_mode=ParseMode.HTML
            )
            await query.message.reply_text(
                "Use the buttons below to continue:",
                reply_markup=get_payment_methods_menu()
            )
        return
        
    elif data == "cancel_binance":
        if 'temp_binance_id' in context.user_data:
            del context.user_data['temp_binance_id']
        await query.edit_message_text(
            "❌ Binance setup cancelled."
        )
        await query.message.reply_text(
            "Use the buttons below to continue:",
            reply_markup=get_payment_methods_menu()
        )
        return

# ==================== MAIN FUNCTION ====================
def main():
    init_database()
    ensure_owner_admin()
    
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler('bot.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("refresh", refresh_command))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(CallbackQueryHandler(handle_callback_query_binance, pattern="confirm_binance|cancel_binance"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    
    async def delete_old_messages_task():
        while True:
            await delete_old_messages(application.bot)
            await asyncio.sleep(3600)
    
    loop = asyncio.get_event_loop()
    loop.create_task(delete_old_messages_task())
    
    logger.info(f"🤖 {BOT_NAME} Bot is starting...")
    logger.info(f"👑 Owner: {OWNER_ID} ({OWNER_USERNAME})")
    logger.info(f"📊 Database: {DB_FILE}")
    logger.info(f"📢 Payout Channel: {get_payout_channel()}")
    logger.info(f"⚙️ Max active tasks per user: {MAX_ACTIVE_TASKS}")
    logger.info(f"📦 Max bulk tasks: {MAX_BULK_TASKS}")
    logger.info("✅ Referral: Uses User ID as referral code")
    logger.info("✅ Cancel Buttons: Working during OTP phase")
    logger.info("✅ Payout Images: Sent to user and posted to channel")
    logger.info("✅ Delete Tasks: Single completed, all completed, all failed")
    logger.info("✅ Export Tasks: CSV with Name, Father Name, Email, Password")
    logger.info("✅ Auto Delete Messages: All messages deleted after 72 hours")
    logger.info("✅ Referral Settings: Admin can edit bonus and percentage")
    logger.info("✅ Milestone Bonuses: Admin can enable/disable and broadcast")
    logger.info("✅ Admin List: Fixed display issue")
    logger.info("✅ Pending Approval List: VIEW ONLY - no approve/reject buttons")
    logger.info("✅ Pending Tasks: Has approve/reject buttons")
    logger.info("✅ OTP Management: Toggle ON/OFF and Generate OTP for users")
    logger.info("✅ Resend Code: Fixed OTP resend functionality")
    logger.info("✅ Cancel Buttons: Fixed during OTP phase")
    logger.info("✅ Payout Channel: Admins can change payout channel")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()