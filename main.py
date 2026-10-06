import os
import time
import random
import asyncio
import sqlite3
import logging
import hashlib
import re
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime, timezone
from typing import Optional

from aiohttp import web
from dotenv import load_dotenv
import discord
from discord.ext import commands, tasks

# ============================================================
# EOW GLOBAL ECONOMY + LEVELING + MODERATION BOT
# Prefixes: $command  OR  eow command (case-insensitive)
# Global economy is shared across every server using this bot.
# ============================================================

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "economy.db")
PORT = int(os.getenv("PORT", "10000"))
PREFIX = "$"
ALT_PREFIX_NAME = "eow"

# Economy
DAILY_REWARD = 1_000
WORK_MIN = 300
WORK_MAX = 1_000
DAILY_COOLDOWN = 24 * 60 * 60
WORK_COOLDOWN = 6 * 60 * 60
MAX_AMOUNT = 10**18

# XP
XP_COOLDOWN = 10
REPEAT_MESSAGE_COOLDOWN = 60
MIN_MESSAGE_LENGTH = 3
LEVEL_REWARD_BASE = 100
LEVEL_REWARD_STEP = 50
LEVEL_MILESTONES = {1: 20, 2: 40, 3: 100}

# Live systems
BALTOP_REFRESH = 45
STATUS_REFRESH = 25

# General cooldowns
DICE_COOLDOWN = 3
MINES_COOLDOWN = 3
DUEL_COOLDOWN = 3

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("eow-bot")

# ============================================================
# PREFIX
# ============================================================

def dynamic_prefix(_bot, message: discord.Message):
    """Allow both $command and eow command, with EOW/Eow/eow casing accepted."""
    content = message.content.lstrip()
    if len(content) >= 3 and content[:3].lower() == ALT_PREFIX_NAME:
        if len(content) == 3 or content[3].isspace():
            # Return the exact casing typed by the user so Eow/eow/EOW all work.
            return [PREFIX, content[:3] + " "]
    return PREFIX


intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True

bot = commands.Bot(
    command_prefix=dynamic_prefix,
    intents=intents,
    case_insensitive=True,
    strip_after_prefix=True,
    help_command=None,
)

# ============================================================
# DATABASE
# ============================================================

os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA foreign_keys=ON")
db_lock = asyncio.Lock()


def init_db():
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            coins INTEGER NOT NULL DEFAULT 0,
            bank INTEGER NOT NULL DEFAULT 0,
            level INTEGER NOT NULL DEFAULT 0,
            level_messages INTEGER NOT NULL DEFAULT 0,
            total_messages INTEGER NOT NULL DEFAULT 0,
            daily_last INTEGER NOT NULL DEFAULT 0,
            work_last INTEGER NOT NULL DEFAULT 0,
            xp_last INTEGER NOT NULL DEFAULT 0,
            last_message_hash TEXT NOT NULL DEFAULT '',
            last_message_at INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            amount INTEGER NOT NULL,
            related_user_id INTEGER,
            balance_after INTEGER NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS baltop_config (
            guild_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS level_config (
            guild_id INTEGER PRIMARY KEY,
            channel_id INTEGER,
            enabled INTEGER NOT NULL DEFAULT 1
        );
        """
    )


init_db()

# Safe migrations for older DBs.
for column_sql in (
    "ALTER TABLE users ADD COLUMN bank INTEGER NOT NULL DEFAULT 0",
):
    try:
        db.execute(column_sql)
    except sqlite3.OperationalError:
        pass

# ============================================================
# DATABASE HELPERS
# ============================================================

async def ensure_user(user_id: int):
    async with db_lock:
        db.execute(
            "INSERT OR IGNORE INTO users(user_id, created_at) VALUES (?, ?)",
            (user_id, int(time.time())),
        )


async def get_user(user_id: int):
    await ensure_user(user_id)
    async with db_lock:
        return db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()


async def change_balance(
    user_id: int,
    delta: int,
    tx_type: str,
    description: str = "",
    related_user_id: Optional[int] = None,
):
    """Atomic wallet balance change. Returns (success, new_balance)."""
    await ensure_user(user_id)
    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT coins FROM users WHERE user_id=?", (user_id,)).fetchone()
            old_balance = int(row["coins"])
            new_balance = old_balance + int(delta)
            if new_balance < 0 or new_balance > MAX_AMOUNT:
                db.execute("ROLLBACK")
                return False, old_balance

            db.execute("UPDATE users SET coins=? WHERE user_id=?", (new_balance, user_id))
            db.execute(
                """
                INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    user_id,
                    tx_type,
                    int(delta),
                    related_user_id,
                    new_balance,
                    description[:250],
                    int(time.time()),
                ),
            )
            db.execute("COMMIT")
            return True, new_balance
        except Exception:
            db.execute("ROLLBACK")
            raise


async def transfer(sender_id: int, receiver_id: int, amount: int):
    if sender_id == receiver_id:
        return False, "self", 0, 0
    if amount <= 0:
        return False, "amount", 0, 0
    if amount > MAX_AMOUNT:
        return False, "amount", 0, 0

    await ensure_user(sender_id)
    await ensure_user(receiver_id)

    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            sender = db.execute("SELECT coins FROM users WHERE user_id=?", (sender_id,)).fetchone()
            receiver = db.execute("SELECT coins FROM users WHERE user_id=?", (receiver_id,)).fetchone()
            sender_balance = int(sender["coins"])
            receiver_balance = int(receiver["coins"])

            if sender_balance < amount:
                db.execute("ROLLBACK")
                return False, "insufficient", sender_balance, receiver_balance

            new_sender = sender_balance - amount
            new_receiver = receiver_balance + amount
            if new_receiver > MAX_AMOUNT:
                db.execute("ROLLBACK")
                return False, "amount", sender_balance, receiver_balance

            now = int(time.time())
            db.execute("UPDATE users SET coins=? WHERE user_id=?", (new_sender, sender_id))
            db.execute("UPDATE users SET coins=? WHERE user_id=?", (new_receiver, receiver_id))

            db.execute(
                """
                INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    sender_id,
                    "payment_sent",
                    -amount,
                    receiver_id,
                    new_sender,
                    f"Sent {amount:,} coins",
                    now,
                ),
            )
            db.execute(
                """
                INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    receiver_id,
                    "payment_received",
                    amount,
                    sender_id,
                    new_receiver,
                    f"Received {amount:,} coins",
                    now,
                ),
            )
            db.execute("COMMIT")
            return True, "ok", new_sender, new_receiver
        except Exception:
            db.execute("ROLLBACK")
            raise


async def move_wallet_bank(user_id: int, amount: int, mode: str):
    await ensure_user(user_id)
    if amount <= 0:
        return False, 0, 0

    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT coins, bank FROM users WHERE user_id=?", (user_id,)).fetchone()
            wallet = int(row["coins"])
            bank = int(row["bank"])

            if mode == "deposit":
                if wallet < amount:
                    db.execute("ROLLBACK")
                    return False, wallet, bank
                wallet -= amount
                bank += amount
                tx_amount = -amount
                tx_type = "deposit"
            else:
                if bank < amount:
                    db.execute("ROLLBACK")
                    return False, wallet, bank
                wallet += amount
                bank -= amount
                tx_amount = amount
                tx_type = "withdraw"

            if wallet < 0 or bank < 0 or wallet > MAX_AMOUNT or bank > MAX_AMOUNT:
                db.execute("ROLLBACK")
                return False, int(row["coins"]), int(row["bank"])

            db.execute("UPDATE users SET coins=?, bank=? WHERE user_id=?", (wallet, bank, user_id))
            db.execute(
                """
                INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (user_id, tx_type, tx_amount, None, wallet, f"{mode.title()} {amount:,} coins", int(time.time())),
            )
            db.execute("COMMIT")
            return True, wallet, bank
        except Exception:
            db.execute("ROLLBACK")
            raise


async def timed_reward(user_id: int, column: str, cooldown: int, reward: int, tx_type: str, description: str):
    if column not in {"daily_last", "work_last"}:
        raise ValueError("Invalid cooldown column")
    await ensure_user(user_id)

    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute(
                f"SELECT coins,{column} AS last_claim FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()
            now = int(time.time())
            remaining = cooldown - (now - int(row["last_claim"]))
            if remaining > 0:
                db.execute("ROLLBACK")
                return False, int(row["coins"]), remaining

            new_balance = int(row["coins"]) + int(reward)
            if new_balance > MAX_AMOUNT:
                db.execute("ROLLBACK")
                return False, int(row["coins"]), 0

            db.execute(
                f"UPDATE users SET {column}=?,coins=? WHERE user_id=?",
                (now, new_balance, user_id),
            )
            db.execute(
                """
                INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (user_id, tx_type, int(reward), None, new_balance, description, now),
            )
            db.execute("COMMIT")
            return True, new_balance, 0
        except Exception:
            db.execute("ROLLBACK")
            raise


async def top_users(limit=10):
    async with db_lock:
        return db.execute(
            "SELECT user_id,coins,level FROM users WHERE coins>0 ORDER BY coins DESC,user_id ASC LIMIT ?",
            (limit,),
        ).fetchall()


async def recent_transactions(user_id: int, limit=10):
    await ensure_user(user_id)
    async with db_lock:
        return db.execute(
            "SELECT * FROM transactions WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()


async def get_baltop_configs():
    async with db_lock:
        return db.execute("SELECT * FROM baltop_config").fetchall()


async def save_baltop(guild_id: int, channel_id: int, message_id: int):
    async with db_lock:
        db.execute(
            """
            INSERT INTO baltop_config(guild_id,channel_id,message_id,updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(guild_id) DO UPDATE SET
            channel_id=excluded.channel_id,
            message_id=excluded.message_id,
            updated_at=excluded.updated_at
            """,
            (guild_id, channel_id, message_id, int(time.time())),
        )


async def save_level_config(guild_id: int, channel_id: Optional[int], enabled: bool):
    async with db_lock:
        db.execute(
            """
            INSERT INTO level_config(guild_id,channel_id,enabled)
            VALUES(?,?,?)
            ON CONFLICT(guild_id) DO UPDATE SET
            channel_id=excluded.channel_id,
            enabled=excluded.enabled
            """,
            (guild_id, channel_id, 1 if enabled else 0),
        )


async def get_level_config(guild_id: int):
    async with db_lock:
        return db.execute("SELECT * FROM level_config WHERE guild_id=?", (guild_id,)).fetchone()

# ============================================================
# LEVELING
# ============================================================

def required_messages(level: int) -> int:
    if level <= 0:
        return 0
    if level in LEVEL_MILESTONES:
        return LEVEL_MILESTONES[level]
    return 100 + (level - 3) * 25


def level_reward(level: int) -> int:
    return LEVEL_REWARD_BASE + (level - 1) * LEVEL_REWARD_STEP


async def process_xp(user_id: int, content: str):
    normalized = " ".join(content.strip().lower().split())
    if len(normalized) < MIN_MESSAGE_LENGTH:
        return None

    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    now = int(time.time())
    await ensure_user(user_id)

    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
            last_xp = int(row["xp_last"])
            last_hash = row["last_message_hash"]
            last_seen = int(row["last_message_at"])

            if last_hash == digest and now - last_seen < REPEAT_MESSAGE_COOLDOWN:
                db.execute("ROLLBACK")
                return None

            if now - last_xp < XP_COOLDOWN:
                db.execute(
                    "UPDATE users SET last_message_hash=?,last_message_at=? WHERE user_id=?",
                    (digest, now, user_id),
                )
                db.execute("COMMIT")
                return None

            old_level = int(row["level"])
            progress = int(row["level_messages"]) + 1
            total = int(row["total_messages"]) + 1
            new_level = old_level

            if old_level < 100 and progress >= required_messages(old_level + 1):
                new_level = old_level + 1

            db.execute(
                """
                UPDATE users SET level=?,level_messages=?,total_messages=?,xp_last=?,
                last_message_hash=?,last_message_at=? WHERE user_id=?
                """,
                (new_level, progress, total, now, digest, now, user_id),
            )
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise

    if new_level == old_level:
        return None

    reward = level_reward(new_level)
    ok, _ = await change_balance(user_id, reward, "level_up", f"Reached level {new_level}")
    if not ok:
        return None

    if new_level == 100:
        async with db_lock:
            db.execute(
                "UPDATE users SET level=0,level_messages=0 WHERE user_id=?",
                (user_id,),
            )
        return {"level": 100, "reward": reward, "reset": True, "progress": 0}

    return {"level": new_level, "reward": reward, "reset": False, "progress": progress}

# ============================================================
# FORMATTING / EMBEDS / ANIMATION
# ============================================================

def coins(value: int) -> str:
    return f"{int(value):,}"


def cooldown_text(seconds: int) -> str:
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def parse_amount(raw: Optional[str]) -> Optional[int]:
    """Supports 1000, 1,000, 100k, 2.5m, 1b and 'all'."""
    if raw is None:
        return None

    text = raw.strip().lower().replace(",", "").replace("_", "")
    if text == "all":
        return -1

    match = re.fullmatch(r"(\d+(?:\.\d+)?)([kmbt]?)", text)
    if not match:
        return None

    number_text, suffix = match.groups()
    multiplier = {"": 1, "k": 10**3, "m": 10**6, "b": 10**9, "t": 10**12}[suffix]
    try:
        value = (Decimal(number_text) * Decimal(multiplier)).quantize(Decimal("1"), rounding=ROUND_DOWN)
    except InvalidOperation:
        return None

    value_int = int(value)
    if value_int <= 0 or value_int > MAX_AMOUNT:
        return None
    return value_int


def error_embed(title: str, text: str):
    return discord.Embed(title=f"⚠️ {title}", description=text, color=discord.Color.red())


def info_embed(title: str, text: str):
    return discord.Embed(title=title, description=text, color=discord.Color.blurple())


def balance_embed(user, row):
    level = int(row["level"])
    progress = int(row["level_messages"])
    xp_text = "MAX" if level >= 100 else f"{progress:,}/{required_messages(level + 1):,}"

    e = discord.Embed(title="💰 Balance", color=discord.Color.blurple())
    e.set_author(name=str(user), icon_url=user.display_avatar.url)
    e.add_field(name="🪙 Wallet", value=f"**{coins(row['coins'])}**", inline=True)
    e.add_field(name="🏦 Bank", value=f"**{coins(row['bank'])}**", inline=True)
    e.add_field(name="📈 Level", value=f"**{level}**", inline=True)
    e.add_field(name="✨ XP", value=f"**{xp_text}**", inline=True)
    e.add_field(name="💬 Messages", value=f"**{int(row['total_messages']):,}**", inline=True)
    e.set_footer(text="Global economy • balance is shared across servers")
    return e


def baltop_embed(rows):
    e = discord.Embed(
        title="🏆 GLOBAL COIN LEADERBOARD",
        description="Top wallet balances across every server using this bot.",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    if not rows:
        e.description = "No users have coins yet."
        return e

    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for index, row in enumerate(rows, 1):
        rank = medals[index - 1] if index <= 3 else f"`#{index}`"
        lines.append(f"{rank} <@{int(row['user_id'])}> — **{coins(row['coins'])}** 🪙")
    e.add_field(name="Richest Players", value="\n".join(lines), inline=False)
    e.set_footer(text="Live global leaderboard")
    return e


def stage_embed(text: str):
    return discord.Embed(title="✨ EOW", description=text, color=discord.Color.blurple())


async def animated_embed(ctx, final_embed: discord.Embed, stages, delay: float = 0.22):
    """Edit one message through a few short stages, then show the final result."""
    message = await ctx.send(embed=stage_embed(stages[0]))
    for stage in stages[1:]:
        await asyncio.sleep(delay)
        try:
            await message.edit(embed=stage_embed(stage))
        except discord.HTTPException:
            return message
    await asyncio.sleep(delay)
    try:
        await message.edit(embed=final_embed)
    except discord.HTTPException:
        pass
    return message

# ============================================================
# HELP / PAGINATION
# ============================================================

HELP_PAGES = [
    (
        "💰 Economy",
        [
            ("$bal [@user]", "Show a global wallet + bank balance."),
            ("eow balance [@user]", "Same balance command using the EOW prefix."),
            ("$pay @user 100k", "Send 100,000 coins to another user."),
            ("$daily", "Claim the daily coin reward."),
            ("$work", "Work for a random coin reward."),
            ("$deposit 10k", "Move wallet coins to the bank."),
            ("$withdraw 10k", "Move bank coins back to the wallet."),
        ],
    ),
    (
        "🎮 Mini Games",
        [
            ("$dice", "Roll 2d6 with an animated result."),
            ("$dice 1d20", "Roll custom dice such as 1d20 or 3d6."),
            ("$mines", "Play a safe non-wager 3x3 minefield puzzle."),
            ("$duel @user", "Random non-wager duel for bragging rights."),
        ],
    ),
    (
        "🏆 Leaderboards",
        [
            ("$baltop", "Show the global richest-user leaderboard."),
            ("$leaderboard", "Alias for $baltop."),
            ("$setbaltop #channel", "Create/update a live leaderboard (admin)."),
        ],
    ),
    (
        "📈 Leveling",
        [
            ("$level [@user]", "Show level, XP progress and messages."),
            ("$setlevelchannel #channel", "Set the level-up announcement channel (admin)."),
            ("$levelchannel off", "Disable level-up announcements (admin)."),
        ],
    ),
    (
        "🧾 Transactions",
        [
            ("$transaction", "Show the latest 10 wallet transactions."),
            ("$transactions", "Alias for transaction history."),
            ("$transection", "Compatibility alias."),
        ],
    ),
    (
        "🛡️ Moderation",
        [
            ("$lockapps", "Disable public use of external/user-installed apps."),
            ("$unlockapps", "Allow public use of external/user-installed apps again."),
            ("$appguard", "Show external-app protection status."),
        ],
    ),
    (
        "ℹ️ Utility",
        [
            ("$help", "Open the animated button-based help menu."),
            ("$ping", "Show bot latency."),
            ("$userinfo [@user]", "Show basic user information."),
            ("$serverinfo", "Show basic server information."),
        ],
    ),
]


class HelpView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.page = 0

    def embed(self):
        title, commands_list = HELP_PAGES[self.page]
        e = discord.Embed(
            title=f"📚 EOW Help • {title}",
            description="Every command works with both `$` and the `eow` prefix.",
            color=discord.Color.blurple(),
        )
        for cmd, desc in commands_list:
            e.add_field(name=f"`{cmd}`", value=desc, inline=False)
        e.set_footer(text=f"Page {self.page + 1}/{len(HELP_PAGES)} • Only the opener can use these buttons")
        return e

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "❌ Only the user who opened this menu can control it.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def previous(self, interaction: discord.Interaction, _button: discord.ui.Button):
        self.page = (self.page - 1) % len(HELP_PAGES)
        await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next(self, interaction: discord.Interaction, _button: discord.ui.Button):
        self.page = (self.page + 1) % len(HELP_PAGES)
        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


# ============================================================
# SAFE MINES PUZZLE (NO WAGERING / NO CASH-OUT)
# ============================================================

class MinesView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.mine_positions = set(random.sample(range(9), 3))
        self.revealed = set()
        self.game_over = False
        self.gems_found = 0

        for index in range(9):
            button = discord.ui.Button(
                label="?",
                style=discord.ButtonStyle.secondary,
                custom_id=f"eow_mine_{index}",
                row=index // 3,
            )
            button.callback = self.make_callback(index)
            self.add_item(button)

    def make_callback(self, index: int):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.author_id:
                await interaction.response.send_message("❌ This puzzle belongs to someone else.", ephemeral=True)
                return
            if self.game_over:
                await interaction.response.send_message("This puzzle is already finished.", ephemeral=True)
                return
            if index in self.revealed:
                await interaction.response.send_message("That tile is already revealed.", ephemeral=True)
                return

            self.revealed.add(index)
            button = next(
                child for child in self.children
                if isinstance(child, discord.ui.Button) and child.custom_id == f"eow_mine_{index}"
            )

            if index in self.mine_positions:
                button.label = "💥"
                button.style = discord.ButtonStyle.danger
                self.game_over = True
                for child in self.children:
                    if isinstance(child, discord.ui.Button) and child.custom_id and child.custom_id.startswith("eow_mine_"):
                        child.disabled = True
                embed = discord.Embed(
                    title="💥 Mine Found",
                    description=f"{interaction.user.mention} hit a mine. Try `eow mines` again.",
                    color=discord.Color.red(),
                )
                await interaction.response.edit_message(embed=embed, view=self)
                return

            button.label = "💎"
            button.style = discord.ButtonStyle.success
            self.gems_found += 1

            safe_count = 9 - len(self.mine_positions)
            if self.gems_found >= safe_count:
                self.game_over = True
                for child in self.children:
                    if isinstance(child, discord.ui.Button):
                        child.disabled = True
                embed = discord.Embed(
                    title="🏆 Minefield Cleared",
                    description=f"{interaction.user.mention} found every safe tile! **{self.gems_found}/{safe_count}** gems.",
                    color=discord.Color.green(),
                )
            else:
                embed = discord.Embed(
                    title="💎 Safe Tile!",
                    description=f"Gems found: **{self.gems_found}/{safe_count}**\nKeep going — no coins are at risk.",
                    color=discord.Color.green(),
                )

            await interaction.response.edit_message(embed=embed, view=self)

        return callback

    async def on_timeout(self):
        self.game_over = True
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

# ============================================================
# STATUS / COMMANDS
# ============================================================

STATUS_TEXTS = [
    "$help • eow help",
    "$bal • eow balance",
    "$pay @user 100k",
    "$dice • safe games",
    "$baltop • global economy",
    "📈 Leveling enabled",
    "🛡️ External-app guard available",
]
status_index = 0


@bot.command(name="help", aliases=["commands"])
async def help_cmd(ctx):
    view = HelpView(ctx.author.id)
    e = view.embed()
    msg = await animated_embed(ctx, e, ("📚 Loading EOW help…", "🧩 Building command pages…"), 0.20)
    try:
        await msg.edit(embed=e, view=view)
    except discord.HTTPException:
        pass


@bot.command(name="bal", aliases=["balance", "wallet", "b"])
async def bal_cmd(ctx, member: Optional[discord.User] = None):
    target = member or ctx.author
    row = await get_user(target.id)
    await animated_embed(
        ctx,
        balance_embed(target, row),
        ("💰 Loading wallet…", "🏦 Checking bank…", "✨ Calculating XP…"),
        0.18,
    )


@bot.command(name="pay", aliases=["give", "transfer"])
async def pay_cmd(ctx, member: Optional[discord.User] = None, amount: Optional[str] = None):
    if member is None or amount is None:
        return await ctx.send(embed=error_embed("Usage", "Try `$pay @friend 100k` or `eow pay @friend 100k`."))
    if member.bot:
        return await ctx.send(embed=error_embed("Invalid User", "You can only pay a normal Discord user."))

    value = parse_amount(amount)
    if value == -1:
        row = await get_user(ctx.author.id)
        value = int(row["coins"])
    if value is None or value <= 0:
        return await ctx.send(embed=error_embed("Invalid Amount", "Use amounts like `1000`, `100k`, `2.5m`, or `all`."))

    ok, status, sender_balance, receiver_balance = await transfer(ctx.author.id, member.id, value)
    if not ok:
        if status == "self":
            return await ctx.send(embed=error_embed("Invalid Transfer", "You cannot pay yourself."))
        if status == "insufficient":
            return await ctx.send(embed=error_embed("Not Enough Coins", f"Your wallet has **{coins(sender_balance)}** 🪙."))
        return await ctx.send(embed=error_embed("Transfer Failed", "That amount is not valid or is too large."))

    e = discord.Embed(
        title="💸 Payment Sent",
        description=(
            f"{ctx.author.mention} sent **{coins(value)}** 🪙 to {member.mention}.\n\n"
            f"Your new wallet: **{coins(sender_balance)}** 🪙\n"
            f"Recipient wallet: **{coins(receiver_balance)}** 🪙"
        ),
        color=discord.Color.green(),
    )
    e.set_footer(text="Global transfer • works across servers")
    await animated_embed(ctx, e, ("💸 Preparing transfer…", "🔐 Checking balances…", "✅ Transfer complete!"), 0.20)


@bot.command(name="daily", aliases=["dailyreward"])
async def daily_cmd(ctx):
    ok, new_balance, remaining = await timed_reward(
        ctx.author.id,
        "daily_last",
        DAILY_COOLDOWN,
        DAILY_REWARD,
        "daily",
        "Daily reward",
    )
    if not ok:
        return await ctx.send(embed=error_embed("Daily Cooldown", f"Come back in **{cooldown_text(remaining)}**."))

    e = discord.Embed(
        title="🎁 Daily Reward Claimed",
        description=f"You received **{coins(DAILY_REWARD)}** 🪙!\nWallet: **{coins(new_balance)}** 🪙",
        color=discord.Color.green(),
    )
    await animated_embed(ctx, e, ("🎁 Opening reward…", "✨ Reward unlocked!", "💰 Updating wallet…"), 0.18)


@bot.command(name="work", aliases=["job"])
async def work_cmd(ctx):
    reward = random.randint(WORK_MIN, WORK_MAX)
    ok, new_balance, remaining = await timed_reward(
        ctx.author.id,
        "work_last",
        WORK_COOLDOWN,
        reward,
        "work",
        "Work reward",
    )
    if not ok:
        return await ctx.send(embed=error_embed("Work Cooldown", f"Come back in **{cooldown_text(remaining)}**."))

    e = discord.Embed(
        title="💼 Work Complete",
        description=f"You earned **{coins(reward)}** 🪙!\nWallet: **{coins(new_balance)}** 🪙",
        color=discord.Color.green(),
    )
    await animated_embed(ctx, e, ("💼 Working…", "🧮 Counting earnings…", "✅ Payment received!"), 0.18)


@bot.command(name="deposit", aliases=["dep"])
async def deposit_cmd(ctx, amount: Optional[str] = None):
    value = parse_amount(amount)
    if value == -1:
        row = await get_user(ctx.author.id)
        value = int(row["coins"])
    if value is None or value <= 0:
        return await ctx.send(embed=error_embed("Usage", "Use `$deposit 10k` or `$deposit all`."))

    ok, wallet, bank = await move_wallet_bank(ctx.author.id, value, "deposit")
    if not ok:
        return await ctx.send(embed=error_embed("Not Enough Wallet Coins", f"Wallet: **{coins(wallet)}** 🪙."))

    e = discord.Embed(
        title="🏦 Deposit Complete",
        description=f"Moved **{coins(value)}** 🪙 from wallet to bank.",
        color=discord.Color.green(),
    )
    e.add_field(name="🪙 Wallet", value=f"**{coins(wallet)}**", inline=True)
    e.add_field(name="🏦 Bank", value=f"**{coins(bank)}**", inline=True)
    await animated_embed(ctx, e, ("🏦 Opening bank…", "💰 Moving coins…", "✅ Deposit complete!"), 0.18)


@bot.command(name="withdraw", aliases=["with"])
async def withdraw_cmd(ctx, amount: Optional[str] = None):
    value = parse_amount(amount)
    if value == -1:
        row = await get_user(ctx.author.id)
        value = int(row["bank"])
    if value is None or value <= 0:
        return await ctx.send(embed=error_embed("Usage", "Use `$withdraw 10k` or `$withdraw all`."))

    ok, wallet, bank = await move_wallet_bank(ctx.author.id, value, "withdraw")
    if not ok:
        return await ctx.send(embed=error_embed("Not Enough Bank Coins", f"Bank: **{coins(bank)}** 🪙."))

    e = discord.Embed(
        title="💳 Withdrawal Complete",
        description=f"Moved **{coins(value)}** 🪙 from bank to wallet.",
        color=discord.Color.green(),
    )
    e.add_field(name="🪙 Wallet", value=f"**{coins(wallet)}**", inline=True)
    e.add_field(name="🏦 Bank", value=f"**{coins(bank)}**", inline=True)
    await animated_embed(ctx, e, ("🏦 Opening bank…", "💸 Moving coins…", "✅ Withdrawal complete!"), 0.18)


@bot.command(name="dice", aliases=["roll"])
@commands.cooldown(1, DICE_COOLDOWN, commands.BucketType.user)
async def dice_cmd(ctx, notation: Optional[str] = None):
    """Safe dice roller. Examples: $dice, $dice 1d20, $dice 3d6."""
    notation = (notation or "2d6").lower().replace(" ", "")
    match = re.fullmatch(r"(\d{1,2})d(\d{1,4})", notation)
    if not match:
        return await ctx.send(embed=error_embed("Invalid Dice", "Use `$dice`, `$dice 1d20` or `$dice 3d6`."))

    count = int(match.group(1))
    sides = int(match.group(2))
    if count < 1 or count > 20 or sides < 2 or sides > 1000:
        return await ctx.send(embed=error_embed("Invalid Dice", "Dice count must be 1–20 and sides 2–1000."))

    rolls = [random.randint(1, sides) for _ in range(count)]
    total = sum(rolls)
    roll_text = ", ".join(map(str, rolls))

    e = discord.Embed(
        title="🎲 DICE RESULT",
        description=f"{ctx.author.mention} rolled **{notation}**\n\nRolls: **{roll_text}**\nTotal: **{total}**",
        color=discord.Color.blurple(),
    )
    e.set_footer(text="No wagering • pure dice roll")
    await animated_embed(ctx, e, ("🎲 Loading dice…", "🎲 Shaking…", "🎲 Rolling…"), 0.22)


@bot.command(name="mines", aliases=["mine", "minefield"])
@commands.cooldown(1, MINES_COOLDOWN, commands.BucketType.user)
async def mines_cmd(ctx):
    """Interactive non-wagering 3x3 minefield puzzle."""
    view = MinesView(ctx.author.id)
    e = discord.Embed(
        title="💎 EOW Minefield",
        description="Find all safe tiles. Avoid the hidden mines. **No coins are wagered or lost.**",
        color=discord.Color.blurple(),
    )
    e.set_footer(text="60 second puzzle • only the opener can click")
    msg = await animated_embed(ctx, e, ("💎 Building minefield…", "🔐 Hiding tiles…", "✨ Puzzle ready!"), 0.18)
    try:
        await msg.edit(embed=e, view=view)
    except discord.HTTPException:
        pass


@bot.command(name="duel", aliases=["fight"])
@commands.cooldown(1, DUEL_COOLDOWN, commands.BucketType.user)
async def duel_cmd(ctx, member: Optional[discord.Member] = None):
    if ctx.guild is None:
        return await ctx.send(embed=error_embed("Server Only", "Duel needs to be used inside a server."))
    if member is None or member.bot or member.id == ctx.author.id:
        return await ctx.send(embed=error_embed("Usage", "Try `$duel @friend`."))

    winner = random.choice([ctx.author, member])
    loser = member if winner.id == ctx.author.id else ctx.author
    e = discord.Embed(
        title="⚔️ DUEL COMPLETE",
        description=f"{winner.mention} wins against {loser.mention}!\n\n🏆 Reward: bragging rights",
        color=discord.Color.gold(),
    )
    e.set_footer(text="No wagering")
    await animated_embed(ctx, e, ("⚔️ Duel starting…", "⚡ Clash!", "🏆 Winner decided!"), 0.20)


@bot.command(name="transaction", aliases=["transactions", "transection", "tx"])
async def transaction_cmd(ctx):
    rows = await recent_transactions(ctx.author.id)
    e = discord.Embed(title="🧾 Recent Transactions", color=discord.Color.blurple())
    if not rows:
        e.description = "No transactions yet."
    else:
        lines = []
        for row in rows:
            amount = int(row["amount"])
            sign = "+" if amount > 0 else ""
            related = f" • <@{int(row['related_user_id'])}>" if row["related_user_id"] else ""
            lines.append(
                f"**{row['type']}** `{sign}{coins(amount)}` 🪙 • <t:{int(row['created_at'])}:R>{related}"
            )
        e.description = "\n".join(lines)
    e.set_footer(text="Global transaction history")
    await animated_embed(ctx, e, ("🧾 Loading transactions…", "🔎 Checking history…"), 0.18)


@bot.command(name="baltop", aliases=["leaderboard", "rich", "top"])
async def baltop_cmd(ctx):
    await animated_embed(
        ctx,
        baltop_embed(await top_users()),
        ("🏆 Loading leaderboard…", "💰 Ranking wallets…"),
        0.20,
    )


@bot.command(name="level", aliases=["lvl"])
async def level_cmd(ctx, member: Optional[discord.User] = None):
    target = member or ctx.author
    row = await get_user(target.id)
    level = int(row["level"])
    progress = int(row["level_messages"])
    total = int(row["total_messages"])
    needed = 0 if level >= 100 else required_messages(level + 1)

    e = discord.Embed(title="📈 Level Profile", color=discord.Color.green())
    e.set_author(name=str(target), icon_url=target.display_avatar.url)
    e.add_field(name="Level", value=f"**{level}**", inline=True)
    e.add_field(name="XP", value=f"**MAX**" if level >= 100 else f"**{progress:,}/{needed:,}**", inline=True)
    e.add_field(name="Messages", value=f"**{total:,}**", inline=True)
    e.add_field(name="Next Reward", value="**Level reward system active**", inline=False)
    await animated_embed(ctx, e, ("📈 Loading level…", "✨ Calculating progress…"), 0.18)


# ============================================================
# BASIC UTILITY COMMANDS
# ============================================================

@bot.command(name="ping")
async def ping_cmd(ctx):
    latency = round(bot.latency * 1000)
    e = discord.Embed(title="🏓 Pong!", description=f"Latency: **{latency}ms**", color=discord.Color.green())
    await animated_embed(ctx, e, ("🏓 Pinging…", "📡 Checking latency…"), 0.15)


@bot.command(name="userinfo", aliases=["user"])
async def userinfo_cmd(ctx, member: Optional[discord.Member] = None):
    if ctx.guild is None and member is None:
        target = ctx.author
    else:
        target = member or ctx.author

    e = discord.Embed(title="👤 User Information", color=discord.Color.blurple())
    e.set_thumbnail(url=target.display_avatar.url)
    e.add_field(name="User", value=f"{target.mention}\n`{target.id}`", inline=True)
    e.add_field(name="Created", value=f"<t:{int(target.created_at.timestamp())}:D>", inline=True)
    if isinstance(target, discord.Member):
        e.add_field(name="Joined", value=f"<t:{int(target.joined_at.timestamp())}:D>", inline=True)
        e.add_field(name="Top Role", value=target.top_role.mention, inline=True)
    await animated_embed(ctx, e, ("👤 Loading user…", "🔎 Reading profile…"), 0.18)


@bot.command(name="serverinfo", aliases=["server"])
async def serverinfo_cmd(ctx):
    if ctx.guild is None:
        return await ctx.send(embed=error_embed("Server Only", "This command needs a server."))

    g = ctx.guild
    e = discord.Embed(title="🏠 Server Information", color=discord.Color.blurple())
    if g.icon:
        e.set_thumbnail(url=g.icon.url)
    e.add_field(name="Name", value=g.name, inline=True)
    e.add_field(name="Members", value=str(g.member_count), inline=True)
    e.add_field(name="Channels", value=str(len(g.channels)), inline=True)
    e.add_field(name="Created", value=f"<t:{int(g.created_at.timestamp())}:D>", inline=True)
    await animated_embed(ctx, e, ("🏠 Loading server…", "📊 Counting channels…"), 0.18)

# ============================================================
# MODERATION: EXTERNAL APP GUARD
# ============================================================

@bot.command(name="lockapps")
@commands.guild_only()
@commands.has_guild_permissions(manage_guild=True)
@commands.bot_has_guild_permissions(manage_roles=True)
async def lockapps_cmd(ctx):
    role = ctx.guild.default_role
    perms = role.permissions
    perms.use_external_apps = False

    try:
        await role.edit(permissions=perms, reason=f"External Apps Guard enabled by {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed(
            "Permission Error",
            "I need **Manage Roles** and must be able to edit the @everyone role.",
        ))
    except discord.HTTPException:
        return await ctx.send(embed=error_embed("Discord Error", "Discord rejected the permission update."))

    e = discord.Embed(
        title="🛡️ External Apps Guard • ON",
        description="Public use of external/user-installed apps is now disabled for members using this default permission.",
        color=discord.Color.green(),
    )
    e.add_field(name="Normal members", value="🔒 External apps blocked", inline=True)
    e.add_field(name="Server-installed apps", value="Not changed", inline=True)
    e.set_footer(text=f"Changed by {ctx.author}")
    await animated_embed(ctx, e, ("🛡️ Checking permissions…", "🔐 Updating @everyone…", "✅ App guard enabled!"), 0.18)


@bot.command(name="unlockapps")
@commands.guild_only()
@commands.has_guild_permissions(manage_guild=True)
@commands.bot_has_guild_permissions(manage_roles=True)
async def unlockapps_cmd(ctx):
    role = ctx.guild.default_role
    perms = role.permissions
    perms.use_external_apps = True

    try:
        await role.edit(permissions=perms, reason=f"External Apps Guard disabled by {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed(
            "Permission Error",
            "I need **Manage Roles** and must be able to edit the @everyone role.",
        ))
    except discord.HTTPException:
        return await ctx.send(embed=error_embed("Discord Error", "Discord rejected the permission update."))

    e = discord.Embed(
        title="🔓 External Apps Guard • OFF",
        description="Public use of external/user-installed apps is allowed again according to Discord permissions.",
        color=discord.Color.orange(),
    )
    e.set_footer(text=f"Changed by {ctx.author}")
    await animated_embed(ctx, e, ("🔓 Checking permissions…", "🔧 Updating @everyone…", "✅ App guard disabled!"), 0.18)


@bot.command(name="appguard")
@commands.guild_only()
@commands.has_guild_permissions(manage_guild=True)
async def appguard_cmd(ctx):
    enabled = not ctx.guild.default_role.permissions.use_external_apps
    e = discord.Embed(
        title="🛡️ External Apps Guard",
        description=(
            "**ON** — normal members cannot publicly use external/user-installed apps."
            if enabled
            else
            "**OFF** — external/user-installed apps are allowed according to Discord permissions."
        ),
        color=discord.Color.green() if enabled else discord.Color.orange(),
    )
    e.add_field(name="Public external apps", value="🔒 Blocked" if enabled else "🔓 Allowed", inline=True)
    e.add_field(name="Server-installed apps", value="Not changed", inline=True)
    e.set_footer(text="Use $lockapps / eow lockapps to change it")
    await animated_embed(ctx, e, ("🛡️ Checking app guard…",), 0.10)

# ============================================================
# ADMIN / LIVE LEADERBOARD / LEVEL CHANNEL
# ============================================================

@bot.command(name="setbaltop")
@commands.guild_only()
@commands.has_guild_permissions(manage_guild=True)
async def setbaltop_cmd(ctx, channel: Optional[discord.TextChannel] = None):
    channel = channel or ctx.channel
    me = ctx.guild.me
    if me is None or not channel.permissions_for(me).send_messages:
        return await ctx.send(embed=error_embed("Missing Permission", f"I cannot send messages in {channel.mention}."))

    msg = await channel.send(embed=baltop_embed(await top_users()))
    await save_baltop(ctx.guild.id, channel.id, msg.id)
    e = discord.Embed(
        title="🏆 Live Baltop Enabled",
        description=f"Global leaderboard is now updating in {channel.mention}.",
        color=discord.Color.gold(),
    )
    await animated_embed(ctx, e, ("🏆 Preparing leaderboard…", "📡 Connecting live updater…", "✅ Live leaderboard enabled!"), 0.18)


@bot.command(name="setlevelchannel")
@commands.guild_only()
@commands.has_guild_permissions(manage_guild=True)
async def setlevelchannel_cmd(ctx, channel: Optional[discord.TextChannel] = None):
    channel = channel or ctx.channel
    await save_level_config(ctx.guild.id, channel.id, True)
    e = discord.Embed(
        title="📈 Level Channel Set",
        description=f"Level-up announcements will be sent in {channel.mention}.",
        color=discord.Color.green(),
    )
    await animated_embed(ctx, e, ("📈 Saving level settings…", "📡 Linking channel…", "✅ Level channel enabled!"), 0.18)


@bot.command(name="levelchannel")
@commands.guild_only()
@commands.has_guild_permissions(manage_guild=True)
async def levelchannel_cmd(ctx, mode: Optional[str] = None):
    if mode and mode.lower() == "off":
        await save_level_config(ctx.guild.id, None, False)
        e = discord.Embed(
            title="📈 Level Announcements Disabled",
            description="Level-up announcements are now disabled in this server.",
            color=discord.Color.orange(),
        )
        await animated_embed(ctx, e, ("📈 Updating settings…", "🔕 Disabling announcements…", "✅ Disabled!"), 0.18)
    else:
        await ctx.send(embed=error_embed("Usage", "Try `$levelchannel off` or `$setlevelchannel #channel`."))

# ============================================================
# LIVE LOOPS
# ============================================================

@tasks.loop(seconds=STATUS_REFRESH)
async def status_loop():
    global status_index
    text = STATUS_TEXTS[status_index % len(STATUS_TEXTS)]
    status_index += 1
    try:
        await bot.change_presence(
            status=discord.Status.online,
            activity=discord.Activity(type=discord.ActivityType.watching, name=text),
        )
    except Exception:
        log.exception("Status update failed")


@status_loop.before_loop
async def status_before():
    await bot.wait_until_ready()


@tasks.loop(seconds=BALTOP_REFRESH)
async def baltop_loop():
    configs = await get_baltop_configs()
    if not configs:
        return

    embed = baltop_embed(await top_users())
    for config in configs:
        guild = bot.get_guild(int(config["guild_id"]))
        if guild is None:
            continue
        channel = guild.get_channel(int(config["channel_id"]))
        if not isinstance(channel, discord.TextChannel):
            continue
        try:
            message = await channel.fetch_message(int(config["message_id"]))
            await message.edit(embed=embed)
        except discord.NotFound:
            try:
                new_msg = await channel.send(embed=embed)
                await save_baltop(guild.id, channel.id, new_msg.id)
            except discord.HTTPException:
                pass
        except (discord.Forbidden, discord.HTTPException):
            pass


@baltop_loop.before_loop
async def baltop_before():
    await bot.wait_until_ready()

# ============================================================
# LEVEL ANNOUNCEMENTS
# ============================================================

async def announce_level(message: discord.Message, result: dict):
    if message.guild is None:
        return

    config = await get_level_config(message.guild.id)
    if config is not None and not bool(config["enabled"]):
        return

    channel = message.channel
    if config is not None and config["channel_id"]:
        configured = message.guild.get_channel(int(config["channel_id"]))
        if isinstance(configured, discord.TextChannel):
            channel = configured

    if result["reset"]:
        e = discord.Embed(
            title="🏆 LEVEL 100 REACHED!",
            description=(
                f"{message.author.mention} reached **Level 100**!\n\n"
                f"🎁 Reward: **{coins(result['reward'])}** 🪙\n"
                "🔄 Level reset to **Level 0** while wallet and history stay safe."
            ),
            color=discord.Color.gold(),
        )
    else:
        e = discord.Embed(
            title="🎉 LEVEL UP!",
            description=(
                f"{message.author.mention} reached **Level {result['level']}**!\n\n"
                f"🎁 Reward: **{coins(result['reward'])}** 🪙\n"
                f"💬 Progress: **{result['progress']:,}** valid messages"
            ),
            color=discord.Color.green(),
        )

    try:
        await channel.send(embed=e)
    except discord.HTTPException:
        pass

# ============================================================
# EVENTS / ERRORS
# ============================================================

def is_bot_command_text(content: str) -> bool:
    stripped = content.lstrip()
    if stripped.startswith(PREFIX):
        return True
    if len(stripped) >= 3 and stripped[:3].lower() == ALT_PREFIX_NAME:
        return len(stripped) == 3 or stripped[3].isspace()
    return False


@bot.event
async def on_ready():
    log.info(
        "Logged in as %s (%s) | %d guild(s)",
        bot.user,
        bot.user.id if bot.user else "?",
        len(bot.guilds),
    )
    if not status_loop.is_running():
        status_loop.start()
    if not baltop_loop.is_running():
        baltop_loop.start()
    await bot.change_presence(
        status=discord.Status.online,
        activity=discord.Activity(type=discord.ActivityType.watching, name=STATUS_TEXTS[0]),
    )


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.webhook_id:
        return

    if message.guild is not None and not is_bot_command_text(message.content):
        try:
            result = await process_xp(message.author.id, message.content)
            if result:
                await announce_level(message, result)
        except Exception:
            log.exception("XP processing error")

    await bot.process_commands(message)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.NoPrivateMessage):
        return await ctx.send(embed=error_embed("Server Only", "This command can only be used in a server."))
    if isinstance(error, commands.MissingPermissions):
        return await ctx.send(embed=error_embed("Permission Denied", "You need **Manage Server** permission."))
    if isinstance(error, commands.BotMissingPermissions):
        perms = ", ".join(error.missing_permissions).replace("manage_roles", "Manage Roles")
        return await ctx.send(embed=error_embed("Bot Permission Missing", f"I need: **{perms}**."))
    if isinstance(error, commands.MissingRequiredArgument):
        return await ctx.send(embed=error_embed("Missing Argument", "Check `$help` or `eow help` for the correct usage."))
    if isinstance(error, commands.BadArgument):
        return await ctx.send(embed=error_embed("Invalid Argument", "Check the command format in `$help`."))
    if isinstance(error, commands.CommandOnCooldown):
        return await ctx.send(embed=error_embed("Cooldown", f"Try again in **{cooldown_text(error.retry_after)}**."))

    original = getattr(error, "original", error)
    log.exception("Command error: %s", original)
    try:
        await ctx.send(embed=error_embed("Unexpected Error", "Something went wrong. Try again."))
    except discord.HTTPException:
        pass

# ============================================================
# RENDER HEALTH SERVER
# ============================================================

async def health_handler(_request):
    return web.json_response(
        {
            "status": "ok",
            "bot_ready": bot.is_ready(),
            "bot": str(bot.user) if bot.user else None,
            "guilds": len(bot.guilds),
        }
    )


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server running on port %s", PORT)
    return runner

# ============================================================
# START
# ============================================================

async def main():
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing. Add it to Render Environment Variables.")

    runner = await start_health_server()
    try:
        await bot.start(TOKEN)
    finally:
        await runner.cleanup()
        try:
            db.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
