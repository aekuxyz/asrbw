import discord
from discord.ext import commands, tasks
from discord.ui import View, Select, Button
from discord import SelectOption, File, ButtonStyle
import aiomysql
import asyncio
import datetime
import json
import random
from PIL import Image, ImageDraw, ImageFont, ImageOps
from io import BytesIO
import aiohttp
import uuid
from collections import defaultdict
import string
import re
import os # For file operations (game counter)
from typing import Union # Correct import for Union type hint

# --- Configuration Section ---
# IMPORTANT: Replace these placeholders with your actual values.
# You can set these as environment variables for better security in production.

# Discord Bot Token (GET THIS FROM Discord Developer Portal)
DISCORD_BOT_TOKEN = 'YOUR_DISCORD_BOT_TOKEN_HERE'

# MySQL Database Details (This is the database shared by the bot and MC server plugin)
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "asrbw_db" # Ensure this database is created and user has access
MYSQL_USER = "asrbw_user"
MYSQL_PASSWORD = "your_mysql_password"

# Discord Role IDs (GET THESE BY ENABLING DEVELOPER MODE IN DISCORD AND RIGHT-CLICKING ROLES)
MUTED_ROLE_ID = 123456789012345678 # Example ID: Create this role and deny it permissions in all channels
BANNED_ROLE_ID = 123456789012345678 # Example ID: For queue bans (if implemented)
FROZEN_ROLE_ID = 123456789012345678 # Example ID: For screenshare (if implemented)
PPP_MANAGER_ROLE_ID = 123456789012345678 # Role allowed to start PPP polls
HIGHEST_ADMIN_ROLE_ID = 123456789012345678 # Role that bypasses channel restrictions (e.g., Server Owner, Head Admin)
SCREENSHARER_ROLE_ID = 123456789012345678 # Role for screensharers (NOT general staff)

# New: Role for all registered users
REGISTERED_ROLE_ID = 123456789012345678 # Example ID: Role assigned upon successful registration

# New: ELO Rank Role IDs (Create these roles in your Discord server)
IRON_ROLE_ID = 123456789012345678
BRONZE_ROLE_ID = 123456789012345678
SILVER_ROLE_ID = 123456789012345678
GOLD_ROLE_ID = 123456789012345678
TOPAZ_ROLE_ID = 123456789012345678
PLATINUM_ROLE_ID = 123456789012345678

# Comprehensive list of roles considered "staff" for moderation commands
MODERATION_ROLES_IDS = [
    HIGHEST_ADMIN_ROLE_ID, # The highest admin role
    123456789012345678, # Example: Moderator Role ID
    123456789012345678, # Example: Administrator Role ID
    123456789012345678  # Example: Uito Role ID (newly added concept)
]

# Roles that can vote on strike polls (PUPS, PUGS, PREMIUM)
VOTER_ROLES_IDS = [
    123456789012345678, # PUPS Role ID
    123456789012345678, # PUGS Role ID
    123456789012345678  # PREMIUM Role ID
]

# Discord Channel IDs
GAME_LOGS_CHANNEL_ID = 123456789012345678
TICKET_LOGS_CHANNEL_ID = 123456789012345678
ALERTS_CHANNEL_ID = 123456789012345678
STRIKE_LOGS_CHANNEL_ID = 123456789012345678 # This channel will now also be used for mute/ban logs
SCREENSHARE_LOGS_CHANNEL_ID = 123456789012345678
GAMES_RESULTS_CHANNEL_ID = 123456789012345678 # New: Channel to post completed game results images

# Specific Channel IDs for Commands (for restrictions)
REGISTER_CHANNEL_ID = 123456789012345678 # Channel where =register can ONLY be used
STRIKE_REQUEST_CHANNEL_ID = 123456789012345678 # Channel where =requeststrike can be used
POLL_CHANNEL_ID = 123456789012345678 # Channel where =poll can be used
TICKET_CHANNEL_ID = 123456789012345678 # Channel where =ticket command is used to create tickets
STAFF_UPDATE_CHANNEL_ID = 123456789012345678 # Channel to log staff role changes

# Queue Voice Channel IDs (where players queue up)
QUEUE_3V3_VOICE_CHANNEL_ID = 123456789012345678 # Players join this VC for 3v3 queue
QUEUE_4V4_VOICE_CHANNEL_ID = 123456789012345678 # Players join this VC for 4v4 queue
PPP_3V3_VOICE_CHANNEL_ID = 123456789012345678 # New: Voice channel for 3v3 PPP queue
PPP_4V4_VOICE_CHANNEL_ID = 123456789012345678 # New: Voice channel for 4v4 PPP queue


# Discord Category IDs
TICKET_CATEGORY_ID = 123456789012345678
SCREENSHARE_CATEGORY_ID = 123456789012345678
GAME_CATEGORY_ID = 123456789012345678 # Text channels for games will be here
GAME_VOICE_CATEGORY_ID = 123456789012345678 # New: Dedicated category for game voice channels

# Main Discord Server ID (Crucial for nickname updates and member fetching)
MAIN_GUILD_ID = 123456789012345678

# ELO Configuration (Unified ELO)
DEFAULT_ELO = 0.0 # Changed from 1000.0 to 0.0
ELO_K_FACTOR = 32 # This might not be directly used if ELO is rank-based

# Rank ELO Thresholds and Values (Adjusted for single ELO)
ELO_THRESHOLDS = {
    "Iron": {"min_elo": 0, "max_elo": 99, "win_gain": 25, "loss_deduct": 15, "mvp_bonus": 10, "role_id": IRON_ROLE_ID},
    "Bronze": {"min_elo": 100, "max_elo": 199, "win_gain": 28, "loss_deduct": 17, "mvp_bonus": 12, "role_id": BRONZE_ROLE_ID},
    "Silver": {"min_elo": 200, "max_elo": 299, "win_gain": 32, "loss_deduct": 20, "mvp_bonus": 15, "role_id": SILVER_ROLE_ID},
    "Gold": {"min_elo": 300, "max_elo": 499, "win_gain": 35, "loss_deduct": 22, "mvp_bonus": 18, "role_id": GOLD_ROLE_ID},
    "Topaz": {"min_elo": 500, "max_elo": 599, "win_gain": 40, "loss_deduct": 25, "mvp_bonus": 20, "role_id": TOPAZ_ROLE_ID},
    "Platinum": {"min_elo": 600, "max_elo": 999999, "win_gain": 45, "loss_deduct": 28, "mvp_bonus": 25, "role_id": PLATINUM_ROLE_ID},
}

# List of all rank role IDs for easy iteration when updating roles
ALL_RANK_ROLE_IDS = [data["role_id"] for data in ELO_THRESHOLDS.values()]


# Queue Configuration
MIN_PLAYERS_3V3 = 6 # 2 Captains + 4 players = 6
MIN_PLAYERS_4V4 = 8 # 2 Captains + 6 players = 8

# Available Bedwars Maps (MUST MATCH NAMES CONFIGURED IN YOUR MINECRAFT PLUGIN)
AVAILABLE_MAPS = ["Ares", "Dragon's Nest", "Playgrounds"]

# Font for Player Cards (Ensure this .ttf file is in the same directory as bot.py)
# You might need to place an actual .ttf file like 'arial.ttf' in the same directory as your bot script.
FONT_PATH = "arial.ttf" 

# Registration Code Expiry
REGISTRATION_CODE_EXPIRY_MINUTES = 5

# File to store the last game number
GAME_CHANNEL_COUNTER_FILE = "game_counter.txt"

# --- Global State (In-memory, will reset on bot restart) ---
queues = {
    "3v3": [],
    "4v4": [],
    "ppp_3v3": [], # Intended for PPP users only
    "ppp_4v4": []  # Intended for PPP users only
}
active_games = {} # Stores game_id -> {text_channel_id, voice_channel_lobby_id, team_a_vc_id, team_b_vc_id, players: {discord_id: team}}
active_tickets = {}
active_screenshares = {}

# Party System: {owner_id: {"members": [member_id], "invite_pending": {invited_id: timestamp}}}
parties = defaultdict(lambda: {"members": [], "invite_pending": {}})
# Reverse lookup: {member_id: owner_id} for quick party lookup
player_party_map = {}

# --- Discord Bot Setup ---
intents = discord.Intents.all()
bot = commands.Bot(command_prefix='=', intents=intents)

# --- Game Counter Management ---
def get_next_game_number():
    """Reads, increments, and writes the game number from a file."""
    game_number = 0
    if os.path.exists(GAME_CHANNEL_COUNTER_FILE):
        with open(GAME_CHANNEL_COUNTER_FILE, 'r') as f:
            try:
                game_number = int(f.read().strip())
            except ValueError:
                game_number = 0 # Reset if file content is invalid
    
    game_number += 1
    with open(GAME_CHANNEL_COUNTER_FILE, 'w') as f:
        f.write(str(game_number))
    return game_number

# --- MySQL Connection Pool (Using aiomysql for async operations) ---
async def get_mysql_connection():
    """Establishes and returns a new MySQL database connection using aiomysql."""
    try:
        return await aiomysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DATABASE, # Changed 'database' to 'db' for aiomysql
            autocommit=True # aiomysql defaults to autocommit=False, set to True for simpler operations
        )
    except aiomysql.Error as err:
        print(f"Error connecting to MySQL with aiomysql: {err}")
        await log_alert(f"Failed to connect to MySQL with aiomysql: {err}")
        return None

async def ensure_db_connection():
    """Ensures the 'accounts' table exists in the database with necessary columns."""
    conn = None
    try:
        conn = await get_mysql_connection()
        if conn:
            # Use conn.cursor() as an async context manager
            async with conn.cursor() as cursor:
                # Create accounts table if it doesn't exist
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        discord_id VARCHAR(255) PRIMARY KEY,
                        minecraft_uuid VARCHAR(255) UNIQUE,
                        minecraft_username VARCHAR(255),
                        registration_code VARCHAR(10),
                        registration_timestamp BIGINT,
                        elo FLOAT DEFAULT 0.0, -- Unified ELO, now defaults to 0.0
                        wins INT DEFAULT 0,
                        losses INT DEFAULT 0,
                        games_played INT DEFAULT 0,
                        wlr FLOAT DEFAULT 0.0,
                        kills INT DEFAULT 0,
                        deaths INT DEFAULT 0,
                        beds_broken INT DEFAULT 0,
                        final_kills INT DEFAULT 0,
                        mvp_count INT DEFAULT 0,
                        strikes INT DEFAULT 0,
                        game_history JSON DEFAULT (JSON_ARRAY()),
                        muted_until BIGINT NULL,    -- For temporary mutes
                        banned_until BIGINT NULL    -- For temporary bans
                    )
                """)
                # Drop old columns if they exist (for migration)
                try:
                    await cursor.execute("ALTER TABLE accounts DROP COLUMN IF EXISTS elo_3v3")
                    print("Dropped 'elo_3v3' column.")
                except aiomysql.Error as err:
                    # This error handling is for when the column doesn't exist, which is fine
                    if "Unknown column 'elo_3v3'" not in str(err):
                        print(f"Error dropping elo_3v3: {err}")
                        await log_alert(f"Error dropping elo_3v3: {err}")
                try:
                    await cursor.execute("ALTER TABLE accounts DROP COLUMN IF EXISTS elo_4v4")
                    print("Dropped 'elo_4v4' column.")
                except aiomysql.Error as err:
                    if "Unknown column 'elo_4v4'" not in str(err):
                        print(f"Error dropping elo_4v4: {err}")
                        await log_alert(f"Error dropping elo_4v4: {err}")
                try:
                    await cursor.execute("ALTER TABLE accounts DROP COLUMN IF EXISTS show_elo_prefix")
                    print("Dropped 'show_elo_prefix' column.")
                except aiomysql.Error as err:
                    if "Unknown column 'show_elo_prefix'" not in str(err):
                        print(f"Error dropping show_elo_prefix: {err}")
                        await log_alert(f"Error dropping show_elo_prefix: {err}")
                
                # Add new columns if they don't exist (using IF NOT EXISTS for safety)
                try:
                    await cursor.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS muted_until BIGINT NULL")
                    print("Added 'muted_until' column.")
                except aiomysql.Error as err:
                    if "Duplicate column name 'muted_until'" not in str(err):
                        print(f"Error adding muted_until: {err}")
                        await log_alert(f"Error adding muted_until: {err}")
                try:
                    await cursor.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS banned_until BIGINT NULL")
                    print("Added 'banned_until' column.")
                except aiomysql.Error as err:
                    if "Duplicate column name 'banned_until'" not in str(err):
                        print(f"Error adding banned_until: {err}")
                        await log_alert(f"Error adding banned_until: {err}")

            # No explicit conn.commit() needed if autocommit=True in connect
            print("MySQL 'accounts' table ensured and updated.")
        else:
            print("Could not establish MySQL connection to ensure 'accounts' table.")
    except aiomysql.Error as err:
        print(f"Error ensuring 'accounts' table: {err}")
        await log_alert(f"Error ensuring 'accounts' table: {err}")
    finally:
        if conn:
            conn.close() # Close the connection

# --- Helper Functions for Database Interaction ---

async def execute_query(query, params=None, fetchone=False, fetchall=False):
    """Executes a MySQL query and optionally fetches results."""
    conn = None
    try:
        conn = await get_mysql_connection()
        if not conn:
            return None

        # aiomysql.cursors.DictCursor for dictionary results
        async with conn.cursor(aiomysql.cursors.DictCursor) as cursor:
            await cursor.execute(query, params)
            # No explicit conn.commit() needed if autocommit=True in connect

            if fetchone:
                return await cursor.fetchone()
            if fetchall:
                return await cursor.fetchall()
            return True # For INSERT/UPDATE/DELETE

    except aiomysql.Error as err:
        print(f"MySQL Error: {err} in query: {query} with params: {params}")
        await log_alert(f"MySQL Error: {err} in query: {query}")
        return None
    finally:
        if conn:
            conn.close()

async def get_minecraft_uuid_from_discord_id(discord_id: str):
    """Fetches a Minecraft UUID given a Discord ID from MySQL (from accounts table)."""
    result = await execute_query(
        "SELECT minecraft_uuid, minecraft_username FROM accounts WHERE discord_id = %s",
        (discord_id,), fetchone=True
    )
    if result:
        return result['minecraft_uuid'], result['minecraft_username']
    return None, None

async def get_discord_id_from_minecraft_uuid(minecraft_uuid: str):
    """Fetches a Discord ID given a Minecraft UUID from MySQL (from accounts table)."""
    result = await execute_query(
        "SELECT discord_id FROM accounts WHERE minecraft_uuid = %s",
        (minecraft_uuid,), fetchone=True
    )
    if result:
        return result['discord_id']
    return None

async def get_player_stats_from_db(discord_id: str):
    """Fetches player statistics from MySQL using Discord ID."""
    return await execute_query(
        "SELECT * FROM accounts WHERE discord_id = %s",
        (discord_id,), fetchone=True
    )

async def register_user_in_db(discord_id: str, minecraft_uuid: str, minecraft_username: str):
    """Registers a user in the database or updates their UUID if already registered."""
    try:
        existing_account = await execute_query(
            "SELECT * FROM accounts WHERE discord_id = %s", (discord_id,), fetchone=True
        )

        if existing_account:
            await execute_query(
                "UPDATE accounts SET minecraft_uuid = %s, minecraft_username = %s, registration_code = NULL, registration_timestamp = NULL WHERE discord_id = %s",
                (minecraft_uuid, minecraft_username, discord_id)
            )
            print(f"Updated Discord ID {discord_id} with Minecraft UUID {minecraft_uuid} and username {minecraft_username}.")
        else:
            await execute_query(
                "INSERT INTO accounts (discord_id, minecraft_uuid, minecraft_username) VALUES (%s, %s, %s)",
                (discord_id, minecraft_uuid, minecraft_username)
            )
            print(f"Registered new user: Discord ID {discord_id}, Minecraft UUID {minecraft_uuid}, Username {minecraft_username}.")

        guild = bot.get_guild(MAIN_GUILD_ID)
        if guild:
            member = guild.get_member(int(discord_id))
            if member:
                # Assign Registered role
                registered_role = guild.get_role(REGISTERED_ROLE_ID)
                if registered_role and registered_role not in member.roles:
                    try:
                        await member.add_roles(registered_role, reason="User registered Minecraft account.")
                        print(f"Assigned {registered_role.name} to {member.display_name}.")
                    except discord.Forbidden:
                        print(f"Bot lacks permission to assign {registered_role.name} to {member.display_name}.")
                        await log_alert(f"Bot lacks permission to assign {registered_role.name} to {member.display_name}.")
                
                # Assign initial Iron rank role and update nickname based on default ELO
                await update_player_rank_role(member, DEFAULT_ELO)
                await update_discord_nickname(member) # Call with member directly
            else:
                print(f"Member with Discord ID {discord_id} not found in guild {MAIN_GUILD_ID} during registration.")
        else:
            print(f"Could not find guild with ID {MAIN_GUILD_ID} to update nickname/roles.")
        return True
    except Exception as e:
        print(f"Error registering user in DB: {e}")
        await log_alert(f"Error registering user {discord_id} in DB: {e}")
        return False

async def unregister_user_in_db(discord_id: str):
    """Removes a user's registration from the database."""
    try:
        success = await execute_query(
            "DELETE FROM accounts WHERE discord_id = %s", (discord_id,)
        )
        if success:
            guild = bot.get_guild(MAIN_GUILD_ID)
            if guild:
                member = guild.get_member(int(discord_id))
                if member:
                    # Remove Registered role and all rank roles
                    registered_role = guild.get_role(REGISTERED_ROLE_ID)
                    roles_to_remove = [r for r_id in ALL_RANK_ROLE_IDS + [REGISTERED_ROLE_ID] if (r := guild.get_role(r_id)) and r in member.roles]
                    try:
                        if roles_to_remove:
                            await member.remove_roles(*roles_to_remove, reason="User unregistered account.")
                            print(f"Removed roles {', '.join([r.name for r in roles_to_remove])} from {member.display_name}.")
                        await update_discord_nickname(member) # Reset nickname
                    except discord.Forbidden:
                        print(f"Bot lacks permission to remove roles/nickname for {member.display_name}.")
                        await log_alert(f"Bot lacks permission to remove roles/nickname for {member.display_name} during unregistration.")
            print(f"Unregistered user: Discord ID {discord_id}.")
        return success
    except Exception as e:
        print(f"Error unregistering user in DB: {e}")
        await log_alert(f"Error unregistering user {discord_id} in DB: {e}")
        return False

async def update_player_stats_in_db(discord_id: str, elo: float, wins: int, losses: int, games_played: int, wlr: float, kills: int, deaths: int, beds_broken: int, final_kills: int, mvp_count: int, strikes: int, game_history: list):
    """Updates a player's statistics in the database."""
    game_history_json = json.dumps(game_history)
    success = await execute_query(
        """
        UPDATE accounts SET
            elo = %s, wins = %s, losses = %s, games_played = %s, wlr = %s,
            kills = %s, deaths = %s, beds_broken = %s, final_kills = %s, mvp_count = %s,
            strikes = %s, game_history = %s
        WHERE discord_id = %s
        """,
        (elo, wins, losses, games_played, wlr, kills, deaths, beds_broken,
         final_kills, mvp_count, strikes, game_history_json, discord_id)
    )
    if success:
        guild = bot.get_guild(MAIN_GUILD_ID)
        if guild:
            member = guild.get_member(int(discord_id))
            if member:
                await update_discord_nickname(member) # Update nickname after ELO change
                await update_player_rank_role(member, elo) # Update rank role after ELO change
            else:
                print(f"Member with Discord ID {discord_id} not found in guild {MAIN_GUILD_ID} for stat update.")
        else:
            print(f"Could not find guild with ID {MAIN_GUILD_ID} to update nickname/roles after ELO change.")
    return success

async def generate_registration_code():
    """Generates a unique alphanumeric registration code."""
    while True:
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        existing_code = await execute_query(
            "SELECT discord_id FROM accounts WHERE registration_code = %s", (code,), fetchone=True
        )
        if not existing_code:
            return code

async def store_registration_code(discord_id: str, code: str):
    """Stores a registration code for a Discord user."""
    timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    await execute_query(
        """
        INSERT INTO accounts (discord_id, registration_code, registration_timestamp)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE registration_code = %s, registration_timestamp = %s
        """,
        (discord_id, code, timestamp, code, timestamp)
    )

async def check_registration_code(discord_id: str, code: str):
    """Checks if the provided registration code is valid and not expired."""
    result = await execute_query(
        "SELECT registration_code, registration_timestamp FROM accounts WHERE discord_id = %s",
        (discord_id,), fetchone=True
    )
    if result and result['registration_code'] == code:
        stored_timestamp = result['registration_timestamp']
        current_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        if (current_timestamp - stored_timestamp) < (REGISTRATION_CODE_EXPIRY_MINUTES * 60):
            return True
    return False

# --- ELO and Rank Calculation ---
def get_rank_from_elo(elo: float) -> str:
    """Determines the rank string based on ELO score."""
    for rank, data in ELO_THRESHOLDS.items():
        if data["min_elo"] <= elo <= data["max_elo"]:
            return rank
    return "Unranked"

async def update_player_rank_role(member: discord.Member, new_elo: float):
    """
    Updates a player's Discord rank role based on their ELO.
    Removes old rank roles and adds the new one.
    """
    guild = member.guild
    if not guild:
        print(f"Guild not found for member {member.display_name}. Cannot update rank role.")
        return

    current_rank_name = get_rank_from_elo(new_elo)
    
    # Find the target role ID for the new ELO
    target_role_id = None
    for rank_name, data in ELO_THRESHOLDS.items():
        if rank_name == current_rank_name:
            target_role_id = data["role_id"]
            break
    
    if not target_role_id:
        print(f"No target role ID found for rank {current_rank_name} (ELO: {new_elo}).")
        return

    target_role = guild.get_role(target_role_id)
    if not target_role:
        print(f"Target rank role (ID: {target_role_id}) not found in guild. Cannot update rank role.")
        await log_alert(f"Rank role with ID {target_role_id} not found. Please check configuration.")
        return

    roles_to_remove = []
    roles_to_add = []

    # Identify existing rank roles to remove
    for role_id in ALL_RANK_ROLE_IDS:
        if role_id == target_role_id:
            continue # Don't remove the target role if they already have it
        role = guild.get_role(role_id)
        if role and role in member.roles:
            roles_to_remove.append(role)
    
    # Add the target role if they don't have it
    if target_role not in member.roles:
        roles_to_add.append(target_role)

    try:
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason=f"ELO changed, updating rank role from {', '.join([r.name for r in roles_to_remove])} to {target_role.name}")
            print(f"Removed roles {', '.join([r.name for r in roles_to_remove])} from {member.display_name}.")
        if roles_to_add:
            await member.add_roles(*roles_to_add, reason=f"ELO changed, assigning rank role {target_role.name}")
            print(f"Assigned role {target_role.name} to {member.display_name}.")
    except discord.Forbidden:
        print(f"Bot lacks permissions to manage roles for {member.display_name}. Check role hierarchy and 'Manage Roles' permission.")
        await log_alert(f"Bot lacks permissions to manage rank roles for {member.display_name}. Please check role hierarchy and 'Manage Roles' permission.")
    except Exception as e:
        print(f"Error updating rank role for {member.display_name}: {e}")
        await log_alert(f"Error updating rank role for {member.display_name}: {e}")


# --- Discord Nickname Management (Includes ELO Prefix) ---
async def update_discord_nickname(member: discord.Member):
    """
    Updates a user's Discord nickname to include their ELO rank prefix and Minecraft username.
    Requires 'Manage Nicknames' permission for the bot.
    """
    guild = member.guild
    if not guild:
        print(f"Guild not found for member {member.display_name}. Cannot update nickname.")
        return

    account_data = await get_player_stats_from_db(str(member.id))

    minecraft_username = account_data.get('minecraft_username') if account_data else None
    elo = account_data.get('elo', DEFAULT_ELO) if account_data else DEFAULT_ELO

    new_nickname = None
    if minecraft_username:
        rank = get_rank_from_elo(elo)
        new_nickname = f"[{rank}] {minecraft_username}"
    else:
        # If not registered, revert to original Discord name or clear nickname
        new_nickname = None # Setting to None clears the nickname, reverting to Discord username

    # Discord nickname length limit is 32 characters
    if new_nickname and len(new_nickname) > 32:
        # Try to shorten intelligently, e.g., "[Rank] MCUser..."
        max_mc_username_len = 32 - len(f"[{get_rank_from_elo(elo)}] ") - 3 # -3 for "..."
        if max_mc_username_len > 0:
            shortened_mc_username = minecraft_username[:max_mc_username_len] + "..."
            new_nickname = f"[{get_rank_from_elo(elo)}] {shortened_mc_username}"
        else:
            new_nickname = new_nickname[:32] # Fallback if even rank prefix is too long

    try:
        # Check if bot's top role is below or equal to the member's top role
        if member.top_role.position >= guild.me.top_role.position:
            print(f"Cannot change nickname for {member.display_name}: Member has equal or higher role than bot.")
            return
        # Prevent changing nickname for the guild owner
        if member.id == guild.owner_id:
            print(f"Cannot change nickname for guild owner {member.display_name}.")
            return

        # Only change nickname if it's different to avoid unnecessary API calls
        if member.nick != new_nickname:
            await member.edit(nick=new_nickname)
            print(f"Updated nickname for {member.display_name} to {new_nickname}")
    except discord.Forbidden:
        print(f"Bot does not have permissions to change nickname for {member.display_name}. Check 'Manage Nicknames' permission.")
        await log_alert(f"Bot lacks 'Manage Nicknames' permission for {member.display_name}. Please check role hierarchy.")
    except Exception as e:
        print(f"Error updating nickname for {member.display_name}: {e}")
        await log_alert(f"Error updating nickname for {member.display_name}: {e}")

# --- Logging Functions ---
async def log_alert(message: str):
    """Sends an alert message to the configured alerts channel."""
    channel = bot.get_channel(ALERTS_CHANNEL_ID)
    if channel:
        await channel.send(f"🚨 **ALERT:** {message}")
    else:
        print(f"Alerts channel not found. Message: {message}")

async def log_game_event(message: str):
    """Sends a game event message to the configured game logs channel."""
    channel = bot.get_channel(GAME_LOGS_CHANNEL_ID)
    if channel:
        await channel.send(message)
    else:
        print(f"Game logs channel not found. Message: {message}")

async def log_ticket_event(message: str):
    """Sends a ticket event message to the configured ticket logs channel."""
    channel = bot.get_channel(TICKET_LOGS_CHANNEL_ID)
    if channel:
        await channel.send(message)
    else:
        print(f"Ticket logs channel not found. Message: {message}")

async def log_strike_event(message: str):
    """Sends a strike event message to the configured strike logs channel."""
    channel = bot.get_channel(STRIKE_LOGS_CHANNEL_ID)
    if channel:
        await channel.send(message)
    else:
        print(f"Strike logs channel not found. Message: {message}")

async def log_screenshare_event(message: str):
    """Sends a screenshare event message to the configured screenshare logs channel."""
    channel = bot.get_channel(SCREENSHARE_LOGS_CHANNEL_ID)
    if channel:
        await channel.send(message)
    else:
        print(f"Screenshare logs channel not found. Message: {message}")

async def log_staff_update(message: str):
    """Sends a staff update message to the configured staff update channel."""
    channel = bot.get_channel(STAFF_UPDATE_CHANNEL_ID)
    if channel:
        await channel.send(f"👥 **STAFF UPDATE:** {message}")
    else:
        print(f"Staff update channel not found. Message: {message}")

# --- Role Hierarchy Check ---
async def is_above_highest_admin_role(member: discord.Member) -> bool:
    """
    Checks if a member's highest role position is strictly above the HIGHEST_ADMIN_ROLE_ID.
    This is for bypassing channel restrictions for top-level admins.
    """
    guild = member.guild
    highest_admin_role = guild.get_role(HIGHEST_ADMIN_ROLE_ID)

    if not highest_admin_role:
        print(f"HIGHEST_ADMIN_ROLE_ID ({HIGHEST_ADMIN_ROLE_ID}) not found in guild. Cannot perform hierarchy check.")
        return False

    if member.id == guild.owner_id:
        return True

    return member.top_role.position > highest_admin_role.position

# --- Duration Parsing Helper ---
def parse_duration(duration_str: str) -> datetime.timedelta | None:
    """Parses a duration string (e.g., '30m', '1h', '7d') into a timedelta object."""
    duration_str = duration_str.lower()
    if duration_str.endswith('m'):
        try:
            minutes = int(duration_str[:-1])
            return datetime.timedelta(minutes=minutes)
        except ValueError:
            return None
    elif duration_str.endswith('h'):
        try:
            hours = int(duration_str[:-1])
            return datetime.timedelta(hours=hours)
        except ValueError:
            return None
    elif duration_str.endswith('d'):
        try:
            days = int(duration_str[:-1])
            return datetime.timedelta(days=days)
        except ValueError:
            return None
    else:
        return None

# --- MySQL Temporary Moderation Functions ---
async def store_temp_moderation_mysql(discord_id: str, mod_type: str, end_timestamp: int, reason: str):
    """Stores a temporary mute or ban record in MySQL."""
    column_name = f"{mod_type}_until"
    success = await execute_query(
        f"UPDATE accounts SET {column_name} = %s WHERE discord_id = %s",
        (end_timestamp, discord_id)
    )
    if success:
        print(f"Stored temporary {mod_type} for {discord_id} until {datetime.datetime.fromtimestamp(end_timestamp, datetime.timezone.utc)}")
    return success

async def remove_temp_moderation_mysql(discord_id: str, mod_type: str):
    """Removes a temporary mute or ban record from MySQL."""
    column_name = f"{mod_type}_until"
    success = await execute_query(
        f"UPDATE accounts SET {column_name} = NULL WHERE discord_id = %s",
        (discord_id,)
    )
    if success:
        print(f"Removed temporary {mod_type} for {discord_id}")
    return success

# --- Background Task to Check Temporary Moderations ---
@tasks.loop(minutes=1) # Check every minute
async def check_temp_moderations_task():
    current_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    guild = bot.get_guild(MAIN_GUILD_ID)
    if not guild:
        print(f"Guild with ID {MAIN_GUILD_ID} not found for temp moderation check.")
        return

    # Check for expired mutes
    expired_mutes = await execute_query(
        "SELECT discord_id FROM accounts WHERE muted_until IS NOT NULL AND muted_until <= %s",
        (current_timestamp,), fetchall=True
    )
    if expired_mutes:
        for record in expired_mutes:
            discord_id = record['discord_id']
            member = guild.get_member(int(discord_id))
            muted_role = guild.get_role(MUTED_ROLE_ID)

            if member and muted_role and muted_role in member.roles:
                try:
                    await member.remove_roles(muted_role, reason="Temporary mute expired")
                    # Log to strike logs channel for moderation actions
                    await log_strike_event(f"🔊 {member.display_name} has been automatically unmuted (duration expired).")
                    print(f"Automatically unmuted {member.display_name}")
                except discord.Forbidden:
                    await log_alert(f"Bot lacks permission to unmute {member.display_name}. Manual intervention needed.")
                except Exception as e:
                    await log_alert(f"Error during auto-unmute for {member.display_name}: {e}")
            await remove_temp_moderation_mysql(discord_id, 'mute')

    # Check for expired bans (now removes BANNED_ROLE_ID instead of server unban)
    expired_bans = await execute_query(
        "SELECT discord_id FROM accounts WHERE banned_until IS NOT NULL AND banned_until <= %s",
        (current_timestamp,), fetchall=True
    )
    if expired_bans:
        for record in expired_bans:
            discord_id = record['discord_id']
            member = guild.get_member(int(discord_id)) # Fetch member, not just user
            banned_role = guild.get_role(BANNED_ROLE_ID)

            if member and banned_role and banned_role in member.roles:
                try:
                    await member.remove_roles(banned_role, reason="Temporary ban role expired")
                    # Log to strike logs channel for moderation actions
                    await log_strike_event(f"🔓 {member.display_name} has been automatically unbanned (role removed, duration expired).")
                    print(f"Automatically unbanned {member.display_name} (role removed)")
                except discord.Forbidden:
                    await log_alert(f"Bot lacks permission to remove ban role from {member.display_name}. Manual intervention needed.")
                except Exception as e:
                    await log_alert(f"Error during auto-unban role removal for {member.display_name}: {e}")
            await remove_temp_moderation_mysql(discord_id, 'ban')

# --- Voice Channel Queue Monitoring Task ---
@tasks.loop(seconds=10) # Check every 10 seconds
async def check_voice_channels_for_queue():
    guild = bot.get_guild(MAIN_GUILD_ID)
    if not guild:
        print(f"Guild with ID {MAIN_GUILD_ID} not found for voice channel queue check.")
        return

    # Check 3v3 queue
    vc_3v3 = guild.get_channel(QUEUE_3V3_VOICE_CHANNEL_ID)
    if vc_3v3 and isinstance(vc_3v3, discord.VoiceChannel):
        current_players = [member for member in vc_3v3.members if not member.bot]
        if len(current_players) >= MIN_PLAYERS_3V3:
            print(f"Enough players ({len(current_players)}) in 3v3 queue. Starting game...")
            # Shuffle players to randomize captain selection if needed, though first two are captains
            random.shuffle(current_players) 
            await start_game_logic(guild, current_players, "3v3")
            # Clear the queue (players will be moved by start_game_logic)
            queues["3v3"].clear()

    # Check 4v4 queue
    vc_4v4 = guild.get_channel(QUEUE_4V4_VOICE_CHANNEL_ID)
    if vc_4v4 and isinstance(vc_4v4, discord.VoiceChannel):
        current_players = [member for member in vc_4v4.members if not member.bot]
        if len(current_players) >= MIN_PLAYERS_4V4:
            print(f"Enough players ({len(current_players)}) in 4v4 queue. Starting game...")
            random.shuffle(current_players)
            await start_game_logic(guild, current_players, "4v4")
            # Clear the queue (players will be moved by start_game_logic)
            queues["4v4"].clear()

async def start_game_logic(guild: discord.Guild, players: list[discord.Member], game_type: str):
    """
    Initiates the game creation process when enough players are in a queue.
    This replaces the manual =startgame command.
    """
    # Select a random map
    map_name = random.choice(AVAILABLE_MAPS)
    game_number = get_next_game_number()

    text_channel, voice_lobby_channel, _, _ = await create_game_channels(guild, players, game_type, map_name, game_number)

    if text_channel and voice_lobby_channel:
        # Send a message to a general announcement channel or the queue channel
        # that a game has started and where to go.
        announcement_channel = bot.get_channel(GAME_LOGS_CHANNEL_ID) # Or a dedicated announcement channel
        if announcement_channel:
            response_embed = discord.Embed(
                title="🎉 Game Started Automatically!",
                description=f"A new **{game_type}** game has started on **{map_name}**!\n"
                            f"Game Channel: {text_channel.mention}\n"
                            f"Voice Lobby: {voice_lobby_channel.mention}\n\n"
                            f"Captains, please use the dropdown in your game channel to pick players!",
                color=discord.Color.green()
            )
            response_embed.set_footer(text=f"Game #{game_number:04d} | Powered by ASRBW.net")
            await announcement_channel.send(embed=response_embed)
        
        await log_game_event(f"Automated Game #{game_number:04d} started with players: {', '.join([p.display_name for p in players])} on map {map_name} ({game_type}).")
    else:
        await log_alert(f"Automated game start failed for {game_type} with players {', '.join([p.display_name for p in players])}. Check bot permissions and category IDs.")

# --- Private Channel Creation Functions ---

async def create_screenshare_channel(guild: discord.Guild, requester: discord.Member, target_player: discord.Member, reason: str, attachments: list[discord.Attachment]):
    """
    Creates a private text channel for a screenshare request.
    Only visible to requester, target, and the specific screensharer role.
    """
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        requester: discord.PermissionOverwrite(view_channel=True, send_messages=True),
        target_player: discord.PermissionOverwrite(view_channel=True, send_messages=True)
    }

    screensharer_role = guild.get_role(SCREENSHARER_ROLE_ID)
    if screensharer_role:
        overwrites[screensharer_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
    else:
        await log_alert(f"Screensharer role (ID: {SCREENSHARER_ROLE_ID}) not found. Screenshare channel visibility might be incorrect.")

    channel_name = f"ss-{target_player.name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:4]}"
    try:
        screenshare_category = guild.get_channel(SCREENSHARE_CATEGORY_ID)
        if not screenshare_category:
            await log_alert(f"Screenshare category (ID: {SCREENSHARE_CATEGORY_ID}) not found for screenshare channel creation.")
            return None

        channel = await guild.create_text_channel(
            channel_name,
            category=screenshare_category,
            overwrites=overwrites,
            topic=f"Screenshare for {target_player.display_name} requested by {requester.display_name}. Reason: {reason}"
        )
        print(f"Created screenshare channel: {channel.name}")

        embed = discord.Embed(
            title="🔍 Screenshare Request",
            description=f"A screenshare has been requested for {target_player.mention}.",
            color=discord.Color.orange()
        )
        embed.add_field(name="Requested By", value=requester.mention, inline=True)
        embed.add_field(name="Target Player", value=target_player.mention, inline=True)
        embed.add_field(name="Reason", value=reason, inline=False)
        
        attachment_urls = [att.url for att in attachments]
        if attachment_urls:
            embed.add_field(name="Proof (Attachments)", value="\n".join(attachment_urls), inline=False)
            if attachments[0].content_type.startswith(('image/')):
                embed.set_image(url=attachments[0].url)
        else:
            embed.add_field(name="Proof", value="No image proof provided.", inline=False)

        embed.set_footer(text=f"Screenshare ID: {uuid.uuid4().hex[:8]}")

        mentions = [target_player.mention, requester.mention]
        if screensharer_role:
            mentions.append(screensharer_role.mention)
        await channel.send(f"{' '.join(mentions)}", embed=embed)
        return channel
    except discord.Forbidden:
        await log_alert(f"Bot lacks permissions to create screenshare channel in category {screenshare_category.name}. Ensure 'Manage Channels' permission is granted.")
        return None
    except Exception as e:
        await log_alert(f"Error creating screenshare channel: {e}")
        print(f"Error creating screenshare channel: {e}")
        return None

async def create_strike_channel(guild: discord.Guild, requester: discord.Member, target_player: discord.Member, reason: str, attachments: list[discord.Attachment]):
    """
    Creates a private text channel for a strike request.
    Only visible to requester, target, and moderation roles.
    """
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        requester: discord.PermissionOverwrite(view_channel=True, send_messages=True),
        target_player: discord.PermissionOverwrite(view_channel=True, send_messages=True)
    }

    for role_id in MODERATION_ROLES_IDS:
        role = guild.get_role(role_id)
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)

    channel_name = f"strike-{target_player.name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:4]}"
    try:
        ticket_category = guild.get_channel(TICKET_CATEGORY_ID)
        if not ticket_category:
            await log_alert(f"Ticket category (ID: {TICKET_CATEGORY_ID}) not found for strike channel creation.")
            return None

        channel = await guild.create_text_channel(
            channel_name,
            category=ticket_category,
            overwrites=overwrites,
            topic=f"Strike request for {target_player.display_name} requested by {requester.display_name}. Reason: {reason}"
        )
        print(f"Created strike channel: {channel.name}")

        embed = discord.Embed(
            title="🚨 Strike Request",
            description=f"A strike has been requested for {target_player.mention}.",
            color=discord.Color.red()
        )
        embed.add_field(name="Requested By", value=requester.mention, inline=True)
        embed.add_field(name="Target Player", value=target_player.mention, inline=True)
        embed.add_field(name="Reason", value=reason, inline=False)

        attachment_urls = [att.url for att in attachments]
        if attachment_urls:
            embed.add_field(name="Proof (Attachments)", value="\n".join(attachment_urls), inline=False)
            if attachments[0].content_type.startswith(('image/')):
                embed.set_image(url=attachments[0].url)
        else:
            embed.add_field(name="Proof", value="No image proof provided.", inline=False)

        embed.set_footer(text=f"Strike ID: {uuid.uuid4().hex[:8]}")

        mentions = [target_player.mention, requester.mention]
        for role_id in MODERATION_ROLES_IDS:
            role = guild.get_role(role_id)
            if role:
                mentions.append(role.mention)
        await channel.send(f"{' '.join(mentions)}", embed=embed)
        return channel
    except discord.Forbidden:
        await log_alert(f"Bot lacks permissions to create strike channel in category {ticket_category.name}. Ensure 'Manage Channels' permission is granted.")
        return None
    except Exception as e:
        await log_alert(f"Error creating strike channel: {e}")
        print(f"Error creating strike channel: {e}")
        return None

async def create_game_channels(guild: discord.Guild, players: list[discord.Member], game_type: str, map_name: str, game_number: int):
    """
    Creates a private text channel and an initial 'Lobby' voice channel for a game.
    The voice channel will be in a separate category.
    """
    if len(players) < 2:
        await log_alert(f"Not enough players ({len(players)}) to start a game for Game #{game_number:04d}. Need at least 2 for captains.")
        return None, None, None, None

    # Assign Captains
    captain_a_id = str(players[0].id)
    captain_b_id = str(players[1].id)
    
    overwrites_text = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False)
    }
    overwrites_voice_lobby = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False)
    }

    for player in players:
        overwrites_text[player] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        overwrites_voice_lobby[player] = discord.PermissionOverwrite(view_channel=True, connect=True)

    for role_id in MODERATION_ROLES_IDS: # Staff (moderation) roles can see game channels
        role = guild.get_role(role_id)
        if role:
            overwrites_text[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            overwrites_voice_lobby[role] = discord.PermissionOverwrite(view_channel=True, connect=True)

    text_channel = None
    voice_lobby_channel = None

    try:
        game_category = guild.get_channel(GAME_CATEGORY_ID)
        if not game_category:
            await log_alert(f"Game text category (ID: {GAME_CATEGORY_ID}) not found for game channel creation.")
            return None, None, None, None

        game_voice_category = guild.get_channel(GAME_VOICE_CATEGORY_ID)
        if not game_voice_category:
            await log_alert(f"Game voice category (ID: {GAME_VOICE_CATEGORY_ID}) not found for game voice channel creation.")
            return None, None, None, None

        channel_name_prefix = "game" if game_type != "3v3" and game_type != "4v4" else game_type # Use game type for naming
        text_channel = await guild.create_text_channel(
            f"{channel_name_prefix}-{game_number:04d}", # e.g., game-0001 or 3v3-0001
            category=game_category,
            overwrites=overwrites_text,
            topic=f"Text channel for Game #{game_number:04d} ({game_type} on {map_name}). Players: {', '.join([p.display_name for p in players])}"
        )
        print(f"Created game text channel: {text_channel.name}")

        voice_lobby_channel = await guild.create_voice_channel(
            f"Game {game_number:04d} Lobby",
            category=game_voice_category,
            overwrites=overwrites_voice_lobby
        )
        print(f"Created game voice lobby channel: {voice_lobby_channel.name}")

        # Initialize players in active_games, assigning captains to teams
        game_id = str(uuid.uuid4())
        active_games[game_id] = {
            "text_channel_id": text_channel.id,
            "voice_channel_lobby_id": voice_lobby_channel.id,
            "team_a_vc_id": None,
            "team_b_vc_id": None,
            "players": {str(p.id): None for p in players}, # {discord_id: team_name (A/B)}
            "game_type": game_type,
            "map_name": map_name,
            "game_number": game_number,
            "team_message_id": None, # To store the message ID of the team embed
            "captains": {"A": captain_a_id, "B": captain_b_id} # Store captain IDs
        }
        active_games[game_id]["players"][captain_a_id] = "A"
        active_games[game_id]["players"][captain_b_id] = "B"


        # Initial message for team selection
        team_embed = discord.Embed(
            title=f"🎮 Game #{game_number:04d} - Team Selection (Draft)",
            description=f"Welcome to your game on **{map_name}**! Please join the voice lobby: {voice_lobby_channel.mention}\n\n"
                        f"**Captains:**\n"
                        f"Team A: {players[0].mention}\n"
                        f"Team B: {players[1].mention}\n\n"
                        f"It's {players[0].mention}'s turn to pick!", # Initial pick for Team A
            color=discord.Color.blue()
        )
        team_embed.set_footer(text="Captains use the dropdown to pick players. Powered by asrbw.net")
        
        view = TeamSelectionView(game_id, players)
        team_message = await text_channel.send(embed=team_embed, view=view)
        view.message = team_message # Store message for view to edit
        active_games[game_id]["team_message_id"] = team_message.id

        # Move players from queue VCs to the new game lobby VC
        queue_vc_id_source = None
        if game_type == '3v3':
            queue_vc_id_source = QUEUE_3V3_VOICE_CHANNEL_ID
        elif game_type == '4v4':
            queue_vc_id_source = QUEUE_4V4_VOICE_CHANNEL_ID
        
        if queue_vc_id_source:
            source_vc = guild.get_channel(queue_vc_id_source)
            if source_vc:
                for player in players:
                    if player.voice and player.voice.channel and player.voice.channel.id == source_vc.id:
                        try:
                            await player.move_to(voice_lobby_channel)
                            print(f"Moved {player.display_name} from queue to game lobby VC.")
                        except discord.Forbidden:
                            print(f"Bot lacks permission to move {player.display_name} to {voice_lobby_channel.name}.")
                            await log_alert(f"Bot lacks permission to move {player.display_name} to game lobby VC. Check 'Move Members' permission.")
                        except Exception as e:
                            print(f"Error moving {player.display_name}: {e}")
                            await log_alert(f"Error moving {player.display_name} to game lobby VC: {e}")
            else:
                print(f"Source queue voice channel (ID: {queue_vc_id_source}) not found.")
        else:
            print(f"No source queue VC configured for game type {game_type}.")

        return text_channel, voice_lobby_channel, None, None # Return None for team VCs initially
    except discord.Forbidden:
        await log_alert(f"Bot lacks permissions to create game channels in categories. Ensure 'Manage Channels' and 'Move Members' permissions are granted.")
        if text_channel: await text_channel.delete()
        if voice_lobby_channel: await voice_lobby_channel.delete()
        return None, None, None, None
    except Exception as e:
        await log_alert(f"Error creating game channels: {e}")
        print(f"Error creating game channels: {e}")
        if text_channel: await text_channel.delete()
        if voice_lobby_channel: await voice_lobby_channel.delete()
        return None, None, None, None

async def move_players_to_team_vcs(guild: discord.Guild, game_id: str):
    """
    Creates team-specific VCs and moves players into them based on their selected teams.
    """
    game_data = active_games.get(game_id)
    if not game_data:
        print(f"Game data not found for game_id: {game_id}")
        return

    text_channel = guild.get_channel(game_data["text_channel_id"])
    voice_lobby_channel = guild.get_channel(game_data["voice_channel_lobby_id"])
    game_voice_category = guild.get_channel(GAME_VOICE_CATEGORY_ID)

    if not text_channel or not voice_lobby_channel or not game_voice_category:
        print("Required channels or category not found for moving players to team VCs.")
        return

    overwrites_team_vc = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False)
    }
    for role_id in MODERATION_ROLES_IDS:
        role = guild.get_role(role_id)
        if role:
            overwrites_team_vc[role] = discord.PermissionOverwrite(view_channel=True, connect=True)

    team_a_vc = None
    team_b_vc = None
    
    try:
        team_a_vc = await guild.create_voice_channel(
            f"Game {game_data['game_number']:04d} Team A",
            category=game_voice_category,
            overwrites=overwrites_team_vc
        )
        team_b_vc = await guild.create_voice_channel(
            f"Game {game_data['game_number']:04d} Team B",
            category=game_voice_category,
            overwrites=overwrites_team_vc
        )
        game_data["team_a_vc_id"] = team_a_vc.id
        game_data["team_b_vc_id"] = team_b_vc.id

        await text_channel.send(f"Teams are set! Please move to your respective voice channels:\n"
                                f"Team A: {team_a_vc.mention}\n"
                                f"Team B: {team_b_vc.mention}")
        
        # Move players
        for discord_id, team in game_data["players"].items():
            member = guild.get_member(int(discord_id))
            if member and member.voice and member.voice.channel == voice_lobby_channel:
                target_vc = None
                if team == "A":
                    target_vc = team_a_vc
                elif team == "B":
                    target_vc = team_b_vc
                
                if target_vc:
                    try:
                        await member.move_to(target_vc)
                        print(f"Moved {member.display_name} to {target_vc.name}.")
                    except discord.Forbidden:
                        print(f"Bot lacks permission to move {member.display_name} to {target_vc.name}.")
                        await log_alert(f"Bot lacks permission to move {member.display_name} to team VC. Check 'Move Members' permission.")
                    except Exception as e:
                        print(f"Error moving {member.display_name}: {e}")
                        await log_alert(f"Error moving {member.display_name} to team VC: {e}")
    except discord.Forbidden:
        await log_alert(f"Bot lacks permissions to create team VCs. Ensure 'Manage Channels' permission is granted.")
    except Exception as e:
        await log_alert(f"Error creating/moving players to team VCs: {e}")
        print(f"Error creating/moving players to team VCs: {e}")
    finally:
        # Delete lobby VC after players are moved
        if voice_lobby_channel:
            try:
                await voice_lobby_channel.delete(reason="Lobby VC no longer needed after team VCs created.")
            except Exception as e:
                print(f"Error deleting lobby VC: {e}")


# --- Team Selection View ---
class TeamSelectionView(View):
    def __init__(self, game_id: str, all_players_members: list[discord.Member]):
        super().__init__(timeout=3600) # Timeout after 1 hour if no interaction
        self.game_id = game_id
        self.all_players_members = {str(p.id): p for p in all_players_members} # Store all members for display
        self.message = None # Will be set by create_game_channels

        game_data = active_games.get(self.game_id)
        if not game_data:
            raise ValueError(f"Game data for {game_id} not found during TeamSelectionView initialization.")

        self.captains = game_data["captains"] # {"A": captain_a_id, "B": captain_b_id}
        
        # Initialize available players (all players minus the already assigned captains)
        self.available_players_ids = [
            p_id for p_id in game_data["players"] 
            if p_id not in self.captains.values()
        ]
        
        # Define picking sequence based on game type
        if game_data["game_type"] == "3v3": # 6 players total: C1, C2, P1, P2, P3, P4
            # A picks 1, B picks 2. Last player (P4) automatically goes to A.
            self.picking_sequence = ["A", "B", "B"] 
        elif game_data["game_type"] == "4v4": # 8 players total: C1, C2, P1, P2, P3, P4, P5, P6
            # A picks 1, B picks 2, A picks 2, B picks 1 (snake draft)
            self.picking_sequence = ["A", "B", "B", "A", "A", "B"]
        else:
            self.picking_sequence = [] # Should not happen with current validation

        self.current_pick_index = 0
        self.current_picking_captain_team = None
        self.current_picking_captain_id = None
        
        self.update_picking_turn() # Set initial picking turn

        self.add_item(self.create_player_select())
        self.add_item(Button(label="Start Game (Staff Only)", style=ButtonStyle.blurple, custom_id="start_game", disabled=True))

    def update_picking_turn(self):
        """Determines whose turn it is to pick."""
        game_data = active_games.get(self.game_id)
        if not game_data: return

        if self.current_pick_index < len(self.picking_sequence):
            self.current_picking_captain_team = self.picking_sequence[self.current_pick_index]
            self.current_picking_captain_id = self.captains[self.current_picking_captain_team]
        else:
            self.current_picking_captain_team = None
            self.current_picking_captain_id = None
        self.update_select_options() # Update select options whenever turn changes

    def update_select_options(self):
        """Updates the options for the player selection dropdown."""
        options = []
        for player_id in self.available_players_ids:
            member = self.all_players_members.get(player_id)
            if member:
                options.append(SelectOption(label=member.display_name, value=player_id))
        
        # Ensure there's always at least one option to avoid Discord API error for empty select
        if not options:
            options.append(SelectOption(label="No players left to pick", value="no_players", default=True))

        # Find the existing select component and update its options
        for child in self.children:
            if isinstance(child, Select) and child.custom_id == "player_pick_select":
                child.options = options
                child.disabled = (self.current_picking_captain_id is None or not self.available_players_ids)
                break
        else: # If select not found, create it
            self.add_item(self.create_player_select())


    def create_player_select(self):
        """Creates the Select component for player picking (used for initial creation)."""
        options = []
        for player_id in self.available_players_ids:
            member = self.all_players_members.get(player_id)
            if member:
                options.append(SelectOption(label=member.display_name, value=player_id))
        
        if not options:
            options.append(SelectOption(label="No players left to pick", value="no_players", default=True))

        select = Select(
            custom_id="player_pick_select",
            placeholder="Pick a player...",
            options=options,
            disabled=(self.current_picking_captain_id is None or not self.available_players_ids)
        )
        return select

    async def update_view(self, interaction: discord.Interaction):
        """Re-renders the view with updated components."""
        # Update Start Game button state
        start_game_button = next((item for item in self.children if item.custom_id == "start_game"), None)
        if start_game_button:
            game_data = active_games.get(self.game_id)
            all_assigned = all(team is not None for team in game_data["players"].values())
            start_game_button.disabled = not all_assigned # Enable only when all players are assigned
            if all_assigned:
                # Disable the pick select once all players are assigned
                for child in self.children:
                    if isinstance(child, Select) and child.custom_id == "player_pick_select":
                        child.disabled = True
        
        await interaction.message.edit(view=self)


    async def update_team_embed(self, interaction: discord.Interaction):
        game_data = active_games.get(self.game_id)
        if not game_data:
            return # Game no longer active or data missing

        team_a_members = []
        team_b_members = []
        available_players_display = []

        # Sort players for consistent display in embed
        sorted_player_ids = sorted(game_data["players"].keys(), key=lambda x: self.all_players_members.get(x).display_name if self.all_players_members.get(x) else x)

        for player_id in sorted_player_ids:
            team = game_data["players"].get(player_id)
            member = self.all_players_members.get(player_id)
            member_display_name = member.display_name if member else f"Unknown User ({player_id})"
            
            if player_id == self.captains["A"]:
                team_a_members.append(f"👑 {member_display_name} (Captain)")
            elif player_id == self.captains["B"]:
                team_b_members.append(f"👑 {member_display_name} (Captain)")
            elif team == "A":
                team_a_members.append(member_display_name)
            elif team == "B":
                team_b_members.append(member_display_name)
            elif player_id in self.available_players_ids:
                available_players_display.append(member_display_name)

        team_a_str = "\n".join(team_a_members) if team_a_members else "(Empty)"
        team_b_str = "\n".join(team_b_members) if team_b_members else "(Empty)"
        available_str = "\n".join(available_players_display) if available_players_display else "(None)"

        embed = discord.Embed(
            title=f"🎮 Game #{game_data['game_number']:04d} - Team Selection (Draft)",
            description=f"Welcome to your game on **{game_data['map_name']}**! Join the voice lobby: {interaction.guild.get_channel(game_data['voice_channel_lobby_id']).mention}\n\n"
                        f"**Team A:**\n{team_a_str}\n\n**Team B:**\n{team_b_str}\n\n"
                        f"**Available Players:**\n{available_str}\n\n",
            color=discord.Color.blue()
        )

        if self.current_picking_captain_id:
            captain_member = self.all_players_members.get(self.current_picking_captain_id)
            if captain_member:
                embed.add_field(name="Current Pick", value=f"It's {captain_member.mention} (Team {self.current_picking_captain_team})'s turn to pick!", inline=False)
            else:
                embed.add_field(name="Current Pick", value=f"It's Team {self.current_picking_captain_team}'s turn to pick!", inline=False)
        else:
            embed.add_field(name="Draft Complete!", value="All players have been assigned to teams.", inline=False)
            
        embed.set_footer(text="Captains use the dropdown to pick players. Powered by asrbw.net")

        # Edit the original message
        try:
            await self.message.edit(embed=embed)
        except discord.NotFound:
            print("Team selection message not found, cannot update.")
        except Exception as e:
            print(f"Error updating team selection embed: {e}")

    @discord.ui.select(custom_id="player_pick_select")
    async def player_pick_select(self, interaction: discord.Interaction, select: Select):
        game_data = active_games.get(self.game_id)
        if not game_data:
            await interaction.response.send_message("This game is no longer active.", ephemeral=True)
            return

        # Check if it's the correct captain's turn
        if str(interaction.user.id) != self.current_picking_captain_id:
            await interaction.response.send_message(f"It's not your turn to pick! It's {self.all_players_members.get(self.current_picking_captain_id).display_name}'s turn.", ephemeral=True)
            return
        
        if not select.values or select.values[0] == "no_players":
            await interaction.response.send_message("No players left to pick.", ephemeral=True)
            return

        picked_player_id = select.values[0]
        picked_player_member = self.all_players_members.get(picked_player_id)

        if picked_player_id not in self.available_players_ids:
            await interaction.response.send_message(f"{picked_player_member.display_name} is no longer available or already picked.", ephemeral=True)
            return

        # Assign picked player to the current captain's team
        game_data["players"][picked_player_id] = self.current_picking_captain_team
        self.available_players_ids.remove(picked_player_id)

        await interaction.response.defer() # Defer the interaction response

        # Check for party member auto-assignment
        party_owner_id = player_party_map.get(picked_player_id)
        if party_owner_id:
            party_info = parties.get(party_owner_id)
            if party_info and len(party_info["members"]) == 2: # Only for 2-person parties
                other_party_member_id = next((m_id for m_id in party_info["members"] if m_id != picked_player_id), None)
                if other_party_member_id and other_party_member_id in self.available_players_ids:
                    game_data["players"][other_party_member_id] = self.current_picking_captain_team
                    self.available_players_ids.remove(other_party_member_id)
                    other_member_display_name = self.all_players_members.get(other_party_member_id).display_name if self.all_players_members.get(other_party_member_id) else f"Unknown User ({other_party_member_id})"
                    await interaction.followup.send(f"👥 {picked_player_member.display_name}'s party member, {other_member_display_name}, has also been assigned to Team {self.current_picking_captain_team}!", ephemeral=False)

        # Auto-assign last player for 3v3 if applicable
        if game_data["game_type"] == "3v3" and len(self.available_players_ids) == 1:
            last_player_id = self.available_players_ids[0]
            game_data["players"][last_player_id] = "A" # Auto-assign to Team A
            self.available_players_ids.remove(last_player_id)
            await interaction.followup.send(f"The last player, {self.all_players_members.get(last_player_id).display_name}, has been automatically assigned to Team A!", ephemeral=False)


        # Move to next pick
        self.current_pick_index += 1
        self.update_picking_turn() # Update whose turn it is and refresh select options

        await self.update_team_embed(interaction)
        await self.update_view(interaction) # Update the view to reflect changes (e.g., disabled select)

        if not self.available_players_ids:
            await interaction.followup.send("Draft complete! All players have been assigned to teams. Staff can now click 'Start Game'.", ephemeral=False)


    @discord.ui.button(label="Start Game (Staff Only)", style=ButtonStyle.blurple, custom_id="start_game")
    async def start_game_button(self, interaction: discord.Interaction, button: Button):
        # Check if user has a moderation role
        is_staff = any(role.id in MODERATION_ROLES_IDS for role in interaction.user.roles)
        if not is_staff:
            await interaction.response.send_message("You do not have permission to start the game.", ephemeral=True)
            return

        game_data = active_games.get(self.game_id)
        if not game_data:
            await interaction.response.send_message("This game is no longer active.", ephemeral=True)
            return

        # Check if all players are assigned to a team
        all_assigned = all(team is not None for team in game_data["players"].values())
        if not all_assigned:
            unassigned_players = [self.all_players_members.get(p_id).mention for p_id, team in game_data["players"].items() if team is None and self.all_players_members.get(p_id)]
            await interaction.response.send_message(f"❌ Not all players have picked a team! Unassigned: {', '.join(unassigned_players)}", ephemeral=True)
            return
        
        await interaction.response.send_message("✅ Game starting! Moving players to team voice channels...", ephemeral=False)
        await move_players_to_team_vcs(interaction.guild, self.game_id)
        
        # Disable all buttons and select after game starts
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self) # Update the message to disable buttons
        self.stop() # Stop the view entirely

# --- Bot Events ---
@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    await ensure_db_connection() # Ensure DB table exists on bot startup
    
    # Start background tasks
    check_temp_moderations_task.start()
    check_voice_channels_for_queue.start() # Start monitoring voice channels

    # Set bot activity
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name="asrbw.net"))
    print("Bot is ready!")

@bot.event
async def on_member_update(before, after):
    """Logs when a staff role is added or removed from a member."""
    guild = after.guild
    staff_update_channel = bot.get_channel(STAFF_UPDATE_CHANNEL_ID)
    if not staff_update_channel:
        print(f"Staff update channel (ID: {STAFF_UPDATE_CHANNEL_ID}) not found.")
        return

    # Get the roles that are considered staff roles for logging purposes
    staff_roles_for_logging = {guild.get_role(r_id) for r_id in MODERATION_ROLES_IDS if guild.get_role(r_id)}

    before_roles = set(before.roles)
    after_roles = set(after.roles)

    added_roles = after_roles - before_roles
    removed_roles = before_roles - after_roles

    for role in added_roles:
        if role in staff_roles_for_logging:
            await log_staff_update(f"{after.mention} was granted the **{role.name}** role.")
            print(f"Logged staff role added: {after.display_name} got {role.name}")
    
    for role in removed_roles:
        if role in staff_roles_for_logging:
            await log_staff_update(f"{after.mention} had the **{role.name}** role removed.")
            print(f"Logged staff role removed: {after.display_name} lost {role.name}")

# --- Image Generation Functions ---
async def generate_player_stats_image(player_stats: dict, target_user: discord.Member):
    """Generates a player stats image similar to image_d029d5.png."""
    mc_username = player_stats.get('minecraft_username', 'N/A')
    elo = player_stats.get('elo', DEFAULT_ELO)
    wins = player_stats.get('wins', 0)
    losses = player_stats.get('losses', 0)
    games_played = player_stats.get('games_played', 0)
    wlr = wins / losses if losses > 0 else wins # Calculate WLR dynamically
    mvp_count = player_stats.get('mvp_count', 0)
    rank = get_rank_from_elo(elo)

    card_width, card_height = 600, 300
    background_color = (44, 47, 51, 255) # Discord dark theme color
    text_color_primary = (255, 255, 255, 255)
    text_color_secondary = (180, 180, 255, 255)
    text_color_stats = (200, 200, 200, 255)
    asrbw_color = (255, 200, 0, 255) # Gold color

    img = Image.new('RGBA', (card_width, card_height), background_color)
    draw = ImageDraw.Draw(img)

    # Rounded corners
    radius = 20
    mask = Image.new('L', (card_width, card_height), 0)
    draw_mask = ImageDraw.Draw(mask)
    draw_mask.rounded_rectangle([(0,0), (card_width, card_height)], radius, fill=255)
    img = Image.composite(img, Image.new('RGBA', (card_width, card_height), (0,0,0,0)), mask)

    # Load fonts
    try:
        font_asrbw = ImageFont.truetype(FONT_PATH, 40)
        font_large = ImageFont.truetype(FONT_PATH, 30)
        font_medium = ImageFont.truetype(FONT_PATH, 20)
        font_small = ImageFont.truetype(FONT_PATH, 16)
    except IOError:
        print(f"Warning: Font file '{FONT_PATH}' not found. Using default Pillow font.")
        font_asrbw = ImageFont.load_default()
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()

    # Add "ASRBW" text
    asrbw_text = "ASRBW"
    asrbw_text_width, asrbw_text_height = draw.textsize(asrbw_text, font=font_asrbw)
    asrbw_text_x = (card_width - asrbw_text_width) / 2
    draw.text((asrbw_text_x, 10), asrbw_text, font=font_asrbw, fill=asrbw_color)

    # Fetch Minecraft UUID for skin
    mc_uuid, _ = await get_minecraft_uuid_from_discord_id(str(target_user.id))
    if not mc_uuid:
        mc_uuid = "069a79f4-44e9-4726-a5be-fca90e3ddaf5" # Default UUID for Steve if not found

    # Download skin head and body render
    head_img = None
    body_img = None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"https://crafatar.com/avatars/{mc_uuid}?size=128&overlay") as resp:
                if resp.status == 200:
                    head_data = await resp.read()
                    head_img = Image.open(BytesIO(head_data)).convert("RGBA")
            async with session.get(f"https://crafatar.com/renders/body/{mc_uuid}?scale=2&overlay") as resp:
                if resp.status == 200:
                    body_data = await resp.read()
                    body_img = Image.open(BytesIO(body_data)).convert("RGBA")
        except Exception as e:
            print(f"Error fetching Crafatar skin for {mc_username}: {e}")

    # Fallback for skin images
    if not head_img:
        head_img = Image.new('RGBA', (128, 128), (150, 150, 150, 255))
        draw_head = ImageDraw.Draw(head_img)
        draw_head.text((10, 50), "NO SKIN", fill=(0,0,0,255), font=ImageFont.load_default())
    if not body_img:
        body_img = Image.new('RGBA', (128, 256), (0,0,0,0))
        draw_body = ImageDraw.Draw(body_img)
        draw_body.rectangle([(30,0), (90,120)], fill=(100,100,100,255)) # Body
        draw_body.rectangle([(0,0), (30,120)], fill=(80,80,80,255)) # Left arm
        draw_body.rectangle([(90,0), (120,120)], fill=(80,80,80,255)) # Right arm
        draw_body.rectangle([(30,120), (60,256)], fill=(60,60,60,255)) # Left leg
        draw_body.rectangle([(60,120), (90,256)], fill=(60,60,60,255)) # Right leg

    # Paste skin head and body render
    head_pos = (card_width - head_img.width - 20, 20)
    body_pos = (card_width - body_img.width - 20, head_img.height + 30)
    img.paste(head_img, head_pos, head_img)
    img.paste(body_img, body_pos, body_img)

    # Add text stats
    start_y_offset = 60
    draw.text((20, start_y_offset), mc_username, font=font_large, fill=text_color_primary)
    draw.text((20, start_y_offset + 40), f"ELO: {int(elo)} ({rank})", font=font_medium, fill=text_color_secondary)

    stats_text = [
        f"Wins: {wins}",
        f"Losses: {losses}",
        f"WLR: {wlr:.2f}",
        f"MVPs: {mvp_count}",
        f"Games Played: {games_played}"
    ]
    y_offset = start_y_offset + 80
    for line in stats_text:
        draw.text((20, y_offset), line, font=font_medium, fill=text_color_stats)
        y_offset += 25
    
    # Add asrbw.net branding at bottom
    asrbw_net_text = "asrbw.net"
    asrbw_net_text_width, asrbw_net_text_height = draw.textsize(asrbw_net_text, font=font_small)
    draw.text((card_width - asrbw_net_text_width - 10, card_height - asrbw_net_text_height - 10), asrbw_net_text, font=font_small, fill=(150, 150, 150, 255))


    img_byte_arr = BytesIO()
    img.save(img_byte_arr, format="PNG")
    img_byte_arr.seek(0)
    return img_byte_arr

async def generate_game_results_image(game_info: dict):
    """Generates a game results image similar to image_d029b6.jpg."""
    card_width, card_height = 700, 450
    background_color = (44, 47, 51, 255) # Discord dark theme color
    win_color = (50, 205, 50, 255) # Lime Green
    loss_color = (220, 20, 60, 255) # Crimson Red
    text_color_primary = (255, 255, 255, 255)
    text_color_secondary = (200, 200, 200, 255)
    asrbw_color = (255, 200, 0, 255) # Gold color

    img = Image.new('RGBA', (card_width, card_height), background_color)
    draw = ImageDraw.Draw(img)

    # Rounded corners
    radius = 20
    mask = Image.new('L', (card_width, card_height), 0)
    draw_mask = ImageDraw.Draw(mask)
    draw_mask.rounded_rectangle([(0,0), (card_width, card_height)], radius, fill=255)
    img = Image.composite(img, Image.new('RGBA', (card_width, card_height), (0,0,0,0)), mask)

    # Load fonts
    try:
        font_asrbw = ImageFont.truetype(FONT_PATH, 35)
        font_title = ImageFont.truetype(FONT_PATH, 28)
        font_player = ImageFont.truetype(FONT_PATH, 22)
        font_elo_change = ImageFont.truetype(FONT_PATH, 18)
        font_small = ImageFont.truetype(FONT_PATH, 16)
    except IOError:
        print(f"Warning: Font file '{FONT_PATH}' not found. Using default Pillow font.")
        font_asrbw = ImageFont.load_default()
        font_title = ImageFont.load_default()
        font_player = ImageFont.load_default()
        font_elo_change = ImageFont.load_default()
        font_small = ImageFont.load_default()

    # Header: KRANKED BEDWARS
    draw.text((20, 15), "KRANKED BEDWARS", font=font_asrbw, fill=asrbw_color)
    
    # Game ID
    draw.text((card_width - 150, 20), f"Game {game_info['game_id'][:8].upper()}", font=font_small, fill=text_color_secondary)
    draw.text((card_width - 150, 40), "/rankedbedwars", font=font_small, fill=text_color_secondary)


    y_start = 80
    line_height = 35
    padding = 15

    # Prepare player data for drawing
    # Sort players by team and then by ELO change (winners first, then losers)
    sorted_players = sorted(game_info['player_results'], key=lambda x: (not x['is_winner'], x['elo_after']), reverse=True)

    current_y = y_start
    for player_data in sorted_players:
        player_name = player_data['minecraft_username']
        elo_before = int(player_data['elo_before'])
        elo_after = int(player_data['elo_after'])
        elo_change = int(player_data['elo_change'])
        is_winner = player_data['is_winner']
        is_mvp = player_data['is_mvp']

        # Determine background color for player row
        row_color = win_color if is_winner else loss_color
        
        # Draw background rectangle for player row
        draw.rounded_rectangle([(padding, current_y), (card_width - padding, current_y + line_height)], 10, fill=row_color)

        # Draw player name
        draw.text((padding + 10, current_y + 5), player_name, font=font_player, fill=text_color_primary)

        # Draw MVP icon if applicable
        if is_mvp:
            draw.text((padding + draw.textsize(player_name, font=font_player)[0] + 20, current_y + 5), "🏆 MVP", font=font_small, fill=text_color_primary)

        # Draw ELO change
        elo_text = f"{elo_before} → {elo_after} ({'+' if elo_change >= 0 else ''}{elo_change})"
        elo_text_width, _ = draw.textsize(elo_text, font=font_elo_change)
        draw.text((card_width - padding - elo_text_width - 10, current_y + 10), elo_text, font=font_elo_change, fill=text_color_primary)
        
        current_y += line_height + 5 # Add some spacing between rows

    # Add asrbw.net branding at bottom
    asrbw_net_text = "asrbw.net"
    asrbw_net_text_width, asrbw_net_text_height = draw.textsize(asrbw_net_text, font=font_small)
    draw.text((card_width - asrbw_net_text_width - 10, card_height - asrbw_net_text_height - 10), asrbw_net_text, font=font_small, fill=(150, 150, 150, 255))

    img_byte_arr = BytesIO()
    img.save(img_byte_arr, format="PNG")
    img_byte_arr.seek(0)
    return img_byte_arr

# --- Commands ---

@bot.command(name='register')
async def register(ctx, minecraft_username: str = None):
    """
    Registers your Discord account with your Minecraft account.
    Can only be used in the designated REGISTER_CHANNEL_ID.
    Usage: =register <Minecraft_Username>
    """
    if ctx.channel.id != REGISTER_CHANNEL_ID:
        embed = discord.Embed(
            title="🚫 Command Restricted",
            description=f"This command can only be used in <#{REGISTER_CHANNEL_ID}>.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    if minecraft_username is None:
        embed = discord.Embed(
            title="📝 Missing Username",
            description="Please provide your Minecraft username. Usage: `=register <Minecraft_Username>`",
            color=discord.Color.orange()
        )
        return await ctx.send(embed=embed)

    discord_id = str(ctx.author.id)
    code = await generate_registration_code()
    await store_registration_code(discord_id, code)

    embed = discord.Embed(
        title="🔗 Minecraft Account Registration",
        description=f"To link your Discord account with your Minecraft account, please join the ASRBW server (`play.asrbw.net`) and type the following command in chat within **{REGISTRATION_CODE_EXPIRY_MINUTES} minutes**:\n\n"
                    f"```/register {code}```\n\n"
                    f"Your Minecraft username will be set to: `{minecraft_username}`. If this is incorrect, please re-run the command with the correct username.",
        color=discord.Color.blue()
    )
    embed.set_footer(text="This code will expire soon. Powered by asrbw.net")
    try:
        await ctx.author.send(embed=embed)
        response_embed = discord.Embed(
            title="📬 Registration Instructions Sent!",
            description=f"{ctx.author.mention}, I've sent you a DM with instructions to complete your registration!",
            color=discord.Color.green()
        )
        await ctx.send(embed=response_embed)
    except discord.Forbidden:
        error_embed = discord.Embed(
            title="❌ DM Failed",
            description=f"{ctx.author.mention}, I couldn't send you a DM. Please check your privacy settings to allow DMs from server members.",
            color=discord.Color.red()
        )
        await ctx.send(embed=error_embed)

@bot.command(name='unregister')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def unregister_command(ctx, member: discord.Member):
    """
    (Moderation) Unregisters a Discord user, removing their Minecraft link and stats.
    Usage: =unregister <@member>
    """
    discord_id = str(member.id)
    success = await unregister_user_in_db(discord_id)

    if success:
        response_embed = discord.Embed(
            title="🗑️ User Unregistered",
            description=f"Successfully unregistered {member.mention}. Their Minecraft account link and stats have been removed.",
            color=discord.Color.green()
        )
        await ctx.send(embed=response_embed)
        await log_alert(f"Admin {ctx.author.display_name} (`{ctx.author.id}`) unregistered {member.display_name} ({discord_id}).")
    else:
        error_embed = discord.Embed(
            title="❌ Unregistration Failed",
            description=f"Failed to unregister {member.mention}. They might not be registered or an error occurred.",
            color=discord.Color.red()
        )
        await ctx.send(embed=error_embed)

@unregister_command.error
async def unregister_command_error(ctx, error):
    if isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Argument", description="Missing arguments. Usage: `=unregister <@member>`", color=discord.Color.orange()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in unregister command: {error}")


@bot.command(name='i') # Changed from 'stats' to 'i'
async def stats(ctx, target_user: discord.Member = None):
    """
    Displays your (or another player's) Bedwars statistics with a custom image card.
    Usage: =i [Discord_User]
    """
    if target_user is None:
        target_user = ctx.author

    discord_id = str(target_user.id)
    player_stats = await get_player_stats_from_db(discord_id)

    if not player_stats or not player_stats.get('minecraft_uuid'):
        if target_user == ctx.author:
            response_embed = discord.Embed(
                title="❌ Not Registered",
                description="You are not registered. Please use `=register <Minecraft_Username>` to link your Minecraft account.",
                color=discord.Color.red()
            )
        else:
            response_embed = discord.Embed(
                title="❌ Player Not Registered",
                description=f"{target_user.display_name} is not registered with an ASRBW Minecraft account.",
                color=discord.Color.red()
            )
        return await ctx.send(embed=response_embed)

    try:
        # Generate the stats image
        image_buffer = await generate_player_stats_image(player_stats, target_user)
        file = File(image_buffer, filename="player_stats_card.png")

        embed = discord.Embed(
            title=f"📊 {player_stats.get('minecraft_username', 'N/A')}'s Bedwars Stats",
            description="For more information, visit [asrbw.net](https://asrbw.net)",
            color=discord.Color.gold()
        )
        embed.set_image(url="attachment://player_stats_card.png")
        embed.set_footer(text=f"Requested by {ctx.author.display_name} | Powered by ASRBW.net")

        await ctx.send(file=file, embed=embed)

    except Exception as e:
        error_embed = discord.Embed(
            title="⚠️ Image Generation Failed",
            description=f"An error occurred while generating the stats card: {e}\n"
                        "Please ensure the bot has necessary permissions and the font file is present.",
            color=discord.Color.red()
        )
        await ctx.send(embed=error_embed)
        print(f"Error generating stats card for {player_stats.get('minecraft_username')}: {e}")


@bot.command(name='forceregister')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def forceregister(ctx, member: discord.Member, minecraft_username: str):
    """
    (Moderation) Manually registers a Discord user with a Minecraft username.
    Usage: =forceregister <@member> <Minecraft_Username>
    """
    discord_id = str(member.id)
    try:
        minecraft_uuid = str(uuid.uuid4()) # Generate a random UUID

        success = await register_user_in_db(discord_id, minecraft_uuid, minecraft_username)

        if success:
            response_embed = discord.Embed(
                title="✅ Force Registration Successful",
                description=f"Successfully force-registered {member.mention} with Minecraft Username: `{minecraft_username}`.",
                color=discord.Color.green()
            )
            await ctx.send(embed=response_embed)
            await log_alert(f"Admin {ctx.author.display_name} (`{ctx.author.id}`) force-registered {member.display_name} ({discord_id}) with Minecraft Username `{minecraft_username}`.")
        else:
            error_embed = discord.Embed(
                title="❌ Force Registration Failed",
                description=f"Failed to force-register {member.mention}. An error occurred.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed)
    except Exception as e:
        error_embed = discord.Embed(
            title="⚠️ Error",
            description=f"An error occurred during force registration: {e}",
            color=discord.Color.red()
        )
        await ctx.send(embed=error_embed)
        print(f"Error in forceregister command: {e}")

@forceregister.error
async def forceregister_error(ctx, error):
    if isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=forceregister <@member> <Minecraft_Username>`", color=discord.Color.orange()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in forceregister command: {error}")

@bot.command(name='forcescore')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def forcescore(ctx, member: discord.Member, new_elo: float):
    """
    (Moderation) Manually sets a player's ELO.
    Usage: =forcescore <@member> <New_ELO>
    """
    discord_id = str(member.id)

    if not isinstance(new_elo, (int, float)):
        try:
            new_elo = float(new_elo)
        except ValueError:
            error_embed = discord.Embed(
                title="❌ Invalid ELO",
                description="Invalid ELO. Please provide a numerical value.",
                color=discord.Color.red()
            )
            return await ctx.send(embed=error_embed)

    try:
        player_stats = await get_player_stats_from_db(discord_id)
        if not player_stats:
            error_embed = discord.Embed(
                title="❌ User Not Registered",
                description=f"User {member.mention} is not registered. Cannot update ELO.",
                color=discord.Color.red()
            )
            return await ctx.send(embed=error_embed)

        success = await update_player_stats_in_db(
            discord_id,
            new_elo, # Unified ELO
            player_stats.get('wins', 0),
            player_stats.get('losses', 0),
            player_stats.get('games_played', 0),
            player_stats.get('wlr', 0.0),
            player_stats.get('kills', 0),
            player_stats.get('deaths', 0),
            player_stats.get('beds_broken', 0),
            player_stats.get('final_kills', 0),
            player_stats.get('mvp_count', 0),
            player_stats.get('strikes', 0),
            player_stats.get('game_history', [])
        )

        if success:
            response_embed = discord.Embed(
                title="✅ ELO Updated",
                description=f"Successfully set {member.mention}'s ELO to {int(new_elo)}.",
                color=discord.Color.green()
            )
            await ctx.send(embed=response_embed)
            await log_alert(f"Admin {ctx.author.display_name} (`{ctx.author.id}`) manually set {member.display_name}'s ({discord_id}) ELO to {int(new_elo)}.")
            guild = bot.get_guild(MAIN_GUILD_ID)
            if guild and member:
                await update_player_rank_role(member, new_elo) # Update rank role after manual ELO change
        else:
            error_embed = discord.Embed(
                title="❌ ELO Update Failed",
                description=f"Failed to update {member.mention}'s ELO. An error occurred.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed)

    except Exception as e:
        error_embed = discord.Embed(
            title="⚠️ Error",
            description=f"An error occurred: {e}",
            color=discord.Color.red()
        )
        await ctx.send(embed=error_embed)
        print(f"Error in forcescore command: {e}")

@forcescore.error
async def forcescore_error(ctx, error):
    if isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=forcescore <@member> <New_ELO>`", color=discord.Color.orange()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in forcescore command: {error}")

# --- Moderation Commands with Duration ---

@bot.command(name='mute')
@commands.has_permissions(manage_roles=True)
@commands.bot_has_permissions(manage_roles=True)
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def mute(ctx, member: Union[discord.Member, int], duration_str: str, *, reason: str = "No reason provided"):
    """
    Mutes a member for a specified duration.
    Usage: =mute <@member|member_id> <duration> [reason] (e.g., 30m, 1h, 7d)
    """
    if isinstance(member, int): # If user provided an ID, try to fetch the member
        try:
            member = await ctx.guild.fetch_member(member)
        except discord.NotFound:
            error_embed = discord.Embed(title="🔍 Member Not Found", description=f"Could not find a member with ID `{member}` in this server.", color=discord.Color.red())
            return await ctx.send(embed=error_embed)
        except Exception as e:
            error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while fetching member: {e}", color=discord.Color.red())
            return await ctx.send(embed=error_embed)

    if ctx.author.top_role.position <= member.top_role.position and ctx.author.id != ctx.guild.owner_id:
        embed = discord.Embed(title="🚫 Permission Denied", description=f"You cannot mute {member.display_name} as their role is equal to or higher than yours.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if ctx.guild.me.top_role.position <= member.top_role.position:
        embed = discord.Embed(title="❌ Bot Permission Issue", description=f"I cannot mute {member.display_name} as their role is equal to or higher than mine. Please adjust my role hierarchy.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    muted_role = ctx.guild.get_role(MUTED_ROLE_ID)
    if not muted_role:
        embed = discord.Embed(title="❌ Configuration Error", description="Muted role not found! Please configure `MUTED_ROLE_ID` correctly.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if muted_role.position >= ctx.guild.me.top_role.position:
        embed = discord.Embed(title="❌ Bot Role Hierarchy", description=f"I cannot assign the muted role as it is equal to or higher than my highest role. Please move my role above the muted role.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    if muted_role in member.roles:
        embed = discord.Embed(title="⚠️ Already Muted", description=f"{member.mention} is already muted.", color=discord.Color.orange())
        return await ctx.send(embed=embed)

    duration = parse_duration(duration_str)
    if not duration:
        embed = discord.Embed(title="❌ Invalid Duration", description="Invalid duration format. Use formats like `30m`, `1h`, `7d`.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    end_time = datetime.datetime.now(datetime.timezone.utc) + duration
    end_timestamp = int(end_time.timestamp())

    try:
        await member.add_roles(muted_role, reason=f"Muted by {ctx.author.name} for {duration_str}: {reason}")
        success = await store_temp_moderation_mysql(str(member.id), 'muted', end_timestamp, reason)
        if success:
            response_embed = discord.Embed(
                title="🔇 User Muted",
                description=f"{member.mention} has been muted for **{duration_str}**.",
                color=discord.Color.green()
            )
            response_embed.add_field(name="Reason", value=reason, inline=False)
            response_embed.add_field(name="Muted Until", value=f"<t:{end_timestamp}:F>", inline=False)
            await ctx.send(embed=response_embed)
            await log_strike_event(f"🔇 {member.display_name} (`{member.id}`) has been muted by {ctx.author.display_name} (`{ctx.author.id}`) for {duration_str}. Reason: {reason}")
        else:
            error_embed = discord.Embed(title="❌ Mute Failed", description=f"Muted {member.mention} but failed to store temporary mute. Auto-unmute may not occur.", color=discord.Color.red())
            await ctx.send(embed=error_embed)
    except discord.Forbidden:
        error_embed = discord.Embed(title="❌ Bot Permission Issue", description="I don't have permission to manage roles. Make sure I have 'Manage Roles' permission and my role is above the member's and the muted role.", color=discord.Color.red())
        await ctx.send(embed=error_embed)
    except Exception as e:
        error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while muting: {e}", color=discord.Color.red())
        await ctx.send(embed=error_embed)
        print(f"Error muting {member.display_name}: {e}")

@mute.error
async def mute_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You don't have the required 'Manage Roles' permission to use this command.", color=discord.Color.red()))
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Bot Permission Issue", description="I don't have the required 'Manage Roles' permission to perform this action.", color=discord.Color.red()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=mute <@member|member_id> <duration> [reason]`", color=discord.Color.orange()))
    elif isinstance(error, commands.BadArgument) and "Could not convert" in str(error):
        await ctx.send(embed=discord.Embed(title="❌ Invalid Input", description="Invalid member or duration format. Please check your input.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in mute command: {error}")


@bot.command(name='unmute')
@commands.has_permissions(manage_roles=True)
@commands.bot_has_permissions(manage_roles=True)
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def unmute(ctx, member: Union[discord.Member, int], *, reason: str = "No reason provided"):
    """
    Unmutes a member by removing the Muted role.
    Usage: =unmute <@member|member_id> [reason]
    """
    if isinstance(member, int): # If user provided an ID, try to fetch the member
        try:
            member = await ctx.guild.fetch_member(member)
        except discord.NotFound:
            error_embed = discord.Embed(title="🔍 Member Not Found", description=f"Could not find a member with ID `{member}` in this server.", color=discord.Color.red())
            return await ctx.send(embed=error_embed)
        except Exception as e:
            error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while fetching member: {e}", color=discord.Color.red())
            return await ctx.send(embed=error_embed)

    if ctx.author.top_role.position <= member.top_role.position and ctx.author.id != ctx.guild.owner_id:
        embed = discord.Embed(title="🚫 Permission Denied", description=f"You cannot unmute {member.display_name} as their role is equal to or higher than yours.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if ctx.guild.me.top_role.position <= member.top_role.position:
        embed = discord.Embed(title="❌ Bot Permission Issue", description=f"I cannot unmute {member.display_name} as their role is equal to or higher than mine. Please adjust my role hierarchy.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    muted_role = ctx.guild.get_role(MUTED_ROLE_ID)
    if not muted_role:
        embed = discord.Embed(title="❌ Configuration Error", description="Muted role not found! Please configure `MUTED_ROLE_ID` correctly.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if muted_role.position >= ctx.guild.me.top_role.position:
        embed = discord.Embed(title="❌ Bot Role Hierarchy", description=f"I cannot remove the muted role as it is equal to or higher than my highest role. Please move my role above the muted role.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    if muted_role not in member.roles:
        embed = discord.Embed(title="⚠️ Not Muted", description=f"{member.mention} is not muted.", color=discord.Color.orange())
        return await ctx.send(embed=embed)

    try:
        await member.remove_roles(muted_role, reason=f"Unmuted by {ctx.author.name}: {reason}")
        success = await remove_temp_moderation_mysql(str(member.id), 'muted')
        if success:
            response_embed = discord.Embed(
                title="🔊 User Unmuted",
                description=f"{member.mention} has been unmuted.",
                color=discord.Color.green()
            )
            response_embed.add_field(name="Reason", value=reason, inline=False)
            await ctx.send(embed=response_embed)
            await log_strike_event(f"🔊 {member.display_name} (`{member.id}`) has been unmuted by {ctx.author.display_name} (`{ctx.author.id}`). Reason: {reason}")
        else:
            error_embed = discord.Embed(title="⚠️ Unmute Warning", description=f"Unmuted {member.mention} but failed to remove temporary mute record from database.", color=discord.Color.orange())
            await ctx.send(embed=error_embed)
    except discord.Forbidden:
        error_embed = discord.Embed(title="❌ Bot Permission Issue", description="I don't have permission to manage roles. Make sure I have 'Manage Roles' permission.", color=discord.Color.red())
        await ctx.send(embed=error_embed)
    except Exception as e:
        error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while unmuting: {e}", color=discord.Color.red())
        await ctx.send(embed=error_embed)
        print(f"Error unmuting {member.display_name}: {e}")

@unmute.error
async def unmute_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You don't have the required 'Manage Roles' permission to use this command.", color=discord.Color.red()))
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Bot Permission Issue", description="I don't have the required 'Manage Roles' permission to perform this action.", color=discord.Color.red()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=unmute <@member|member_id> [reason]`", color=discord.Color.orange()))
    elif isinstance(error, commands.BadArgument) and "Could not convert" in str(error):
        await ctx.send(embed=discord.Embed(title="❌ Invalid Input", description="Invalid member format. Please check your input.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in unmute command: {error}")


@bot.command(name='ban')
@commands.has_permissions(manage_roles=True)
@commands.bot_has_permissions(manage_roles=True)
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def ban(ctx, member: Union[discord.Member, int], duration_str: str, *, reason: str = "No reason provided"):
    """
    Assigns a 'banned' role to a member for a specified duration, preventing them from queuing.
    Usage: =ban <@member|member_id> <duration> [reason] (e.g., 30m, 1h, 7d, 'perm' for permanent)
    """
    if isinstance(member, int): # If user provided an ID, try to fetch the member
        try:
            member = await ctx.guild.fetch_member(member)
        except discord.NotFound:
            error_embed = discord.Embed(title="🔍 Member Not Found", description=f"Could not find a member with ID `{member}` in this server.", color=discord.Color.red())
            return await ctx.send(embed=error_embed)
        except Exception as e:
            error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while fetching member: {e}", color=discord.Color.red())
            return await ctx.send(embed=error_embed)

    if ctx.author.top_role.position <= member.top_role.position and ctx.author.id != ctx.guild.owner_id:
        embed = discord.Embed(title="🚫 Permission Denied", description=f"You cannot apply the ban role to {member.display_name} as their role is equal to or higher than yours.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if ctx.guild.me.top_role.position <= member.top_role.position:
        embed = discord.Embed(title="❌ Bot Permission Issue", description=f"I cannot apply the ban role to {member.display_name} as their role is equal to or higher than mine. Please adjust my role hierarchy.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if member.id == ctx.guild.owner_id:
        embed = discord.Embed(title="🚫 Cannot Ban Owner", description=f"I cannot apply the ban role to the server owner.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    banned_role = ctx.guild.get_role(BANNED_ROLE_ID)
    if not banned_role:
        embed = discord.Embed(title="❌ Configuration Error", description="Banned role not found! Please configure `BANNED_ROLE_ID` correctly.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if banned_role.position >= ctx.guild.me.top_role.position:
        embed = discord.Embed(title="❌ Bot Role Hierarchy", description=f"I cannot assign the banned role as it is equal to or higher than my highest role. Please move my role above the banned role.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    if banned_role in member.roles:
        embed = discord.Embed(title="⚠️ Already Banned", description=f"{member.mention} already has the banned role.", color=discord.Color.orange())
        return await ctx.send(embed=embed)

    duration = None
    end_timestamp = None
    duration_display = "permanently"
    if duration_str.lower() != 'perm':
        duration = parse_duration(duration_str)
        if not duration:
            embed = discord.Embed(title="❌ Invalid Duration", description="Invalid duration format. Use formats like `30m`, `1h`, `7d`, or `perm` for permanent.", color=discord.Color.red())
            return await ctx.send(embed=embed)
        end_time = datetime.datetime.now(datetime.timezone.utc) + duration
        end_timestamp = int(end_time.timestamp())
        duration_display = duration_str

    try:
        await member.add_roles(banned_role, reason=f"Ban role assigned by {ctx.author.name} for {duration_display}: {reason}")
        
        if duration_str.lower() != 'perm':
            success = await store_temp_moderation_mysql(str(member.id), 'banned', end_timestamp, reason)
            if success:
                response_embed = discord.Embed(
                    title="🚫 User Banned (Role Assigned)",
                    description=f"{member.mention} has been given the banned role for **{duration_display}**.",
                    color=discord.Color.red()
                )
                response_embed.add_field(name="Reason", value=reason, inline=False)
                response_embed.add_field(name="Banned Until", value=f"<t:{end_timestamp}:F>", inline=False)
                await ctx.send(embed=response_embed)
                await log_strike_event(f"🚫 {member.display_name} (`{member.id}`) has been given the banned role by {ctx.author.display_name} (`{ctx.author.id}`) for {duration_display}. Reason: {reason}")
            else:
                error_embed = discord.Embed(title="❌ Ban Failed", description=f"Assigned banned role to {member.mention} but failed to store temporary ban. Auto-role-removal may not occur.", color=discord.Color.red())
                await ctx.send(embed=error_embed)
        else:
            response_embed = discord.Embed(
                title="🚫 User Permanently Banned (Role Assigned)",
                description=f"{member.mention} has been permanently given the banned role.",
                color=discord.Color.red()
            )
            response_embed.add_field(name="Reason", value=reason, inline=False)
            await ctx.send(embed=response_embed)
            await log_strike_event(f"🚫 {member.display_name} (`{member.id}`) has been permanently given the banned role by {ctx.author.display_name} (`{ctx.author.id}`). Reason: {reason}")

    except discord.Forbidden:
        error_embed = discord.Embed(title="❌ Bot Permission Issue", description="I don't have permission to manage roles. Make sure I have 'Manage Roles' permission and my role is above the member's and the banned role.", color=discord.Color.red())
        await ctx.send(embed=error_embed)
    except Exception as e:
        error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while assigning ban role: {e}", color=discord.Color.red())
        await ctx.send(embed=error_embed)
        print(f"Error assigning ban role to {member.display_name}: {e}")

@ban.error
async def ban_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You don't have the required 'Manage Roles' permission to use this command.", color=discord.Color.red()))
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Bot Permission Issue", description="I don't have the required 'Manage Roles' permission to perform this action.", color=discord.Color.red()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=ban <@member|member_id> <duration> [reason]`", color=discord.Color.orange()))
    elif isinstance(error, commands.BadArgument) and "Could not convert" in str(error):
        await ctx.send(embed=discord.Embed(title="❌ Invalid Input", description="Invalid member or duration format. Please check your input.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in ban command: {error}")


@bot.command(name='unban')
@commands.has_permissions(manage_roles=True)
@commands.bot_has_permissions(manage_roles=True)
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def unban(ctx, member: Union[discord.Member, int], *, reason: str = "No reason provided"):
    """
    Removes the 'banned' role from a member.
    Usage: =unban <@member|member_id> [reason]
    """
    if isinstance(member, int): # If user provided an ID, try to fetch the member
        try:
            member = await ctx.guild.fetch_member(member)
        except discord.NotFound:
            error_embed = discord.Embed(title="🔍 Member Not Found", description=f"Could not find a member with ID `{member}` in this server.", color=discord.Color.red())
            return await ctx.send(embed=error_embed)
        except Exception as e:
            error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while fetching member: {e}", color=discord.Color.red())
            return await ctx.send(embed=error_embed)

    if ctx.author.top_role.position <= member.top_role.position and ctx.author.id != ctx.guild.owner_id:
        embed = discord.Embed(title="🚫 Permission Denied", description=f"You cannot remove the ban role from {member.display_name} as their role is equal to or higher than yours.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if ctx.guild.me.top_role.position <= member.top_role.position:
        embed = discord.Embed(title="❌ Bot Permission Issue", description=f"I cannot remove the ban role from {member.display_name} as their role is equal to or higher than mine. Please adjust my role hierarchy.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    banned_role = ctx.guild.get_role(BANNED_ROLE_ID)
    if not banned_role:
        embed = discord.Embed(title="❌ Configuration Error", description="Banned role not found! Please configure `BANNED_ROLE_ID` correctly.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    if banned_role.position >= ctx.guild.me.top_role.position:
        embed = discord.Embed(title="❌ Bot Role Hierarchy", description=f"I cannot remove the banned role as it is equal to or higher than my highest role. Please move my role above the banned role.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    if banned_role not in member.roles:
        embed = discord.Embed(title="⚠️ Not Banned", description=f"{member.mention} does not have the banned role.", color=discord.Color.orange())
        return await ctx.send(embed=embed)

    try:
        await member.remove_roles(banned_role, reason=f"Ban role removed by {ctx.author.name}: {reason}")
        success = await remove_temp_moderation_mysql(str(member.id), 'banned')
        if success:
            response_embed = discord.Embed(
                title="🔓 User Unbanned (Role Removed)",
                description=f"{member.mention} has had the banned role removed.",
                color=discord.Color.green()
            )
            response_embed.add_field(name="Reason", value=reason, inline=False)
            await ctx.send(embed=response_embed)
            await log_strike_event(f"🔓 {member.display_name} (`{member.id}`) has had the banned role removed by {ctx.author.display_name} (`{ctx.author.id}`). Reason: {reason}")
        else:
            error_embed = discord.Embed(title="⚠️ Unban Warning", description=f"Removed banned role from {member.mention} but failed to remove temporary ban record from database.", color=discord.Color.orange())
            await ctx.send(embed=error_embed)
    except discord.Forbidden:
        error_embed = discord.Embed(title="❌ Bot Permission Issue", description="I don't have permission to manage roles. Make sure I have 'Manage Roles' permission.", color=discord.Color.red())
        await ctx.send(embed=error_embed)
    except Exception as e:
        error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred while removing ban role: {e}", color=discord.Color.red())
        await ctx.send(embed=error_embed)
        print(f"Error removing ban role from {member.display_name}: {e}")

@unban.error
async def unban_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You don't have the required 'Manage Roles' permission to use this command.", color=discord.Color.red()))
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send(embed=discord.Embed(title="⛔ Bot Permission Issue", description="I don't have the required 'Manage Roles' permission to perform this action.", color=discord.Color.red()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=unban <@member|member_id> [reason]`", color=discord.Color.orange()))
    elif isinstance(error, commands.BadArgument) and "Could not convert" in str(error):
        await ctx.send(embed=discord.Embed(title="❌ Invalid Input", description="Invalid member format. Please check your input.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in unban command: {error}")

# --- Stat Modification Commands ---

async def apply_stat_changes(discord_id: str, is_win: bool, game_type: str, is_mvp: bool = False, game_id: str = None):
    """
    Applies ELO and stat changes for a single player for a single game.
    Returns a dictionary of changes made, or None if player not found.
    The game_type is for logging in game_history, not ELO calculation.
    """
    player_stats = await get_player_stats_from_db(discord_id)
    if not player_stats:
        return None # Player not registered

    current_elo = player_stats.get('elo', DEFAULT_ELO) # Unified ELO
    wins = player_stats.get('wins', 0)
    losses = player_stats.get('losses', 0)
    games_played = player_stats.get('games_played', 0)
    mvp_count = player_stats.get('mvp_count', 0)
    game_history = player_stats.get('game_history', [])

    elo_change = 0
    elo_before = current_elo

    if is_win:
        elo_gain = ELO_THRESHOLDS.get(get_rank_from_elo(elo_before), ELO_THRESHOLDS["Bronze"])["win_gain"]
        elo_change = elo_gain
        if is_mvp:
            mvp_bonus = ELO_THRESHOLDS.get(get_rank_from_elo(elo_before), ELO_THRESHOLDS["Bronze"])["mvp_bonus"]
            elo_change += mvp_bonus
            mvp_count += 1
        wins += 1
    else: # is_loss
        elo_deduct = ELO_THRESHOLDS.get(get_rank_from_elo(elo_before), ELO_THRESHOLDS["Bronze"])["loss_deduct"]
        elo_change = -elo_deduct
        losses += 1
    
    games_played += 1
    wlr = wins / losses if losses > 0 else wins

    current_elo += elo_change
    current_elo = max(0, current_elo) # ELO cannot go below 0

    elo_after = current_elo

    # Record game in history
    game_entry = {
        "game_id": game_id if game_id else str(uuid.uuid4()),
        "timestamp": int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
        "game_type": game_type, # Still log game type for historical context
        "is_winner": is_win,
        "is_mvp": is_mvp,
        "elo_before": elo_before,
        "elo_after": elo_after,
        "elo_change": elo_change,
        "wins_change": 1 if is_win else 0,
        "losses_change": 1 if not is_win else 0,
        "games_played_change": 1,
        "mvp_count_change": 1 if is_mvp else 0,
        "kills_change": 0, "deaths_change": 0, "beds_broken_change": 0, "final_kills_change": 0 # Placeholder
    }
    game_history.append(game_entry)

    success = await update_player_stats_in_db(
        discord_id,
        current_elo, # Unified ELO
        wins,
        losses,
        games_played,
        wlr,
        player_stats.get('kills', 0),
        player_stats.get('deaths', 0),
        player_stats.get('beds_broken', 0),
        player_stats.get('final_kills', 0),
        mvp_count,
        player_stats.get('strikes', 0),
        game_history
    )
    if success:
        return {
            "discord_id": discord_id,
            "elo_change": elo_change,
            "elo_before": elo_before,
            "elo_after": elo_after,
            "wins_change": 1 if is_win else 0,
            "losses_change": 1 if not is_win else 0,
            "games_played_change": 1,
            "mvp_count_change": 1 if is_mvp else 0,
            "game_entry": game_entry
        }
    return None

async def revert_stat_changes(discord_id: str, game_entry: dict):
    """
    Reverts ELO and stat changes for a single player based on a game entry.
    """
    player_stats = await get_player_stats_from_db(discord_id)
    if not player_stats:
        print(f"Cannot revert for unregistered player {discord_id}.")
        return False

    current_elo = player_stats.get('elo', DEFAULT_ELO) # Unified ELO
    wins = player_stats.get('wins', 0)
    losses = player_stats.get('losses', 0)
    games_played = player_stats.get('games_played', 0)
    mvp_count = player_stats.get('mvp_count', 0)
    game_history = player_stats.get('game_history', [])

    original_game_history_len = len(game_history)
    game_history = [entry for entry in game_history if entry.get('game_id') != game_entry['game_id']]
    if len(game_history) == original_game_history_len:
        print(f"Game ID {game_entry['game_id']} not found in history for {discord_id}. Cannot revert.")
        return False

    current_elo -= game_entry['elo_change']
    wins -= game_entry.get('wins_change', 0)
    losses -= game_entry.get('losses_change', 0)
    games_played -= game_entry.get('games_played_change', 0)
    mvp_count -= game_entry.get('mvp_count_change', 0)

    wins = max(0, wins)
    losses = max(0, losses)
    games_played = max(0, games_played)
    mvp_count = max(0, mvp_count)
    wlr = wins / losses if losses > 0 else wins

    success = await update_player_stats_in_db(
        discord_id,
        current_elo,
        wins,
        losses,
        games_played,
        wlr,
        player_stats.get('kills', 0),
        player_stats.get('deaths', 0),
        player_stats.get('beds_broken', 0),
        player_stats.get('final_kills', 0),
        mvp_count,
        player_stats.get('strikes', 0),
        game_history
    )
    return success

@bot.command(name='win')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def win(ctx, player: discord.Member, game_type: str = "general", is_mvp: bool = False):
    """
    (Moderation) Manually awards a win to a player.
    Usage: =win <@player> [game_type (e.g., 3v3, 4v4)] [is_mvp (True/False)]
    """
    game_type = game_type.lower()
    if game_type not in ['3v3', '4v4', 'general']: # Allow 'general' for manual wins not tied to specific mode
        embed = discord.Embed(title="❌ Invalid Game Type", description="Invalid game type. Use '3v3', '4v4', or 'general'.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    discord_id = str(player.id)
    game_id = str(uuid.uuid4())

    result = await apply_stat_changes(discord_id, True, game_type, is_mvp, game_id)

    if result:
        response_embed = discord.Embed(
            title="✅ Win Recorded",
            description=f"Awarded win to {player.mention} in {game_type.upper()} mode.",
            color=discord.Color.green()
        )
        response_embed.add_field(name="ELO Change", value=result['elo_change'], inline=True)
        response_embed.add_field(name="New ELO", value=int(result['elo_after']), inline=True)
        response_embed.add_field(name="MVP", value="Yes" if is_mvp else "No", inline=True)
        response_embed.set_footer(text=f"Game ID: {game_id}")
        await ctx.send(embed=response_embed)
        await log_alert(f"Admin {ctx.author.display_name} manually awarded win to {player.display_name} ({discord_id}) in {game_type.upper()} mode. ELO Change: {result['elo_change']}. MVP: {is_mvp}. Game ID: `{game_id}`.")
    else:
        error_embed = discord.Embed(title="❌ Operation Failed", description=f"Failed to award win to {player.mention}. Player might not be registered.", color=discord.Color.red())
        await ctx.send(embed=error_embed)

@win.error
async def win_error(ctx, error):
    if isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=win <@player> [game_type (e.g., 3v3, 4v4)] [is_mvp (True/False)]`", color=discord.Color.orange()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    elif isinstance(error, commands.BadArgument) and "Could not convert" in str(error):
        await ctx.send(embed=discord.Embed(title="❌ Invalid Input", description="Invalid value for `is_mvp`. Please use `True` or `False`.", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in win command: {error}")

@bot.command(name='loss')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def loss(ctx, player: discord.Member, game_type: str = "general"):
    """
    (Moderation) Manually records a loss for a player.
    Usage: =loss <@player> [game_type (e.g., 3v3, 4v4)]
    """
    game_type = game_type.lower()
    if game_type not in ['3v3', '4v4', 'general']:
        embed = discord.Embed(title="❌ Invalid Game Type", description="Invalid game type. Use '3v3', '4v4', or 'general'.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    discord_id = str(player.id)
    game_id = str(uuid.uuid4())

    result = await apply_stat_changes(discord_id, False, game_type, False, game_id)

    if result:
        response_embed = discord.Embed(
            title="✅ Loss Recorded",
            description=f"Recorded loss for {player.mention} in {game_type.upper()} mode.",
            color=discord.Color.green()
        )
        response_embed.add_field(name="ELO Change", value=result['elo_change'], inline=True)
        response_embed.add_field(name="New ELO", value=int(result['elo_after']), inline=True)
        response_embed.set_footer(text=f"Game ID: {game_id}")
        await ctx.send(embed=response_embed)
        await log_alert(f"Admin {ctx.author.display_name} manually recorded loss for {player.display_name} ({discord_id}) in {game_type.upper()} mode. ELO Change: {result['elo_change']}. Game ID: `{game_id}`.")
    else:
        error_embed = discord.Embed(title="❌ Operation Failed", description=f"Failed to record loss for {player.mention}. Player might not be registered.", color=discord.Color.red())
        await ctx.send(embed=error_embed)

@loss.error
async def loss_error(ctx, error):
    if isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=loss <@player> [game_type (e.g., 3v3, 4v4)]`", color=discord.Color.orange()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in loss command: {error}")

@bot.command(name='undo')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def undo_game_score(ctx, game_id: str):
    """
    (Moderation) Undoes the ELO and stat changes for a specific game.
    Usage: =undo <game_id>
    """
    guild = ctx.guild
    affected_players_info = []

    all_accounts = await execute_query("SELECT discord_id, game_history FROM accounts", fetchall=True)
    if not all_accounts:
        embed = discord.Embed(title="❌ No Player Data", description="No player data found in the database to undo games.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    found_game = False
    players_to_revert = defaultdict(list)

    for account in all_accounts:
        discord_id = account['discord_id']
        game_history = json.loads(account['game_history']) if account['game_history'] else []
        
        for entry in game_history:
            if entry.get('game_id') == game_id:
                players_to_revert[discord_id].append(entry)
                found_game = True
                break

    if not found_game:
        embed = discord.Embed(title="🔍 Game Not Found", description=f"Game with ID `{game_id}` not found in any player's history.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    for discord_id, entries_to_revert in players_to_revert.items():
        member = guild.get_member(int(discord_id))
        member_name = member.display_name if member else f"Unknown User ({discord_id})"

        for entry in entries_to_revert:
            success = await revert_stat_changes(discord_id, entry)
            if success:
                affected_players_info.append(f"✅ Reverted {member_name}'s stats for game `{game_id}` (ELO change: {-entry['elo_change']}).")
                if member:
                    await update_discord_nickname(member) # Update nickname after undo
                    # Fetch updated stats to get current ELO for rank role update
                    updated_player_stats = await get_player_stats_from_db(discord_id)
                    if updated_player_stats:
                        await update_player_rank_role(member, updated_player_stats.get('elo', DEFAULT_ELO))
            else:
                affected_players_info.append(f"❌ Failed to revert {member_name}'s stats for game `{game_id}`.")

    if affected_players_info:
        response_embed = discord.Embed(
            title="↩️ Game Score Undo Results",
            description="\n".join(affected_players_info),
            color=discord.Color.blue()
        )
        await ctx.send(embed=response_embed)
        await log_alert(f"Admin {ctx.author.display_name} (`{ctx.author.id}`) performed `=undo` for game ID `{game_id}`. Details:\n" + "\n".join(affected_players_info))
    else:
        embed = discord.Embed(title="⚠️ No Changes Applied", description=f"No changes were applied for game ID `{game_id}` during undo.", color=discord.Color.orange())
        await ctx.send(embed=embed)

@undo_game_score.error
async def undo_game_score_error(ctx, error):
    if isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=undo <game_id>`", color=discord.Color.orange()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in undo command: {error}")

@bot.command(name='rescore')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def rescore_game(ctx, game_id: str, winning_player_ids_str: str, mvp_player: Union[discord.Member, int] = None, new_game_type: str = "general"):
    """
    (Moderation) Rescores a previous game. First undoes, then applies new scores.
    Usage: =rescore <game_id> <winning_player_discord_ids> [mvp_player (@member|member_id)] [new_game_type (e.g., 3v3, 4v4)]
    winning_player_discord_ids should be a comma-separated list of Discord IDs (e.g., '123,456,789')
    """
    guild = ctx.guild
    
    all_accounts = await execute_query("SELECT discord_id, game_history FROM accounts", fetchall=True)
    if not all_accounts:
        embed = discord.Embed(title="❌ No Player Data", description="No player data found in the database to rescore games.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    original_game_entries = {}
    
    for account in all_accounts:
        discord_id = account['discord_id']
        game_history = json.loads(account['game_history']) if account['game_history'] else []
        
        for entry in game_history:
            if entry.get('game_id') == game_id:
                original_game_entries[discord_id] = entry
                break
    
    if not original_game_entries:
        embed = discord.Embed(title="🔍 Game Not Found", description=f"Game with ID `{game_id}` not found in any player's history. Cannot rescore.", color=discord.Color.red())
        return await ctx.send(embed=embed)
    
    if new_game_type.lower() not in ['3v3', '4v4', 'general']:
        embed = discord.Embed(title="❌ Invalid Game Type", description="Invalid new game type. Use '3v3', '4v4', or 'general'.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    # Resolve MVP player if provided as ID
    resolved_mvp_member = None
    if mvp_player:
        if isinstance(mvp_player, int):
            try:
                resolved_mvp_member = await ctx.guild.fetch_member(mvp_player)
            except discord.NotFound:
                embed = discord.Embed(title="🔍 MVP Member Not Found", description=f"Could not find MVP member with ID `{mvp_player}`.", color=discord.Color.red())
                return await ctx.send(embed=embed)
            except Exception as e:
                embed = discord.Embed(title="⚠️ Error", description=f"Error fetching MVP member: {e}", color=discord.Color.red())
                return await ctx.send(embed=embed)
        else: # It's already a discord.Member object
            resolved_mvp_member = mvp_player

    # 1. Undo the original scoring for all involved players
    undo_results = []
    for discord_id, entry in original_game_entries.items():
        member = guild.get_member(int(discord_id))
        member_name = member.display_name if member else f"Unknown User ({discord_id})"
        success = await revert_stat_changes(discord_id, entry)
        if success:
            undo_results.append(f"✅ Undid {member_name}'s original score for game `{game_id}`.")
        else:
            undo_results.append(f"❌ Failed to undo {member_name}'s original score for game `{game_id}`.")
    
    undo_embed = discord.Embed(
        title="↩️ Rescore: Undoing Original Scores",
        description="\n".join(undo_results),
        color=discord.Color.blue()
    )
    await ctx.send(embed=undo_embed)
    await log_alert(f"Admin {ctx.author.display_name} (`{ctx.author.id}`) initiating `=rescore` for game ID `{game_id}`. Original scores undone.")

    # 2. Apply new scores
    winning_player_ids = [did.strip() for did in winning_player_ids_str.split(',') if did.strip().isdigit()]
    if not winning_player_ids:
        embed = discord.Embed(title="❌ Invalid Winning Players", description="Invalid winning player Discord IDs provided. Please use comma-separated IDs.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    mvp_id = str(resolved_mvp_member.id) if resolved_mvp_member else None

    rescore_results = []
    all_involved_ids = list(original_game_entries.keys()) # All players who were in the original game
    
    # Collect data for image generation
    players_for_image = []

    for discord_id in all_involved_ids:
        member = guild.get_member(int(discord_id))
        member_name = member.display_name if member else f"Unknown User ({discord_id})"

        is_winner = discord_id in winning_player_ids
        is_mvp = (discord_id == mvp_id)

        result = await apply_stat_changes(discord_id, is_winner, new_game_type, is_mvp, game_id) # Reuse original game_id

        if result:
            rescore_results.append(f"✅ Applied new score for {member_name}. ELO Change: {result['elo_change']}. New ELO: {int(result['elo_after'])}. Win: {is_winner}. MVP: {is_mvp}.")
            if member: await update_discord_nickname(member) # Nickname and rank role updated inside update_player_stats_in_db
            
            # Prepare data for image
            players_for_image.append({
                "discord_id": discord_id,
                "minecraft_username": (await get_player_stats_from_db(discord_id)).get('minecraft_username', member_name),
                "elo_before": result['elo_before'],
                "elo_after": result['elo_after'],
                "elo_change": result['elo_change'],
                "is_winner": is_winner,
                "is_mvp": is_mvp
            })
        else:
            rescore_results.append(f"❌ Failed to apply new score for {member_name}.")

    if rescore_results:
        rescore_embed = discord.Embed(
            title="🔄 Rescore: New Scores Applied",
            description="\n".join(rescore_results),
            color=discord.Color.green()
        )
        await ctx.send(embed=rescore_embed)
        await log_alert(f"Admin {ctx.author.display_name} (`{ctx.author.id}`) completed `=rescore` for game ID `{game_id}`. New scores applied:\n" + "\n".join(rescore_results))

        # Generate and send game results image
        game_info_for_image = {
            "game_id": game_id,
            "game_type": new_game_type,
            "player_results": players_for_image
        }
        try:
            image_buffer = await generate_game_results_image(game_info_for_image)
            file = File(image_buffer, filename=f"game_results_{game_id[:8]}.png")
            games_results_channel = bot.get_channel(GAMES_RESULTS_CHANNEL_ID)
            if games_results_channel:
                await games_results_channel.send(f"📊 **Game #{game_id[:8]} Rescore Results:**", file=file)
            else:
                await ctx.send(embed=discord.Embed(title="⚠️ Warning", description="Game results channel not found, could not post image.", color=discord.Color.orange()))
        except Exception as e:
            await ctx.send(embed=discord.Embed(title="❌ Image Generation Failed", description=f"Failed to generate game results image: {e}", color=discord.Color.red()))
            print(f"Error generating game results image for rescore {game_id}: {e}")

    else:
        embed = discord.Embed(title="⚠️ No New Scores Applied", description=f"No new scores were applied for game ID `{game_id}`.", color=discord.Color.orange())
        await ctx.send(embed=embed)

@rescore_game.error
async def rescore_game_error(ctx, error):
    if isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=rescore <game_id> <winning_player_discord_ids> [mvp_player (@member|member_id)] [new_game_type]`", color=discord.Color.orange()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find the MVP member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    elif isinstance(error, commands.BadArgument) and "Could not convert" in str(error):
        await ctx.send(embed=discord.Embed(title="❌ Invalid Input", description="Invalid player ID or game type format. Please check your input.", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in rescore command: {error}")

# --- Leaderboard Command ---
@bot.command(name='lb')
async def leaderboard(ctx):
    """
    Displays the top 10 players on the Bedwars ELO leaderboard.
    Usage: =lb
    """
    all_players_data = await execute_query(
        "SELECT discord_id, minecraft_username, elo FROM accounts WHERE minecraft_uuid IS NOT NULL",
        fetchall=True
    )

    if not all_players_data:
        embed = discord.Embed(title="Empty Leaderboard", description="No registered players found to display a leaderboard.", color=discord.Color.blue())
        return await ctx.send(embed=embed)

    sorted_players = sorted(all_players_data, key=lambda x: x.get('elo', DEFAULT_ELO), reverse=True)

    leaderboard_entries = []
    for i, player in enumerate(sorted_players[:10]):
        discord_id = player['discord_id']
        mc_username = player.get('minecraft_username', 'N/A')
        elo = int(player.get('elo', DEFAULT_ELO))
        rank = get_rank_from_elo(elo)
        
        member = ctx.guild.get_member(int(discord_id))
        display_name = member.display_name if member else mc_username

        leaderboard_entries.append(f"**#{i+1}** {display_name} (`{mc_username}`) - ELO: {elo} ({rank})")

    embed = discord.Embed(
        title="🏆 Top 10 Bedwars Leaderboard",
        description="\n".join(leaderboard_entries),
        color=discord.Color.blue()
    )
    embed.set_footer(text="ELO based on all games. Powered by asrbw.net")

    await ctx.send(embed=embed)

# --- Ticket System Rework ---
class TicketTypeSelect(View):
    def __init__(self, bot_instance, author_id):
        super().__init__(timeout=180)
        self.bot = bot_instance
        self.author_id = author_id

        self.add_item(
            Select(
                custom_id="ticket_type_select",
                placeholder="Select Ticket Type...",
                options=[
                    SelectOption(label="General Ticket", value="general", description="For general questions or issues."),
                    SelectOption(label="Appeal Ticket", value="appeal", description="To appeal a ban or punishment."),
                    SelectOption(label="Shop Ticket", value="shop", description="For issues related to in-game purchases or shop.")
                ]
            )
        )

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if hasattr(self, 'message'):
            try:
                await self.message.edit(content="Ticket creation timed out.", view=self)
            except discord.NotFound:
                pass

    @discord.ui.select(custom_id="ticket_type_select")
    async def select_ticket_type(self, interaction: discord.Interaction, select: Select):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This is not your ticket menu!", ephemeral=True)
            return

        selected_type = select.values[0]
        await interaction.response.send_message(f"Creating a {selected_type} ticket...", ephemeral=True)

        guild = interaction.guild
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True)
        }
        for role_id in MODERATION_ROLES_IDS:
            role = guild.get_role(role_id)
            if role:
                overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)

        channel_name = f"{selected_type}-{interaction.user.name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:4]}"
        try:
            ticket_category = guild.get_channel(TICKET_CATEGORY_ID)
            if not ticket_category:
                await log_alert(f"Ticket category (ID: {TICKET_CATEGORY_ID}) not found for ticket channel creation.")
                await interaction.followup.send(embed=discord.Embed(title="❌ Error", description="Ticket category not found. Please contact an admin.", color=discord.Color.red()), ephemeral=True)
                return

            ticket_channel = await guild.create_text_channel(
                channel_name,
                category=ticket_category,
                overwrites=overwrites,
                topic=f"{selected_type.capitalize()} ticket for {interaction.user.display_name}"
            )
            print(f"Created ticket channel: {ticket_channel.name}")

            embed = discord.Embed(
                title=f"🎫 {selected_type.capitalize()} Ticket",
                description=f"{interaction.user.mention} has opened a {selected_type} ticket. Staff will be with you shortly.",
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Ticket ID: {uuid.uuid4().hex[:8]} | Powered by asrbw.net")
            
            await ticket_channel.send(f"{interaction.user.mention} {' '.join([guild.get_role(r_id).mention for r_id in MODERATION_ROLES_IDS if guild.get_role(r_id)])}", embed=embed)
            await log_ticket_event(f"New {selected_type} ticket opened by {interaction.user.display_name} in {ticket_channel.mention}.")
            await interaction.message.edit(content=f"Your ticket has been created: {ticket_channel.mention}", view=None) # Disable view
        except discord.Forbidden:
            await log_alert(f"Bot lacks permissions to create ticket channel in category {ticket_category.name}. Ensure 'Manage Channels' permission is granted.")
            await interaction.followup.send(embed=discord.Embed(title="❌ Bot Permission Issue", description="I don't have permission to create channels. Please contact an admin.", color=discord.Color.red()), ephemeral=True)
        except Exception as e:
            await log_alert(f"Error creating ticket channel: {e}")
            print(f"Error creating ticket channel: {e}")
            await interaction.followup.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {e}", color=discord.Color.red()), ephemeral=True)

@bot.command(name='ticket')
async def create_ticket(ctx):
    """
    Initiates the ticket creation process, allowing selection of ticket type.
    Must be used in the designated TICKET_CHANNEL_ID.
    Usage: =ticket
    """
    if ctx.channel.id != TICKET_CHANNEL_ID:
        embed = discord.Embed(
            title="🚫 Command Restricted",
            description=f"This command can only be used in <#{TICKET_CHANNEL_ID}>.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    view = TicketTypeSelect(bot, ctx.author.id)
    message = await ctx.send(embed=discord.Embed(title="🎫 Open a New Ticket", description="Please select the type of ticket you'd like to open:", color=discord.Color.blue()), view=view)
    view.message = message # Store message for timeout editing

# --- Commands Requiring Image Attachments and Channel Restrictions ---

@bot.command(name='requeststrike')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def requeststrike(ctx, target_player: discord.Member, *, reason: str = "No reason provided"):
    """
    (Moderation) Requests a strike against a player. Requires an image attachment as proof.
    Must be used in STRIKE_REQUEST_CHANNEL_ID (unless user is above Highest Admin).
    Usage: =requeststrike <@User> [reason] (attach image)
    """
    if ctx.channel.id != STRIKE_REQUEST_CHANNEL_ID and not await is_above_highest_admin_role(ctx.author):
        embed = discord.Embed(title="🚫 Command Restricted", description=f"This command can only be used in <#{STRIKE_REQUEST_CHANNEL_ID}>.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    if not ctx.message.attachments:
        embed = discord.Embed(title="⚠️ Missing Proof", description="Please attach an image as proof for your strike request.", color=discord.Color.orange())
        return await ctx.send(embed=embed)

    strike_channel = await create_strike_channel(ctx.guild, ctx.author, target_player, reason, ctx.message.attachments)

    if strike_channel:
        response_embed = discord.Embed(
            title="✅ Strike Request Submitted",
            description=f"Strike request for {target_player.mention} submitted. Discussion channel: {strike_channel.mention}",
            color=discord.Color.green()
        )
        await ctx.send(embed=response_embed)
        await log_strike_event(f"Strike requested by {ctx.author.display_name} for {target_player.display_name} for '{reason}'. Discussion in {strike_channel.mention}. Attachments: {[att.url for att in ctx.message.attachments]}")
    else:
        error_embed = discord.Embed(title="❌ Request Failed", description="Failed to create strike channel. Request not fully processed.", color=discord.Color.red())
        await ctx.send(embed=error_embed)
        await log_alert(f"Failed to create strike channel for {target_player.display_name} by {ctx.author.display_name}.")

@requeststrike.error
async def requeststrike_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=requeststrike <@User> [reason]` (and attach image).", color=discord.Color.orange()))
    elif isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in requeststrike command: {error}")


@bot.command(name='requestss')
@commands.has_any_role(*MODERATION_ROLES_IDS)
async def requestss(ctx, target_player: discord.Member, *, reason: str = "No reason provided"):
    """
    (Moderation) Requests a screenshare for a player. Requires an image attachment as proof.
    Usage: =requestss <@User> [reason] (attach image)
    """
    if not ctx.message.attachments:
        embed = discord.Embed(title="⚠️ Missing Proof", description="Please attach an image as proof for your screenshare request.", color=discord.Color.orange())
        return await ctx.send(embed=embed)

    screenshare_channel = await create_screenshare_channel(ctx.guild, ctx.author, target_player, reason, ctx.message.attachments)

    if screenshare_channel:
        response_embed = discord.Embed(
            title="✅ Screenshare Request Submitted",
            description=f"Screenshare request for {target_player.mention} submitted. Discussion channel: {screenshare_channel.mention}",
            color=discord.Color.green()
        )
        await ctx.send(embed=response_embed)
        await log_screenshare_event(f"Screenshare requested by {ctx.author.display_name} for {target_player.display_name} for '{reason}'. Discussion in {screenshare_channel.mention}. Attachments: {[att.url for att in ctx.message.attachments]}")
    else:
        error_embed = discord.Embed(title="❌ Request Failed", description="Failed to create screenshare channel. Request not fully processed.", color=discord.Color.red())
        await ctx.send(embed=error_embed)
        await log_alert(f"Failed to create screenshare channel for {target_player.display_name} by {ctx.author.display_name}.")

@requestss.error
async def requestss_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=requestss <@User> [reason]` (and attach image).", color=discord.Color.orange()))
    elif isinstance(error, commands.MissingAnyRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description="You do not have permission to use this command. You need a moderation role.", color=discord.Color.red()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in requestss command: {error}")

@bot.command(name='poll')
@commands.has_role(PPP_MANAGER_ROLE_ID)
async def poll(ctx, poll_type: str, target_member: discord.Member, *, question: str):
    """
    (PPP Manager) Starts a new poll for a specific user/context.
    Must be used in POLL_CHANNEL_ID (unless user is above Highest Admin).
    Usage: =poll <type> <@target_member> <Your Poll Question>
    Example: =poll pups @User123 Should this user be allowed in PPP?
    """
    if ctx.channel.id != POLL_CHANNEL_ID and not await is_above_highest_admin_role(ctx.author):
        embed = discord.Embed(title="🚫 Command Restricted", description=f"This command can only be used in <#{POLL_CHANNEL_ID}>.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    poll_type = poll_type.lower()
    if poll_type not in ['pups', 'general']: # Extend with more types as needed
        embed = discord.Embed(title="❌ Invalid Poll Type", description="Invalid poll type. Currently supported types: `pups`, `general`.", color=discord.Color.red())
        return await ctx.send(embed=embed)

    embed = discord.Embed(
        title=f"📊 New {poll_type.upper()} Poll",
        description=f"**Question:** {question}\n\n**Target:** {target_member.mention}\n\nReact with ✅ for Yes, ❌ for No.",
        color=discord.Color.green()
    )
    embed.set_footer(text=f"Poll started by {ctx.author.display_name} | Powered by asrbw.net")
    poll_message = await ctx.send(embed=embed)
    await poll_message.add_reaction("✅")
    await poll_message.add_reaction("❌")
    try:
        await ctx.message.delete()
        response_embed = discord.Embed(title="✅ Poll Created", description=f"Poll created in {poll_message.channel.mention}!", color=discord.Color.green())
        await ctx.send(embed=response_embed, delete_after=5)
    except discord.Forbidden:
        response_embed = discord.Embed(title="⚠️ Warning", description="Poll created, but I couldn't delete your command message (missing 'Manage Messages' permission).", color=discord.Color.orange())
        await ctx.send(embed=response_embed)


@poll.error
async def poll_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Arguments", description="Missing arguments. Usage: `=poll <type> <@target_member> <Your Poll Question>`", color=discord.Color.orange()))
    elif isinstance(error, commands.MissingRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description=f"You do not have permission to use this command. You need the `{ctx.guild.get_role(PPP_MANAGER_ROLE_ID).name}` role.", color=discord.Color.red()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find the target member. Please mention them properly (e.g., `@Username` or their ID).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in poll command: {error}")

# --- Purge All Data Command ---
class PurgeConfirmationView(View):
    def __init__(self, author_id):
        super().__init__(timeout=30) # 30 seconds to confirm
        self.author_id = author_id
        self.confirmed = False

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if hasattr(self, 'message'):
            try:
                await self.message.edit(content="Purge operation timed out. Data was NOT purged.", view=self)
            except discord.NotFound:
                pass

    @discord.ui.button(label="Yes, Purge All Data", style=ButtonStyle.danger)
    async def confirm_purge(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This confirmation is not for you!", ephemeral=True)
            return

        self.confirmed = True
        self.stop() # Stop the view to prevent further interactions
        await interaction.response.edit_message(content="Confirmation received. Purging data...", view=None)

    @discord.ui.button(label="No, Cancel", style=ButtonStyle.secondary)
    async def cancel_purge(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This cancellation is not for you!", ephemeral=True)
            return

        self.confirmed = False
        self.stop() # Stop the view
        await interaction.response.edit_message(content="Purge operation cancelled.", view=None)

@bot.command(name='purgeall')
@commands.has_role(HIGHEST_ADMIN_ROLE_ID)
async def purge_all_data(ctx):
    """
    (Highest Admin) Purges all user data and statistics from the database.
    Requires explicit confirmation. Resets Discord roles and nicknames for all members.
    Usage: =purgeall
    """
    embed = discord.Embed(
        title="⚠️ DANGER: Purge All Data Confirmation",
        description=(
            "You are about to **delete ALL user registration data and statistics** "
            "from the ASRBW database. This action is **irreversible**.\n\n"
            "After purging the database, I will attempt to **remove all registered and rank roles** "
            "from all members, and **reset their nicknames** to their default Discord names.\n\n"
            "Are you absolutely sure you want to proceed?"
        ),
        color=discord.Color.red()
    )

    view = PurgeConfirmationView(ctx.author.id)
    confirmation_message = await ctx.send(embed=embed, view=view)
    view.message = confirmation_message # Store message for timeout editing

    await view.wait() # Wait for the user to click a button

    if view.confirmed:
        try:
            # Truncate the accounts table
            success = await execute_query("TRUNCATE TABLE accounts")
            if success:
                response_embed = discord.Embed(
                    title="✅ Data Purged",
                    description="All user data and statistics have been purged from the database.",
                    color=discord.Color.green()
                )
                await ctx.send(embed=response_embed)
                await log_alert(f"🚨 **CRITICAL ACTION:** All user data and statistics purged by {ctx.author.display_name} (`{ctx.author.id}`).")
                
                # Now, reset Discord roles and nicknames for all members
                guild = ctx.guild
                registered_role = guild.get_role(REGISTERED_ROLE_ID)
                all_rank_roles = [guild.get_role(r_id) for r_id in ALL_RANK_ROLE_IDS if guild.get_role(r_id)]

                await ctx.send(embed=discord.Embed(title="🔄 Resetting Discord Roles & Nicknames", description="Attempting to reset Discord roles and nicknames for all members. This may take a moment...", color=discord.Color.blue()))
                
                members_processed = 0
                members_failed_roles = []
                members_failed_nicknames = []

                async for member in guild.fetch_members(limit=None): # Fetch all members
                    members_processed += 1
                    roles_to_remove_from_member = []
                    
                    if registered_role and registered_role in member.roles:
                        roles_to_remove_from_member.append(registered_role)
                    
                    for role_obj in all_rank_roles:
                        if role_obj and role_obj in member.roles:
                            roles_to_remove_from_member.append(role_obj)
                    
                    if roles_to_remove_from_member:
                        try:
                            await member.remove_roles(*roles_to_remove_from_member, reason="Purge all data - reset roles")
                            print(f"Removed roles from {member.display_name}: {[r.name for r in roles_to_remove_from_member]}")
                        except discord.Forbidden:
                            members_failed_roles.append(f"{member.display_name} (Forbidden)")
                            print(f"Failed to remove roles from {member.display_name}: Forbidden")
                        except Exception as e:
                            members_failed_roles.append(f"{member.display_name} ({e})")
                            print(f"Failed to remove roles from {member.display_name}: {e}")

                    # Reset nickname to default (None reverts to original Discord username)
                    if member.nick is not None:
                        try:
                            await member.edit(nick=None, reason="Purge all data - reset nickname")
                            print(f"Reset nickname for {member.display_name}")
                        except discord.Forbidden:
                            members_failed_nicknames.append(f"{member.display_name} (Forbidden)")
                            print(f"Failed to reset nickname for {member.display_name}: Forbidden")
                        except Exception as e:
                            members_failed_nicknames.append(f"{member.display_name} ({e})")
                            print(f"Failed to reset nickname for {member.display_name}: {e}")
                
                summary_message = f"Finished processing {members_processed} members.\n"
                if members_failed_roles:
                    summary_message += f"⚠️ Failed to remove roles from {len(members_failed_roles)} members: {', '.join(members_failed_roles[:5])}...\n"
                if members_failed_nicknames:
                    summary_message += f"⚠️ Failed to reset nicknames for {len(members_failed_nicknames)} members: {', '.join(members_failed_nicknames[:5])}...\n"
                
                await ctx.send(embed=discord.Embed(title="✅ Role & Nickname Reset Summary", description=summary_message, color=discord.Color.blue()))
                await log_alert(f"Purge operation completed. Role/Nickname reset summary:\n{summary_message}")

            else:
                error_embed = discord.Embed(title="❌ Purge Failed", description="Failed to purge data. An error occurred with the database operation.", color=discord.Color.red())
                await ctx.send(embed=error_embed)
                await log_alert(f"❌ **ERROR:** Failed to purge data by {ctx.author.display_name} (`{ctx.author.id}`).")
        except Exception as e:
            error_embed = discord.Embed(title="⚠️ Error", description=f"An error occurred during the purge operation: {e}", color=discord.Color.red())
            await ctx.send(embed=error_embed)
            await log_alert(f"❌ **ERROR:** Exception during purge operation by {ctx.author.display_name} (`{ctx.author.id}`): {e}")
    else:
        # Message already updated by the view's cancel_purge method
        pass

@purge_all_data.error
async def purge_all_data_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send(embed=discord.Embed(title="⛔ Permission Denied", description=f"You do not have permission to use this command. You need the `{ctx.guild.get_role(HIGHEST_ADMIN_ROLE_ID).name}` role.", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in purgeall command: {error}")


# --- Placeholder/Example Functions ---

# This function would typically be called by your Minecraft plugin via a webhook or similar.
# For demonstration, you can manually call it or create a test command.
async def process_game_results(game_data: dict):
    """
    Processes game results received from the Minecraft plugin,
    updates player ELO and stats, and logs the game.
    game_data example:
    {
        "game_id": "D8A1A",
        "game_type": "4v4",
        "winning_team_name": "Team A", # Optional, for display
        "mvp_discord_id": "123456789012345678",
        "player_results": [
            {"discord_id": "123456789012345678", "is_winner": True, "is_mvp": True, "elo_before": 700, "elo_after": 720, "elo_change": 20, "minecraft_username": "CDTago3"},
            {"discord_id": "123456789012345679", "is_winner": True, "is_mvp": False, "elo_before": 650, "elo_after": 675, "elo_change": 25, "minecraft_username": "p4cd"},
            # ... other players
        ]
    }
    """
    print(f"Processing game results: {game_data}")
    
    guild = bot.get_guild(MAIN_GUILD_ID)
    if not guild:
        print(f"Guild with ID {MAIN_GUILD_ID} not found for processing game results.")
        await log_alert(f"Failed to process game results for game {game_data.get('game_id', 'N/A')}: Guild not found.")
        return

    game_id = game_data.get('game_id', str(uuid.uuid4()))
    
    # Apply ELO changes and update stats for each player
    for player_result in game_data.get('player_results', []):
        discord_id = player_result['discord_id']
        is_win = player_result.get('is_winner', False)
        is_mvp = player_result.get('is_mvp', False)
        game_type = game_data.get('game_type', 'general') # Game type from plugin
        
        # This will update player stats in DB and also their nickname/rank role
        await apply_stat_changes(discord_id, is_win, game_type, is_mvp, game_id)

    # Generate and send game results image to the dedicated channel
    games_results_channel = bot.get_channel(GAMES_RESULTS_CHANNEL_ID)
    if games_results_channel:
        try:
            image_buffer = await generate_game_results_image(game_data)
            file = File(image_buffer, filename=f"game_results_{game_id}.png")
            await games_results_channel.send(f"📊 **Game {game_id} Results:**", file=file)
            await log_game_event(f"Game {game_id} finished. Results posted to {games_results_channel.mention}.")
        except Exception as e:
            print(f"Error generating/sending game results image for game {game_id}: {e}")
            await log_alert(f"Error generating/sending game results image for game {game_id}: {e}")
            # Fallback to text if image fails
            embed = discord.Embed(
                title=f"📊 Game {game_id} Results (Image Failed)",
                description="An error occurred displaying the game results image. Here's a text summary:",
                color=discord.Color.orange()
            )
            winning_players_str = "\n".join([p['minecraft_username'] for p in game_data['player_results'] if p['is_winner']])
            losing_players_str = "\n".join([p['minecraft_username'] for p in game_data['player_results'] if not p['is_winner']])
            mvp_player_name = next((p['minecraft_username'] for p in game_data['player_results'] if p.get('is_mvp')), 'N/A')

            embed.add_field(name="Winning Team", value=winning_players_str or "N/A", inline=False)
            embed.add_field(name="Losing Team", value=losing_players_str or "N/A", inline=False)
            embed.add_field(name="MVP", value=mvp_player_name, inline=False)
            embed.set_footer(text=f"Powered by asrbw.net")
            await games_results_channel.send(embed=embed)
    else:
        print(f"Games results channel (ID: {GAMES_RESULTS_CHANNEL_ID}) not found. Cannot post game results.")
        await log_alert(f"Games results channel (ID: {GAMES_RESULTS_CHANNEL_ID}) not found. Game {game_id} results not posted.")

# --- Party System ---
class PartyInviteView(View):
    def __init__(self, owner_id: str, invited_id: str):
        super().__init__(timeout=180) # Invite expires in 3 minutes
        self.owner_id = owner_id
        self.invited_id = invited_id
        self.accepted = False

    async def on_timeout(self):
        owner_member = bot.get_guild(MAIN_GUILD_ID).get_member(int(self.owner_id))
        invited_member = bot.get_guild(MAIN_GUILD_ID).get_member(int(self.invited_id))
        
        if not self.accepted:
            if owner_member:
                try:
                    await owner_member.send(embed=discord.Embed(
                        title="⏱️ Party Invite Expired",
                        description=f"Your invite to {invited_member.display_name if invited_member else 'an unknown user'} has expired.",
                        color=discord.Color.orange()
                    ))
                except discord.Forbidden: pass # Cannot DM
            if invited_member:
                try:
                    await invited_member.send(embed=discord.Embed(
                        title="⏱️ Party Invite Expired",
                        description=f"The party invite from {owner_member.display_name if owner_member else 'an unknown user'} has expired.",
                        color=discord.Color.orange()
                    ))
                except discord.Forbidden: pass # Cannot DM
        
        # Clean up pending invite
        if self.owner_id in parties and self.invited_id in parties[self.owner_id]["invite_pending"]:
            del parties[self.owner_id]["invite_pending"][self.invited_id]
        
        for child in self.children:
            child.disabled = True
        if hasattr(self, 'message'):
            try:
                await self.message.edit(view=self)
            except discord.NotFound: pass

    @discord.ui.button(label="Accept", style=ButtonStyle.green)
    async def accept_invite(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.invited_id:
            await interaction.response.send_message("This invite is not for you!", ephemeral=True)
            return

        owner_member = interaction.guild.get_member(int(self.owner_id))
        
        # Check if either is already in a party
        if str(interaction.user.id) in player_party_map:
            await interaction.response.send_message("You are already in a party!", ephemeral=True)
            return
        if self.owner_id in player_party_map and player_party_map[self.owner_id] != self.owner_id:
            await interaction.response.send_message("The party owner is no longer the owner or is in another party.", ephemeral=True)
            return
        if self.owner_id in parties and len(parties[self.owner_id]["members"]) >= 2:
            await interaction.response.send_message("This party is already full!", ephemeral=True)
            return

        # Add to party
        parties[self.owner_id]["members"].append(str(interaction.user.id))
        player_party_map[str(interaction.user.id)] = self.owner_id
        
        self.accepted = True
        self.stop() # Stop the view

        # Clean up pending invite
        if self.owner_id in parties and self.invited_id in parties[self.owner_id]["invite_pending"]:
            del parties[self.owner_id]["invite_pending"][self.invited_id]

        await interaction.response.send_message(f"You have joined {owner_member.display_name}'s party!", ephemeral=True)
        if owner_member:
            try:
                await owner_member.send(embed=discord.Embed(
                    title="🎉 Party Invite Accepted!",
                    description=f"{interaction.user.display_name} has joined your party!",
                    color=discord.Color.green()
                ))
            except discord.Forbidden: pass # Cannot DM
        
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)


    @discord.ui.button(label="Decline", style=ButtonStyle.red)
    async def decline_invite(self, interaction: discord.Interaction, button: Button):
        if str(interaction.user.id) != self.invited_id:
            await interaction.response.send_message("This invite is not for you!", ephemeral=True)
            return
        
        owner_member = interaction.guild.get_member(int(self.owner_id))

        self.stop() # Stop the view

        # Clean up pending invite
        if self.owner_id in parties and self.invited_id in parties[self.owner_id]["invite_pending"]:
            del parties[self.owner_id]["invite_pending"][self.invited_id]

        await interaction.response.send_message(f"You have declined the party invite from {owner_member.display_name}.", ephemeral=True)
        if owner_member:
            try:
                await owner_member.send(embed=discord.Embed(
                    title="Party Invite Declined",
                    description=f"{interaction.user.display_name} has declined your party invite.",
                    color=discord.Color.red()
                ))
            except discord.Forbidden: pass # Cannot DM
        
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)


@bot.group(name='party', invoke_without_command=True)
async def party(ctx):
    """
    Manages your party. Use `=help party` for subcommands.
    """
    if ctx.invoked_subcommand is None:
        embed = discord.Embed(
            title="🎉 Party System",
            description="Manage your party with these subcommands:\n"
                        "`=party create` - Create your own party.\n"
                        "`=party invite <@member>` - Invite a member to your party (max 1).\n"
                        "`=party status` - View your current party status.\n"
                        "`=leave` - Leave your current party (or disband if owner).",
            color=discord.Color.blue()
        )
        embed.set_footer(text="Powered by asrbw.net")
        await ctx.send(embed=embed)

@party.command(name='create')
async def party_create(ctx):
    """
    Creates a new party. You will be the party owner.
    """
    user_id = str(ctx.author.id)
    if user_id in player_party_map:
        embed = discord.Embed(
            title="❌ Already in a Party",
            description="You are already in a party. Use `=leave` to leave your current party.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)
    
    parties[user_id]["members"].append(user_id)
    player_party_map[user_id] = user_id

    embed = discord.Embed(
        title="✅ Party Created!",
        description=f"You have created a party! You are the party owner. "
                    f"Use `=party invite <@member>` to invite one person.",
        color=discord.Color.green()
    )
    embed.set_footer(text="Powered by asrbw.net")
    await ctx.send(embed=embed)

@party.command(name='invite')
async def party_invite(ctx, member: discord.Member):
    """
    Invites a member to your party (max 1 invited member).
    Usage: =party invite <@member>
    """
    owner_id = str(ctx.author.id)
    invited_id = str(member.id)

    if owner_id not in parties or player_party_map.get(owner_id) != owner_id:
        embed = discord.Embed(
            title="❌ Not a Party Owner",
            description="You must be a party owner to invite members. Use `=party create` first.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    if len(parties[owner_id]["members"]) >= 2:
        embed = discord.Embed(
            title="❌ Party Full",
            description="Your party is already full (max 2 members).",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    if invited_id in player_party_map:
        embed = discord.Embed(
            title="❌ Already in a Party",
            description=f"{member.display_name} is already in a party.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)
    
    if invited_id == owner_id:
        embed = discord.Embed(
            title="❌ Cannot Invite Yourself",
            description="You cannot invite yourself to your own party.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    # Check for pending invites to prevent spam
    if invited_id in parties[owner_id]["invite_pending"]:
        embed = discord.Embed(
            title="⚠️ Invite Already Sent",
            description=f"An invite has already been sent to {member.display_name} and is pending.",
            color=discord.Color.orange()
        )
        return await ctx.send(embed=embed)

    try:
        invite_embed = discord.Embed(
            title="💌 Party Invite!",
            description=f"{ctx.author.display_name} has invited you to their party!",
            color=discord.Color.blue()
        )
        invite_embed.set_footer(text="This invite will expire in 3 minutes.")
        
        view = PartyInviteView(owner_id, invited_id)
        invite_message = await member.send(embed=invite_embed, view=view)
        view.message = invite_message # Store message for timeout editing

        # Store pending invite
        parties[owner_id]["invite_pending"][invited_id] = datetime.datetime.now(datetime.timezone.utc)

        embed = discord.Embed(
            title="✅ Party Invite Sent!",
            description=f"An invite has been sent to {member.mention}.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
    except discord.Forbidden:
        embed = discord.Embed(
            title="❌ DM Failed",
            description=f"Could not send a DM to {member.mention}. They might have DMs disabled.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(
            title="⚠️ Error",
            description=f"An error occurred while sending the invite: {e}",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        print(f"Error sending party invite: {e}")

@party_invite.error
async def party_invite_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=discord.Embed(title="❓ Missing Argument", description="Please mention the member you want to invite. Usage: `=party invite <@member>`", color=discord.Color.orange()))
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=discord.Embed(title="🔍 Member Not Found", description="Could not find that member. Please mention them properly (e.g., `@Username`).", color=discord.Color.red()))
    else:
        await ctx.send(embed=discord.Embed(title="⚠️ Error", description=f"An unexpected error occurred: {error}", color=discord.Color.red()))
        print(f"Error in party invite command: {error}")

@bot.command(name='leave')
async def leave_party(ctx):
    """
    Leaves your current party. If you are the owner, the party will be disbanded.
    """
    user_id = str(ctx.author.id)

    if user_id not in player_party_map:
        embed = discord.Embed(
            title="❌ Not in a Party",
            description="You are not currently in a party.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    owner_id = player_party_map[user_id]

    if owner_id == user_id: # User is the party owner
        party_members = parties[owner_id]["members"].copy() # Get a copy before modifying
        for member_id in party_members:
            if member_id in player_party_map:
                del player_party_map[member_id]
        del parties[owner_id] # Disband the party

        embed = discord.Embed(
            title="👋 Party Disbanded",
            description="You have disbanded your party. All members have been removed.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)

        # Notify other member if any
        if len(party_members) > 1:
            other_member_id = next((m for m in party_members if m != user_id), None)
            if other_member_id:
                other_member = ctx.guild.get_member(int(other_member_id))
                if other_member:
                    try:
                        await other_member.send(embed=discord.Embed(
                            title="💔 Party Disbanded",
                            description=f"{ctx.author.display_name} has disbanded the party.",
                            color=discord.Color.red()
                        ))
                    except discord.Forbidden: pass # Cannot DM
    else: # User is a party member
        parties[owner_id]["members"].remove(user_id)
        del player_party_map[user_id]

        embed = discord.Embed(
            title="🚶‍♂️ Left Party",
            description=f"You have left the party owned by {ctx.guild.get_member(int(owner_id)).display_name}.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)

        # Notify owner
        owner_member = ctx.guild.get_member(int(owner_id))
        if owner_member:
            try:
                await owner_member.send(embed=discord.Embed(
                    title="Party Member Left",
                    description=f"{ctx.author.display_name} has left your party.",
                    color=discord.Color.orange()
                ))
            except discord.Forbidden: pass # Cannot DM

@bot.command(name='pwarp')
async def party_warp(ctx):
    """
    (Party Owner) Warps your party members to your current voice channel.
    Usage: =pwarp
    """
    user_id = str(ctx.author.id)

    if user_id not in parties or player_party_map.get(user_id) != user_id:
        embed = discord.Embed(
            title="❌ Not a Party Owner",
            description="You must be a party owner to use this command.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    if not ctx.author.voice or not ctx.author.voice.channel:
        embed = discord.Embed(
            title="❌ Not in a Voice Channel",
            description="You must be in a voice channel to warp your party members.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    target_vc = ctx.author.voice.channel
    party_info = parties[user_id]
    
    warped_members = []
    failed_members = []

    for member_id in party_info["members"]:
        if member_id == user_id: continue # Skip owner

        member = ctx.guild.get_member(int(member_id))
        if member and member.voice and member.voice.channel != target_vc:
            try:
                await member.move_to(target_vc, reason=f"Party warp by {ctx.author.display_name}")
                warped_members.append(member.display_name)
            except discord.Forbidden:
                failed_members.append(f"{member.display_name} (No permission)")
            except Exception as e:
                failed_members.append(f"{member.display_name} ({e})")
        elif member and member.voice and member.voice.channel == target_vc:
            warped_members.append(f"{member.display_name} (already there)")
        elif member:
            failed_members.append(f"{member.display_name} (Not in VC)")
        else:
            failed_members.append(f"Unknown User ({member_id})")

    if warped_members or failed_members:
        description = f"Attempted to warp party members to {target_vc.mention}.\n\n"
        if warped_members:
            description += "**Warped/Already There:**\n" + "\n".join(warped_members) + "\n"
        if failed_members:
            description += "**Failed to Warp:**\n" + "\n".join(failed_members)

        embed = discord.Embed(
            title="🚀 Party Warp Results",
            description=description,
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)
    else:
        embed = discord.Embed(
            title="ℹ️ Party Warp",
            description="No party members to warp, or all are already in your VC.",
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)

@party.command(name='status')
async def party_status(ctx):
    """
    Displays information about your current party.
    """
    user_id = str(ctx.author.id)

    if user_id not in player_party_map:
        embed = discord.Embed(
            title="❌ Not in a Party",
            description="You are not currently in a party.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    owner_id = player_party_map[user_id]
    party_info = parties.get(owner_id)

    if not party_info:
        embed = discord.Embed(
            title="❌ Party Not Found",
            description="Could not find information for your party. It might have been disbanded.",
            color=discord.Color.red()
        )
        return await ctx.send(embed=embed)

    owner_member = ctx.guild.get_member(int(owner_id))
    owner_display_name = owner_member.display_name if owner_member else f"Unknown User ({owner_id})"

    member_names = []
    for member_id in party_info["members"]:
        member = ctx.guild.get_member(int(member_id))
        if member:
            member_names.append(member.display_name)
        else:
            member_names.append(f"Unknown User ({member_id})")
    
    pending_invites_str = ""
    if party_info["invite_pending"]:
        pending_members = []
        for invited_id, timestamp in party_info["invite_pending"].items():
            invited_member = ctx.guild.get_member(int(invited_id))
            if invited_member:
                pending_members.append(invited_member.display_name)
        if pending_members:
            pending_invites_str = "\n**Pending Invites:**\n" + "\n".join(pending_members)

    embed = discord.Embed(
        title="🎉 Your Party Status",
        description=f"**Owner:** {owner_display_name}\n"
                    f"**Members:**\n" + "\n".join(member_names) + pending_invites_str,
        color=discord.Color.blue()
    )
    embed.set_footer(text="Powered by asrbw.net")
    await ctx.send(embed=embed)


# --- Run the Bot ---
bot.run(DISCORD_BOT_TOKEN)
