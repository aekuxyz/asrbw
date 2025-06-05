import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import aiomysql
import asyncio
import random
import string
import datetime
import time
from typing import Optional, List, Dict, Any
from PIL import Image, ImageDraw, ImageFont
import io
import requests
import aiohttp
import html
import os

# --- Configuration Section ---
# IMPORTANT: Replace these with your actual Discord IDs and names.
# If IDs are None, the bot will attempt to find categories/channels by name or create them.
# It is highly recommended to use IDs for stability.

# --- Configuration Section ---
# IMPORTANT: Replace these with your actual Discord IDs and names.
# If IDs are None, the bot will attempt to find categories/channels by name or create them.
# It is highly recommended to use IDs for stability.

# Category IDs
GAME_CATEGORY_ID = 1377353788226011246  # ID of your "Games" category
VOICE_CATEGORY_ID = 1377352366038454344 # ID of your "Voice Channels" category
TICKET_CATEGORY_ID = 1378238886056169533 # ID of your "Tickets" category
CLOSED_TICKETS_CATEGORY_ID = 1379869175106764813 # ID of your "Closed Tickets" category
STRIKE_REQUESTS_CATEGORY_ID = 1378389076503171083 # ID of your "Strike Requests" category

# Channel IDs (Optional, but recommended for specific channels)
REGISTER_CHANNEL_ID = 1376879395574124544 # ID of your registration channel
BAN_LOG_CHANNEL_ID = 1377355353678811278 # ID of your ban logs channel
MUTE_LOG_CHANNEL_ID = 1377355376743153775 # ID of your mute logs channel
STRIKE_LOG_CHANNEL_ID = 1377355415284875425 # ID of your strike logs channel
TICKET_CHANNEL_ID = 1377617914177392640 # ID of the channel where users create tickets
TICKET_LOG_CHANNEL_ID = 1377617800150913126 # ID of your ticket logs channel
STRIKE_REQUEST_CHANNEL_ID = 1377351296868417647 # ID of the channel where users make strike requests
SCREENSNARE_LOG_CHANNEL_ID = 1377688164923343072 # ID of your screenshare ticket logs channel
GAME_LOG_CHANNEL_ID = 1377611419234865152 # ID of your game logs channel
PPP_VOTING_CHANNEL_ID = 1378388708205527110 # ID of your #ppp-voting channel
STAFF_UPDATES_CHANNEL_ID = 1377306838793453578 # ID of your staff-updates channel
GAMES_DISPLAY_CHANNEL_ID = 1377353788226011246 # ID of the channel to display game results image
AFK_VOICE_CHANNEL_ID = 1380096256109707275 # ID of your AFK voice channel

# Channel Names (Fallback if IDs are None or channel not found by ID)
REGISTER_CHANNEL_NAME = "register"
BAN_LOG_CHANNEL_NAME = "bans"
MUTE_LOG_CHANNEL_NAME = "mutes"
STRIKE_LOG_CHANNEL_NAME = "strikes"
TICKET_CHANNEL_NAME = "tickets"
TICKET_LOG_CHANNEL_NAME = "ticket-logs"
STRIKE_REQUEST_CHANNEL_NAME = "strike-requests"
SCREENSNARE_LOG_CHANNEL_NAME = "ss-logs"
GAME_LOG_CHANNEL_NAME = "game-logs"
PPP_VOTING_CHANNEL_NAME = "ppp-poll"
STAFF_UPDATES_CHANNEL_NAME = "staff-updates"
GAMES_DISPLAY_CHANNEL_NAME = "games"

# Role Names (Used for permissions and role management)
REGISTERED_ROLE_NAME = "Registered" # Role assigned upon successful registration
UNREGISTERED_ROLE_NAME = "Unregistered" # Role for new members, removed upon registration
BANNED_ROLE_NAME = "Banned"
MUTED_ROLE_NAME = "Muted"
FROZEN_ROLE_NAME = "Frozen" # Role assigned during screenshare
PPP_MANAGER_ROLE_NAME = "P.P.P. Manager" # Role for poll command (Pups, Pugs, Premium)
MANAGER_ROLE_NAME = "Manager" # Role for modify stats command (and above)
ADMIN_STAFF_ROLE_NAME = "Administrator" # Role for game commands (and above)
STAFF_ROLE_NAME = "Staff" # Role for force register command, ticket claim (and above)
MODERATOR_ROLE_NAME = "Moderator" # Base role for staff commands (e.g., ban, mute, strike)
PI_ROLE_NAME = "𓆩 𓆪" # Role for admin commands
SCREENSHARING_TEAM_ROLE_NAME = "Screensharing" # Role for screenshare ticket access

# Database connection details
DB_HOST = "localhost"
DB_USER = "asrbw-user"
DB_PASSWORD = "asdfdaf"
DB_NAME = "asrbw_db"

# Discord Bot Token
DISCORD_BOT_TOKEN = "YOUR_DISCORD_BOT_TOKEN" # Replace with your actual bot token

# --- Global Variables and Constants ---
ELO_ROLES = {
    "Iron": (0, 150),
    "Bronze": (150, 400),
    "Silver": (400, 700),
    "Gold": (700, 900),
    "Topaz": (900, 1200),
    "Platinum": (1200, float('inf'))
}

ELO_REWARDS = { # Adjusted for manual admin commands, if needed for other places
    "Iron": {"win": 25, "loss": 10, "mvp": 20},
    "Bronze": {"win": 20, "loss": 10, "mvp": 15},
    "Silver": {"win": 20, "loss": 10, "mvp": 10},
    "Gold": {"win": 15, "loss": 10, "mvp": 10},
    "Topaz": {"win": 10, "loss": 15, "mvp": 10},
    "Platinum": {"win": 5, "loss": 20, "mvp": 10}
}

# Fixed ELO change for manual admin commands (wins/losses)
ADMIN_WIN_ELO_CHANGE = 20
ADMIN_LOSS_ELO_CHANGE = -20
ADMIN_MVP_ELO_CHANGE = 10


QUEUE_TYPES = {
    "3v3": 6,
    "4v4": 8,
    "3v3_pups": 6,
    "4v4_pups": 8
}

# Queue system state
queues: Dict[str, List[int]] = {
    "3v3": [],
    "4v4": [],
    "3v3_pups": [],
    "4v4_pups": []
}

active_games: Dict[int, Dict[str, Any]] = {} # {game_id: {channel_id, voice_channel_id, players, queue_type, status, teams, captains, current_picker, picking_turn, db_game_id}}
game_counter = 1
party_size: Optional[int] = None # None for non-party season, 2, 3, or 4 for party size
queue_status = True # True if queues are open, False if closed
active_queues = ["3v3", "4v4"] # Queues active for the current season

# Store active polls and strike requests for button interactions
active_polls: Dict[int, Any] = {} # {poll_message_id: PollView_instance}
active_strike_requests: Dict[int, Any] = {} # {poll_message_id: StrikeRequestView_instance}
active_screenshare_tickets: Dict[int, Any] = {} # {ticket_channel_id: ScreenshareView_instance}

# Global database connection pool
db_pool: Optional[aiomysql.Pool] = None

# --- Database Connection Pool Setup ---
async def setup_db_pool():
    """Initializes the aiomysql connection pool."""
    global db_pool
    try:
        db_pool = await aiomysql.create_pool(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            autocommit=True, # Auto-commit for simpler operations, can be set to False for explicit transactions
            minsize=1,
            maxsize=10,
            loop=bot.loop # Use the bot's event loop
        )
        print("MariaDB connection pool created successfully.")
    except aiomysql.Error as e:
        print(f"Error creating MariaDB connection pool: {e}")
        # Consider exiting or retrying if database connection is critical

# --- Bot Setup ---
intents = discord.Intents.all()
bot = commands.Bot(command_prefix='=', intents=intents)

# --- Helper Functions ---
def create_embed(title: str, description: str, color: discord.Color, fields: Optional[List[Dict[str, Any]]] = None):
    """Creates a standardized Discord embed."""
    embed = discord.Embed(
        title=title,
        description=description,
        color=color
    )
    if fields:
        for field in fields:
            embed.add_field(name=field['name'], value=field['value'], inline=field.get('inline', False))
    embed.set_footer(text="asrbw.fun") # Changed footer
    return embed

async def get_channel_or_create_category(guild: discord.Guild, id: Optional[int], name: str, is_category: bool = False):
    """
    Attempts to get a channel/category by ID, then by name. If not found and is_category is True, creates it.
    """
    target = None
    if id:
        target = guild.get_channel(id)
    if not target:
        if is_category:
            target = discord.utils.get(guild.categories, name=name)
            if not target:
                target = await guild.create_category(name)
                print(f"Created new '{name}' category with ID: {target.id}")
            else:
                print(f"Found existing '{name}' category with ID: {target.id}")
        else:
            target = discord.utils.get(guild.text_channels, name=name)
            if not target:
                target = discord.utils.get(guild.voice_channels, name=name)
    return target

def get_role_by_name(guild: discord.Guild, role_name: str) -> Optional[discord.Role]:
    """Retrieves a role object by its name."""
    return discord.utils.get(guild.roles, name=role_name)

async def get_channel_by_config(guild: discord.Guild, channel_id: Optional[int], channel_name: str):
    """Retrieves a channel using ID first, then name."""
    if channel_id:
        channel = guild.get_channel(channel_id)
        if channel:
            return channel
    return discord.utils.get(guild.channels, name=channel_name)

async def send_log_embed(guild: discord.Guild, channel_id: Optional[int], channel_name: str, embed: discord.Embed):
    """Sends a Discord embed to a specified log channel."""
    log_channel = await get_channel_by_config(guild, channel_id, channel_name)
    if not log_channel or not isinstance(log_channel, discord.TextChannel):
        print(f"Warning: Log channel '{channel_name}' (ID: {channel_id}) not found or is not a text channel.")
        return
    try:
        await log_channel.send(embed=embed)
    except discord.Forbidden:
        print(f"Bot lacks permissions to send embeds in {log_channel.name}.")
    except Exception as e:
        print(f"Error sending embed log to {log_channel.name}: {e}")


# --- ELO and Role Management ---
async def get_player_elo(player_id: int) -> int:
    """Retrieves a player's ELO from the database."""
    if db_pool is None: return 0
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT elo FROM users WHERE discord_id = %s",
                    (player_id,)
                )
                result = await cursor.fetchone()
                return result[0] if result else 0
            except aiomysql.Error as e:
                print(f"Error getting player ELO: {e}")
                return 0

async def update_player_elo_in_db(player_id: int, elo_change: int) -> bool:
    """Updates a player's ELO in the database."""
    if db_pool is None: return False
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "UPDATE users SET elo = elo + %s WHERE discord_id = %s",
                    (elo_change, player_id)
                )
                await conn.commit()
                return True
            except aiomysql.Error as e:
                print(f"Error updating player ELO in DB: {e}")
                return False

async def update_streak(player_id: int, won: bool):
    """Updates a player's win/loss streak."""
    if db_pool is None: return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT streak FROM users WHERE discord_id = %s",
                    (player_id,)
                )
                current_streak = (await cursor.fetchone())[0] if await cursor.rowcount > 0 else 0

                if won:
                    new_streak = current_streak + 1
                else:
                    new_streak = 0 # Reset streak on loss

                await cursor.execute(
                    "UPDATE users SET streak = %s WHERE discord_id = %s",
                    (new_streak, player_id)
                )
                await conn.commit()
            except aiomysql.Error as e:
                print(f"Error updating streak: {e}")

async def get_elo_role_name(elo: int) -> str:
    """Determines the ELO role name based on ELO value."""
    for role, (min_elo, max_elo) in ELO_ROLES.items():
        if min_elo <= elo < max_elo:
            return role
    return "Iron" # Default for 0 ELO

async def update_elo_role(player_id: int, new_elo: int):
    """Updates a player's ELO role and nickname on Discord."""
    guild = bot.guilds[0] # Assuming bot operates in a single guild
    member = guild.get_member(player_id)
    
    if not member:
        return
    
    new_role_name = await get_elo_role_name(new_elo)
    
    # Remove all existing ELO roles
    for role_name in ELO_ROLES.keys():
        role = get_role_by_name(guild, role_name)
        if role and role in member.roles:
            try:
                await member.remove_roles(role)
            except discord.Forbidden:
                print(f"Bot lacks permissions to remove role {role.name} from {member.display_name}")
            except Exception as e:
                print(f"Error removing role {role.name}: {e}")
    
    # Add the new ELO role
    new_role = get_role_by_name(guild, new_role_name)
    if new_role:
        if new_role not in member.roles:
            try:
                await member.add_roles(new_role)
            except discord.Forbidden:
                print(f"Bot lacks permissions to add role {new_role.name} to {member.display_name}")
            except Exception as e:
                print(f"Error adding role {new_role.name}: {e}")
    
    # Update nickname
    if db_pool is None: return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT minecraft_ign FROM users WHERE discord_id = %s",
                    (player_id,)
                )
                ign = await cursor.fetchone()
                if ign:
                    ign = ign[0]
                    try:
                        await member.edit(nick=f"[{new_elo}] {ign}")
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to change nickname for {member.display_name}")
                    except Exception as e:
                        print(f"Error changing nickname: {e}")
            except aiomysql.Error as e:
                print(f"Error getting IGN for nickname update: {e}")

async def get_user_ign(discord_id: int) -> Optional[str]:
    """Fetches a user's Minecraft IGN from the database."""
    if db_pool is None: return None
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute("SELECT minecraft_ign FROM users WHERE discord_id = %s", (discord_id,))
                result = await cursor.fetchone()
                return result[0] if result else None
            except aiomysql.Error as e:
                print(f"Error fetching IGN: {e}")
                return None

async def is_registered(discord_id: int) -> bool:
    """Checks if a Discord user is registered in the database."""
    if db_pool is None: return False
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT verified FROM users WHERE discord_id = %s",
                    (discord_id,)
                )
                result = await cursor.fetchone()
                return result[0] if result else False
            except aiomysql.Error as e:
                print(f"Error checking registration: {e}")
                return False

async def is_banned(discord_id: int) -> bool:
    """Checks if a Discord user is currently banned."""
    if db_pool is None: return False
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT 1 FROM bans WHERE discord_id = %s AND active = TRUE AND (expires_at IS NULL OR expires_at > NOW())",
                    (discord_id,)
                )
                return await cursor.fetchone() is not None
            except aiomysql.Error as e:
                print(f"Error checking ban: {e}")
                return False

# --- Game Management Functions ---
async def get_game_data_from_db(game_id: int) -> Optional[Dict[str, Any]]:
    """Fetches comprehensive game data from the database."""
    if db_pool is None: return None
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor: # Use DictCursor for dictionary results
            try:
                await cursor.execute(
                    """
                    SELECT g.*, 
                           GROUP_CONCAT(CASE WHEN gp.team = 1 THEN gp.discord_id ELSE NULL END) AS team1_players,
                           GROUP_CONCAT(CASE WHEN gp.team = 2 THEN gp.discord_id ELSE NULL END) AS team2_players
                    FROM games g
                    LEFT JOIN game_players gp ON g.game_id = gp.game_id
                    WHERE g.game_id = %s
                    GROUP BY g.game_id
                    """,
                    (game_id,)
                )
                game_data = await cursor.fetchone()
                if game_data:
                    # Convert comma-separated strings to lists of integers
                    game_data['team1_players'] = [int(p) for p in game_data['team1_players'].split(',')] if game_data['team1_players'] else []
                    game_data['team2_players'] = [int(p) for p in game_data['team2_players'].split(',')] if game_data['team2_players'] else []
                return game_data
            except aiomysql.Error as e:
                print(f"Error fetching game data from DB: {e}")
                return None

async def cleanup_game(game_id: int):
    """Deletes game-related channels and removes game from active_games."""
    if game_id not in active_games:
        return
    
    game_data = active_games[game_id]
    
    # Delete text channel
    try:
        channel = bot.get_channel(game_data["channel_id"])
        if channel and isinstance(channel, discord.TextChannel):
            await channel.delete(reason=f"Game #{game_id} concluded.")
    except discord.NotFound:
        print(f"Text channel for game {game_id} already deleted.")
    except discord.Forbidden:
        print(f"Bot lacks permissions to delete text channel for game {game_id}.")
    except Exception as e:
        print(f"Error deleting text channel for game {game_id}: {e}")
    
    # Delete voice channel
    try:
        voice_channel = bot.get_channel(game_data["voice_channel_id"])
        if voice_channel and isinstance(voice_channel, discord.VoiceChannel):
            await voice_channel.delete(reason=f"Game #{game_id} concluded.")
    except discord.NotFound:
        print(f"Voice channel for game {game_id} already deleted.")
    except discord.Forbidden:
        print(f"Bot lacks permissions to delete voice channel for game {game_id}.")
    except Exception as e:
        print(f"Error deleting voice channel for game {game_id}: {e}")
    
    del active_games[game_id]

async def generate_game_results_image(game_data: Dict[str, Any], winning_team: int, mvp_player_id: int):
    """Generates a monochrome image displaying game results."""
    img_width, img_height = 800, 400
    bg_color = (30, 30, 30) # Dark grey
    text_color = (220, 220, 220) # Light grey
    accent_color = (150, 150, 150) # Medium grey for highlights

    img = Image.new('RGB', (img_width, img_height), color=bg_color)
    draw = ImageDraw.Draw(img)

    try:
        # Attempt to load a common font, or fall back to default
        font_large = ImageFont.truetype("arial.ttf", 30)
        font_medium = ImageFont.truetype("arial.ttf", 22)
        font_small = ImageFont.truetype("arial.ttf", 18)
    except IOError:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()

    # Title
    title_text = f"Game #{game_data['db_game_id']:04d} Results"
    title_bbox = draw.textbbox((0,0), title_text, font=font_large)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(((img_width - title_width) // 2, 30), title_text, font=font_large, fill=text_color)

    # Team 1
    team1_players = []
    for p_id in game_data["teams"][1]:
        user = bot.get_user(p_id) or await bot.fetch_user(p_id)
        team1_players.append(user.display_name if user else f"Unknown User ({p_id})")
    
    draw.text((50, 100), "Team 1", font=font_medium, fill=accent_color)
    y_offset = 140
    for player_name in team1_players:
        draw.text((50, y_offset), player_name, font=font_small, fill=text_color)
        y_offset += 25

    # Team 2
    team2_players = []
    for p_id in game_data["teams"][2]:
        user = bot.get_user(p_id) or await bot.fetch_user(p_id)
        team2_players.append(user.display_name if user else f"Unknown User ({p_id})")

    team2_title_width = draw.textlength("Team 2", font=font_medium)
    draw.text((img_width - 50 - team2_title_width, 100), "Team 2", font=font_medium, fill=accent_color)
    y_offset = 140
    for player_name in team2_players:
        player_name_width = draw.textlength(player_name, font=font_small)
        draw.text((img_width - 50 - player_name_width, y_offset), player_name, font=font_small, fill=text_color)
        y_offset += 25

    # Winning Team Indicator (Crown)
    crown_emoji = "👑" 
    winning_team_text = f"Team {winning_team} Wins! {crown_emoji}"
    winning_team_bbox = draw.textbbox((0,0), winning_team_text, font=font_medium)
    winning_team_width = winning_team_bbox[2] - winning_team_bbox[0]
    draw.text(((img_width - winning_team_width) // 2, img_height - 80), winning_team_text, font=font_medium, fill=accent_color)

    # MVP Indicator (Crown next to name)
    mvp_user = bot.get_user(mvp_player_id) or await bot.fetch_user(mvp_player_id)
    mvp_name = mvp_user.display_name if mvp_user else f"Unknown User ({mvp_player_id})"
    mvp_text = f"{mvp_name} {crown_emoji} (MVP)"
    mvp_bbox = draw.textbbox((0,0), mvp_text, font=font_medium)
    mvp_width = mvp_bbox[2] - mvp_bbox[0]
    draw.text(((img_width - mvp_width) // 2, img_height - 40), mvp_text, font=font_medium, fill=text_color)

    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    
    return discord.File(img_bytes, filename=f"game_{game_data['db_game_id']}_results.png")

async def generate_player_info_image(ign: str, elo: int, wins: int, losses: int, wlr: float, mvps: int, streak: int):
    """Generates a monochrome image displaying player stats and Minecraft skin."""
    img_width, img_height = 600, 300
    bg_color = (30, 30, 30) # Dark grey
    text_color = (220, 220, 220) # Light grey
    accent_color = (150, 150, 150) # Medium grey for highlights

    img = Image.new('RGB', (img_width, img_height), color=bg_color)
    draw = ImageDraw.Draw(img)
    
    try:
        font_large = ImageFont.truetype("arial.ttf", 28)
        font_medium = ImageFont.truetype("arial.ttf", 20)
        font_small = ImageFont.truetype("arial.ttf", 16)
    except IOError:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
    
    # Get Minecraft skin (head)
    skin_img = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://mc-heads.net/avatar/{ign}/100.png") as resp:
                if resp.status == 200:
                    skin_data = io.BytesIO(await resp.read())
                    skin_img = Image.open(skin_data).convert("RGBA")
                    # Make skin monochrome
                    skin_img = skin_img.convert("L").convert("RGBA") # Convert to grayscale, then back to RGBA for alpha
                    img.paste(skin_img, (30, 30), skin_img)
    except Exception as e:
        print(f"Could not fetch or process skin for {ign}: {e}")
        # Draw a placeholder if skin fails
        draw.rectangle((30, 30, 130, 130), fill=accent_color, outline=text_color)
        draw.text((40, 65), "No Skin", font=font_small, fill=text_color)
    
    # Draw player info
    draw.text((160, 40), f"{ign}", font=font_large, fill=text_color)
    draw.text((160, 80), f"ELO: {elo}", font=font_medium, fill=accent_color)
    
    # Draw stats
    stats_x_start = 160
    stats_y_start = 130
    line_height = 30
    
    draw.text((stats_x_start, stats_y_start), f"Wins: {wins}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + line_height), f"Losses: {losses}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + 2 * line_height), f"W/L Ratio: {wlr:.2f}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + 3 * line_height), f"MVPs: {mvps}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + 4 * line_height), f"Streak: {streak}", font=font_medium, fill=text_color)
    
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    
    return discord.File(img_bytes, filename="player_stats.png")


# --- Discord.py Events ---
@bot.event
async def on_ready():
    """Executes when the bot successfully connects to Discord."""
    print(f'Logged in as {bot.user.name}')
    await setup_db_pool() # Initialize database connection pool
    # Start background tasks
    check_queues.start()
    check_expired_punishments.start()
    check_elo_decay.start()
    sync_db.start()
    check_afk_players.start()
    # Sync slash commands (if any are defined using @bot.tree.command)
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print(f"Error syncing commands: {e}")

    # Set bot status
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name="asrbw.fun"))


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """Monitors role changes for staff promotions/demotions."""
    staff_roles_config = {
        MODERATOR_ROLE_NAME: 1,
        ADMIN_STAFF_ROLE_NAME: 2,
        MANAGER_ROLE_NAME: 3,
        PI_ROLE_NAME: 4
    }
    
    before_level = 0
    after_level = 0
    
    # Determine the highest staff level before and after the update
    for role_name, level in staff_roles_config.items():
        role_obj = get_role_by_name(after.guild, role_name)
        if role_obj:
            if role_obj in after.roles:
                after_level = max(after_level, level)
            if role_obj in before.roles:
                before_level = max(before_level, level)
            
    staff_updates_channel = await get_channel_by_config(after.guild, STAFF_UPDATES_CHANNEL_ID, STAFF_UPDATES_CHANNEL_NAME)

    if before_level < after_level:
        # Promotion
        embed = create_embed(
            title="Staff Update: Promotion!",
            description=f"{after.mention} has been promoted!",
            color=discord.Color.green(),
            fields=[
                {"name": "Old Rank Level", "value": f"{before_level}", "inline": True},
                {"name": "New Rank Level", "value": f"{after_level}", "inline": True}
            ]
        )
        if staff_updates_channel and isinstance(staff_updates_channel, discord.TextChannel):
            await staff_updates_channel.send(embed=embed)
    elif before_level > after_level:
        # Demotion
        embed = create_embed(
            title="Staff Update: Demotion!",
            description=f"{after.mention} has been demoted!",
            color=discord.Color.red(),
            fields=[
                {"name": "Old Rank Level", "value": f"{before_level}", "inline": True},
                {"name": "New Rank Level", "value": f"{after_level}", "inline": True}
            ]
        )
        if staff_updates_channel and isinstance(staff_updates_channel, discord.TextChannel):
            await staff_updates_channel.send(embed=embed)

@bot.event
async def on_message(message: discord.Message):
    """Handles messages for game picking phase."""
    if message.author == bot.user:
        return
    
    # Check if message is in a game channel for picking
    for game_id, game_data in active_games.items():
        if message.channel.id == game_data["channel_id"] and game_data["status"] == "picking":
            await handle_pick(message, game_id, game_data)
            break # Only handle one game per message
    
    await bot.process_commands(message)

# --- Background Tasks ---
@tasks.loop(minutes=5)
async def sync_db():
    """Periodically checks and ensures database connection pool is active."""
    if db_pool:
        # Attempt to acquire and release a connection to ensure the pool is healthy
        try:
            async with db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1") # Simple query to keep connection alive
                # print("Database connection pool health check passed.")
        except aiomysql.Error as e:
            print(f"Database connection pool health check failed: {e}")
    else:
        print("Database pool not initialized.")

@tasks.loop(seconds=10)
async def check_queues():
    """Checks active queues and starts games when enough players are present."""
    global game_counter
    
    if not queue_status:
        return
    
    for queue_type in active_queues:
        required_players = QUEUE_TYPES[queue_type]
        
        if len(queues[queue_type]) >= required_players:
            players_in_queue = queues[queue_type][:required_players]
            
            guild = bot.guilds[0]

            # Determine category for game and voice channels
            game_category = await get_channel_or_create_category(guild, GAME_CATEGORY_ID, "Games", is_category=True)
            voice_category = await get_channel_or_create_category(guild, VOICE_CATEGORY_ID, "Voice Channels", is_category=True)

            if not game_category or not voice_category:
                print(f"Error: Could not find or create game/voice categories for queue type {queue_type}. Skipping game creation.")
                continue

            # Create text channel
            game_channel = await guild.create_text_channel(
                f"game-{game_counter:04d}", # Use lowercase and hyphens for channel names
                category=game_category,
                topic=f"Discussion and commands for Game #{game_counter:04d} ({queue_type})"
            )
            
            # Create voice channel
            voice_channel = await guild.create_voice_channel(
                f"Game #{game_counter:04d}",
                category=voice_category
            )
            
            teams: Dict[int, List[int]] = {1: [], 2: []}
            captains: List[int] = []
            description: str = ""
            color: discord.Color = discord.Color.blue()
            
            # --- Party Season Logic vs. Captain Picking ---
            if party_size is not None:
                # Fair ELO matchmaking for party season
                players_with_elo = []
                for p_id in players_in_queue:
                    elo = await get_player_elo(p_id)
                    players_with_elo.append({"id": p_id, "elo": elo})
                
                players_with_elo.sort(key=lambda x: x["elo"]) # Sort by ELO ascending
                
                # Distribute players to balance ELO
                # Simple alternating distribution for fairness
                for i, player in enumerate(players_with_elo):
                    if i % 2 == 0:
                        teams[1].append(player["id"])
                    else:
                        teams[2].append(player["id"])
                
                description = "Teams have been automatically balanced by ELO!"
                color = discord.Color.purple()
            else:
                # Non-Party Season (Captain Picking)
                player_elos = []
                for player_id in players_in_queue:
                    elo = await get_player_elo(player_id)
                    player_elos.append((player_id, elo))
                
                player_elos.sort(key=lambda x: x[1], reverse=True) # Sort by ELO descending
                
                # Select top 2 as captains
                captain1 = player_elos[0][0]
                captain2 = player_elos[1][0]
                
                captains = [captain1, captain2]
                teams[1].append(captain1)
                teams[2].append(captain2)
                
                description = "Captains have been selected by ELO! Time to pick teams."
                color = discord.Color.blu
