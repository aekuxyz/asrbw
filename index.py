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
MANAGER_ROLE_ROLE_NAME = "Manager" # Role for modify stats command (and above) # Renamed to avoid conflict
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
    "Iron": {"win": 25, "loss": -10, "mvp": 20}, # Loss values should be negative for reduction
    "Bronze": {"win": 20, "loss": -10, "mvp": 15},
    "Silver": {"win": 20, "loss": -10, "mvp": 10},
    "Gold": {"win": 15, "loss": -10, "mvp": 10},
    "Topaz": {"win": 10, "loss": -15, "mvp": 10},
    "Platinum": {"win": 5, "loss": -20, "mvp": 10}
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

    # Add .gg/asianrbw
    gg_text = ".gg/asianrbw"
    gg_bbox = draw.textbbox((0, 0), gg_text, font=font_small)
    gg_width = gg_bbox[2] - gg_bbox[0]
    draw.text((img_width - gg_width - 10, img_height - 25), gg_text, font=font_small, fill=text_color)


    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    
    return discord.File(img_bytes, filename=f"game_{game_data['db_game_id']}_results.png")

async def generate_player_info_image(ign: str, elo: int, wins: int, losses: int, wlr: float, mvps: int, streak: int):
    """Generates a monochrome image displaying player stats and Minecraft skin."""
    img_width, img_height = 600, 350 # Increased height for more space
    bg_color = (30, 30, 30) # Dark grey
    text_color = (220, 220, 220) # Light grey
    accent_color = (150, 150, 150) # Medium grey for highlights
    
    img = Image.new('RGB', (img_width, img_height), color=bg_color)
    draw = ImageDraw.Draw(img)
    
    try:
        font_large = ImageFont.truetype("arial.ttf", 32) # Slightly larger font
        font_medium = ImageFont.truetype("arial.ttf", 22)
        font_small = ImageFont.truetype("arial.ttf", 18)
        font_mono = ImageFont.truetype("arial.ttf", 16) # For stats, if a monospace font is preferred
    except IOError:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
        font_mono = ImageFont.load_default()
    
    # Get Minecraft skin (full body, if possible, otherwise head)
    # Using a 3D avatar for better visual
    skin_img = None
    try:
        async with aiohttp.ClientSession() as session:
            # Attempt to get a 3D render
            async with session.get(f"https://mc-heads.net/body/{ign}/150.png") as resp:
                if resp.status == 200:
                    skin_data = io.BytesIO(await resp.read())
                    skin_img = Image.open(skin_data).convert("RGBA")
                else: # Fallback to avatar if body fails
                    async with session.get(f"https://mc-heads.net/avatar/{ign}/150.png") as resp_avatar:
                        if resp_avatar.status == 200:
                            skin_data = io.BytesIO(await resp_avatar.read())
                            skin_img = Image.open(skin_data).convert("RGBA")

            if skin_img:
                # Make skin monochrome
                skin_img = skin_img.convert("L").convert("RGBA") # Convert to grayscale, then back to RGBA for alpha
                # Resize for consistency and position
                skin_img = skin_img.resize((150, 250), Image.Resampling.LANCZOS) # Adjust size as needed
                img.paste(skin_img, (30, 40), skin_img) # Adjusted position
    except Exception as e:
        print(f"Could not fetch or process skin for {ign}: {e}")
        # Draw a placeholder if skin fails
        draw.rectangle((30, 40, 180, 290), fill=accent_color, outline=text_color)
        draw.text((60, 150), "No Skin", font=font_small, fill=text_color)
    
    # Draw player info
    draw.text((200, 40), f"{ign}", font=font_large, fill=text_color)
    draw.text((200, 90), f"ELO: {elo}", font=font_medium, fill=accent_color)
    
    # Draw stats with better alignment
    stats_x_start = 200
    stats_y_start = 140
    line_height = 30
    
    draw.text((stats_x_start, stats_y_start), f"Wins: {wins}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + line_height), f"Losses: {losses}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + 2 * line_height), f"W/L Ratio: {wlr:.2f}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + 3 * line_height), f"MVPs: {mvps}", font=font_medium, fill=text_color)
    draw.text((stats_x_start, stats_y_start + 4 * line_height), f"Streak: {streak}", font=font_medium, fill=text_color)

    # Add .gg/asianrbw at the bottom
    gg_text = ".gg/asianrbw"
    gg_bbox = draw.textbbox((0, 0), gg_text, font=font_small)
    gg_width = gg_bbox[2] - gg_bbox[0]
    draw.text((img_width - gg_width - 10, img_height - 25), gg_text, font=font_small, fill=text_color)

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
        MANAGER_ROLE_ROLE_NAME: 3, # Use the renamed variable
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
                color = discord.Color.blue() # Corrected color

            # Add more game-related functions and commands here
            # ... (rest of your check_queues function) ...

# Placeholder for check_expired_punishments, check_elo_decay, check_afk_players
@tasks.loop(hours=1)
async def check_expired_punishments():
    """Checks for and removes expired bans and mutes."""
    if db_pool is None: return
    guild = bot.guilds[0]
    banned_role = get_role_by_name(guild, BANNED_ROLE_NAME)
    muted_role = get_role_by_name(guild, MUTED_ROLE_NAME)

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Check for expired bans
            await cursor.execute(
                "SELECT discord_id, ban_id FROM bans WHERE active = TRUE AND expires_at <= NOW() AND expires_at IS NOT NULL"
            )
            expired_bans = await cursor.fetchall()
            for discord_id, ban_id in expired_bans:
                member = guild.get_member(discord_id)
                if member and banned_role and banned_role in member.roles:
                    try:
                        await member.remove_roles(banned_role, reason="Ban expired")
                        user_embed = create_embed(
                            title="Ban Expired",
                            description=f"Your ban on {guild.name} has expired. You can now participate fully.",
                            color=discord.Color.green()
                        )
                        await member.send(embed=user_embed)
                        log_embed = create_embed(
                            title="Ban Expired (Log)",
                            description=f"Ban for <@{discord_id}> has expired and role removed.",
                            color=discord.Color.green(),
                            fields=[
                                {"name": "User", "value": f"<@{discord_id}> ({discord_id})", "inline": True},
                                {"name": "Ban ID", "value": str(ban_id), "inline": True}
                            ]
                        )
                        await send_log_embed(guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME, log_embed)
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to remove banned role from {member.display_name}")
                await cursor.execute("UPDATE bans SET active = FALSE WHERE ban_id = %s", (ban_id,))
                await conn.commit()

            # Check for expired mutes
            await cursor.execute(
                "SELECT discord_id, mute_id FROM mutes WHERE active = TRUE AND expires_at <= NOW() AND expires_at IS NOT NULL"
            )
            expired_mutes = await cursor.fetchall()
            for discord_id, mute_id in expired_mutes:
                member = guild.get_member(discord_id)
                if member and muted_role and muted_role in member.roles:
                    try:
                        await member.remove_roles(muted_role, reason="Mute expired")
                        user_embed = create_embed(
                            title="Mute Expired",
                            description=f"Your mute on {guild.name} has expired. You can now speak in chat.",
                            color=discord.Color.green()
                        )
                        await member.send(embed=user_embed)
                        log_embed = create_embed(
                            title="Mute Expired (Log)",
                            description=f"Mute for <@{discord_id}> has expired and role removed.",
                            color=discord.Color.green(),
                            fields=[
                                {"name": "User", "value": f"<@{discord_id}> ({discord_id})", "inline": True},
                                {"name": "Mute ID", "value": str(mute_id), "inline": True}
                            ]
                        )
                        await send_log_embed(guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME, log_embed)
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to remove muted role from {member.display_name}")
                await cursor.execute("UPDATE mutes SET active = FALSE WHERE mute_id = %s", (mute_id,))
                await conn.commit()

@tasks.loop(days=7)
async def check_elo_decay():
    """Applies ELO decay to inactive players."""
    if db_pool is None: return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Implement ELO decay logic here based on your rules (e.g., last game played)
            pass

@tasks.loop(minutes=1)
async def check_afk_players():
    """Moves AFK players to the AFK voice channel."""
    guild = bot.guilds[0]
    afk_channel = guild.get_channel(AFK_VOICE_CHANNEL_ID)

    if not afk_channel or not isinstance(afk_channel, discord.VoiceChannel):
        print("AFK voice channel not configured or not found.")
        return

    for member in guild.members:
        if member.voice and member.voice.channel and member.voice.channel.id != afk_channel.id:
            # Check if member is AFK (e.g., deafened, or in an AFK timeout state)
            # Discord's built-in AFK handling is usually sufficient, but if you have custom logic
            # for "AFK" state (e.g., idle for X minutes in a voice channel), implement it here.
            # For simplicity, we'll just check if they are self-deafened.
            if member.voice.self_deaf or member.voice.afk: # afk property usually means in server's AFK channel
                try:
                    await member.move_to(afk_channel)
                    print(f"Moved {member.display_name} to AFK channel.")
                except discord.Forbidden:
                    print(f"Bot lacks permissions to move {member.display_name} to AFK channel.")
                except Exception as e:
                    print(f"Error moving {member.display_name} to AFK channel: {e}")

# --- Views for interactions (StrikeRequestView, ScreenshareView, TicketView, PPPVotingView - placeholders) ---

class StrikeRequestView(discord.ui.View):
    def __init__(self, target_user: discord.Member, reason: str, requestor: discord.Member):
        super().__init__(timeout=60) # Changed timeout to 60 seconds
        self.target_user = target_user
        self.reason = reason
        self.yes_votes = set()
        self.no_votes = set()
        self.requestor = requestor

    async def update_message(self, interaction: discord.Interaction):
        required_votes = 3 # Example: 3 votes needed
        yes_count = len(self.yes_votes)
        no_count = len(self.no_votes)

        embed = create_embed(
            title=f"Strike Request for {self.target_user.display_name}",
            description=f"Reason: {self.reason}\nRequested by: {self.requestor.mention}",
            color=discord.Color.orange(),
            fields=[
                {"name": "👍 Votes", "value": str(yes_count), "inline": True},
                {"name": "👎 Votes", "value": str(no_count), "inline": True},
                {"name": "Status", "value": f"Awaiting {required_votes - yes_count} more positive votes.", "inline": False}
            ]
        )
        await interaction.message.edit(embed=embed, view=self)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.green, emoji="👍")
    async def yes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.yes_votes or interaction.user.id in self.no_votes:
            await interaction.response.send_message("You have already voted.", ephemeral=True)
            return
        
        # Check if user has staff role (Moderator, Admin, Manager, PI)
        staff_roles = [get_role_by_name(interaction.guild, MODERATOR_ROLE_NAME),
                        get_role_by_name(interaction.guild, ADMIN_STAFF_ROLE_NAME),
                        get_role_by_name(interaction.guild, MANAGER_ROLE_ROLE_NAME),
                        get_role_by_name(interaction.guild, PI_ROLE_NAME)]
        if not any(role in interaction.user.roles for role in staff_roles if role):
            await interaction.response.send_message("You need a staff role to vote on strike requests.", ephemeral=True)
            return

        self.yes_votes.add(interaction.user.id)
        await self.update_message(interaction)
        await interaction.response.defer() # Acknowledge the interaction

        required_votes = 3 # Example: 3 votes needed
        if len(self.yes_votes) >= required_votes:
            self.stop() # Stop the view

            # Apply strike logic here
            strike_time = datetime.datetime.now()
            if db_pool is None: return

            async with db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    try:
                        await cursor.execute(
                            "INSERT INTO strikes (discord_id, reason, issued_by, strike_date) VALUES (%s, %s, %s, %s)",
                            (self.target_user.id, self.reason, self.requestor.id, strike_time)
                        )
                        await conn.commit()

                        user_embed = create_embed(
                            title="You have been Stripped of your Dignity (Strike)", # Changed title
                            description=f"You have received a strike on {interaction.guild.name} for: {self.reason}",
                            color=discord.Color.red()
                        )
                        try:
                            await self.target_user.send(embed=user_embed)
                        except discord.Forbidden:
                            print(f"Could not DM {self.target_user.display_name} about strike.")

                        log_embed = create_embed(
                            title="Strike Issued (Log)",
                            description=f"<@{self.target_user.id}> has received a strike.",
                            color=discord.Color.red(),
                            fields=[
                                {"name": "User", "value": f"<@{self.target_user.id}> ({self.target_user.id})", "inline": True},
                                {"name": "Reason", "value": self.reason, "inline": True},
                                {"name": "Issued By", "value": f"<@{self.requestor.id}>", "inline": True}
                            ]
                        )
                        await send_log_embed(interaction.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME, log_embed)
                        
                        await interaction.message.channel.send(f"Strike applied to {self.target_user.mention}.")
                        
                        # Automatically delete the ticket channel
                        if interaction.message.channel:
                            try:
                                await interaction.message.channel.delete(reason="Strike request approved and processed.")
                            except discord.Forbidden:
                                print(f"Bot lacks permissions to delete strike request channel.")
                            except Exception as e:
                                print(f"Error deleting strike request channel: {e}")

                    except aiomysql.Error as e:
                        print(f"Error applying strike to DB: {e}")
                        await interaction.message.channel.send("An error occurred while applying the strike to the database.")

    @discord.ui.button(label="No", style=discord.ButtonStyle.red, emoji="👎")
    async def no_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.yes_votes or interaction.user.id in self.no_votes:
            await interaction.response.send_message("You have already voted.", ephemeral=True)
            return

        # Check if user has staff role
        staff_roles = [get_role_by_name(interaction.guild, MODERATOR_ROLE_NAME),
                        get_role_by_name(interaction.guild, ADMIN_STAFF_ROLE_NAME),
                        get_role_by_name(interaction.guild, MANAGER_ROLE_ROLE_NAME),
                        get_role_by_name(interaction.guild, PI_ROLE_NAME)]
        if not any(role in interaction.user.roles for role in staff_roles if role):
            await interaction.response.send_message("You need a staff role to vote on strike requests.", ephemeral=True)
            return

        self.no_votes.add(interaction.user.id)
        await self.update_message(interaction)
        await interaction.response.defer() # Acknowledge the interaction

    async def on_timeout(self):
        # When timeout occurs, check votes
        required_votes = 3 # Example: 3 votes needed
        yes_count = len(self.yes_votes)
        
        if yes_count < required_votes:
            await self.message.channel.send("Strike request timed out due to insufficient positive votes. Ticket will now be deleted.")
            # Delete the ticket channel if it times out without enough votes
            if self.message.channel:
                try:
                    await self.message.channel.delete(reason="Strike request timed out.")
                except discord.Forbidden:
                    print(f"Bot lacks permissions to delete strike request channel on timeout.")
                except Exception as e:
                    print(f"Error deleting strike request channel on timeout: {e}")
        else:
            # If for some reason it timed out but had enough votes (should be handled by stop() in button callback)
            pass

class ScreenshareView(discord.ui.View):
    def __init__(self, target_user: discord.Member):
        super().__init__(timeout=600)  # 10 minutes timeout
        self.target_user = target_user
        self.claimed_by: Optional[discord.Member] = None

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.primary, emoji="✋")
    async def claim_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = get_role_by_name(interaction.guild, SCREENSHARING_TEAM_ROLE_NAME)
        if not staff_role or staff_role not in interaction.user.roles:
            await interaction.response.send_message("You must be a Screensharing team member to claim this ticket.", ephemeral=True)
            return

        if self.claimed_by is not None:
            await interaction.response.send_message(f"This ticket has already been claimed by {self.claimed_by.mention}.", ephemeral=True)
            return

        self.claimed_by = interaction.user
        button.label = f"Claimed by {self.claimed_by.display_name}"
        button.style = discord.ButtonStyle.green
        button.disabled = True
        
        # Add a "Close" button when claimed
        self.add_item(discord.ui.Button(label="Close Screenshare", style=discord.ButtonStyle.red, custom_id="close_ss_ticket"))

        await interaction.message.edit(view=self)
        await interaction.response.send_message(f"You have claimed the screenshare ticket for {self.target_user.mention}.", ephemeral=True)

        embed = create_embed(
            title="Screenshare Ticket Claimed",
            description=f"Ticket for {self.target_user.mention} has been claimed by {self.claimed_by.mention}.",
            color=discord.Color.blue()
        )
        await send_log_embed(interaction.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, embed)

    @discord.ui.button(label="Close Screenshare", style=discord.ButtonStyle.red, custom_id="close_ss_ticket", disabled=True)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.claimed_by != interaction.user:
            await interaction.response.send_message("Only the person who claimed this ticket or an Administrator+ can close it.", ephemeral=True)
            return

        self.stop() # Stop the view

        # Remove Frozen role from user
        frozen_role = get_role_by_name(interaction.guild, FROZEN_ROLE_NAME)
        if frozen_role and frozen_role in self.target_user.roles:
            try:
                await self.target_user.remove_roles(frozen_role, reason="Screenshare concluded.")
                print(f"Removed Frozen role from {self.target_user.display_name}.")
            except discord.Forbidden:
                print(f"Bot lacks permissions to remove Frozen role from {self.target_user.display_name}")
            except Exception as e:
                print(f"Error removing Frozen role: {e}")

        embed = create_embed(
            title="Screenshare Ticket Closed",
            description=f"Screenshare ticket for {self.target_user.mention} has been closed by {interaction.user.mention}.",
            color=discord.Color.green()
        )
        await send_log_embed(interaction.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, embed)
        
        await interaction.response.send_message(f"Screenshare ticket for {self.target_user.mention} has been closed.")
        
        # Delete the ticket channel
        if interaction.channel:
            try:
                await interaction.channel.delete(reason="Screenshare ticket closed.")
            except discord.Forbidden:
                print(f"Bot lacks permissions to delete screenshare ticket channel.")
            except Exception as e:
                print(f"Error deleting screenshare ticket channel: {e}")

    async def on_timeout(self):
        # Remove Frozen role from user if ticket times out
        frozen_role = get_role_by_name(self.target_user.guild, FROZEN_ROLE_NAME)
        if frozen_role and frozen_role in self.target_user.roles:
            try:
                await self.target_user.remove_roles(frozen_role, reason="Screenshare ticket timed out.")
                print(f"Removed Frozen role from {self.target_user.display_name} due to timeout.")
            except discord.Forbidden:
                print(f"Bot lacks permissions to remove Frozen role from {self.target_user.display_name}")
            except Exception as e:
                print(f"Error removing Frozen role on timeout: {e}")

        # Delete the ticket channel
        if self.message.channel:
            try:
                await self.message.channel.delete(reason="Screenshare ticket timed out.")
            except discord.Forbidden:
                print(f"Bot lacks permissions to delete screenshare ticket channel on timeout.")
            except Exception as e:
                print(f"Error deleting screenshare ticket channel on timeout: {e}")
        
        log_embed = create_embed(
            title="Screenshare Ticket Timed Out",
            description=f"Screenshare ticket for {self.target_user.mention} timed out and was automatically closed.",
            color=discord.Color.red()
        )
        await send_log_embed(self.target_user.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, log_embed)


class TicketView(discord.ui.View):
    def __init__(self, owner: discord.Member):
        super().__init__(timeout=None)
        self.owner = owner

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.red, emoji="🔒")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only owner or staff can close
        is_staff = any(role in interaction.user.roles for role in [
            get_role_by_name(interaction.guild, MODERATOR_ROLE_NAME),
            get_role_by_name(interaction.guild, ADMIN_STAFF_ROLE_NAME),
            get_role_by_name(interaction.guild, MANAGER_ROLE_ROLE_NAME),
            get_role_by_name(interaction.guild, PI_ROLE_NAME),
            get_role_by_name(interaction.guild, STAFF_ROLE_NAME)
        ] if role)

        if interaction.user != self.owner and not is_staff:
            await interaction.response.send_message("You are not authorized to close this ticket.", ephemeral=True)
            return

        channel = interaction.channel
        ticket_log_channel = await get_channel_by_config(interaction.guild, TICKET_LOG_CHANNEL_ID, TICKET_LOG_CHANNEL_NAME)
        closed_tickets_category = await get_channel_or_create_category(interaction.guild, CLOSED_TICKETS_CATEGORY_ID, "Closed Tickets", is_category=True)

        if not closed_tickets_category:
            await interaction.response.send_message("Could not find/create the 'Closed Tickets' category. Please contact an administrator.", ephemeral=True)
            return

        await interaction.response.send_message("Closing ticket...")

        # Change permissions to private
        await channel.set_permissions(interaction.guild.default_role, read_messages=False)
        await channel.set_permissions(self.owner, read_messages=True, send_messages=False) # Owner can read but not send
        
        # Move to closed category
        await channel.edit(category=closed_tickets_category, name=f"closed-{channel.name}")

        embed = create_embed(
            title="Ticket Closed",
            description=f"This ticket has been closed by {interaction.user.mention}.",
            color=discord.Color.red()
        )
        await channel.send(embed=embed)

        log_embed = create_embed(
            title="Ticket Closed (Log)",
            description=f"Ticket {channel.name} owned by {self.owner.mention} was closed by {interaction.user.mention}.",
            color=discord.Color.red(),
            fields=[
                {"name": "Ticket Owner", "value": f"<@{self.owner.id}> ({self.owner.id})", "inline": True},
                {"name": "Closed By", "value": f"<@{interaction.user.id}> ({interaction.user.id})", "inline": True},
                {"name": "Channel", "value": channel.mention, "inline": True}
            ]
        )
        if ticket_log_channel:
            await ticket_log_channel.send(embed=log_embed)

    @discord.ui.button(label="Claim Ticket", style=discord.ButtonStyle.blurple, emoji="🙋")
    async def claim_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = get_role_by_name(interaction.guild, STAFF_ROLE_NAME)
        if not staff_role or staff_role not in interaction.user.roles:
            await interaction.response.send_message("You must be a Staff member to claim this ticket.", ephemeral=True)
            return

        # Ensure only one staff can claim
        # You might need a more robust way to track claimed tickets if multiple staff can claim same ticket
        # For now, we'll assume claiming means setting permissions for the claiming staff
        
        # Remove default read for staff roles, and explicitly give to claiming staff
        for role_name in [MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME, STAFF_ROLE_NAME, SCREENSHARING_TEAM_ROLE_NAME]:
            role = get_role_by_name(interaction.guild, role_name)
            if role:
                await interaction.channel.set_permissions(role, overwrite=None) # Reset existing staff permissions

        await interaction.channel.set_permissions(interaction.user, read_messages=True, send_messages=True, manage_channels=True)
        await interaction.channel.set_permissions(self.owner, read_messages=True, send_messages=True)
        await interaction.channel.set_permissions(interaction.guild.default_role, read_messages=False)

        await interaction.response.send_message(f"You have claimed this ticket. {self.owner.mention} can now see and reply.", ephemeral=True)

        embed = create_embed(
            title="Ticket Claimed",
            description=f"This ticket has been claimed by {interaction.user.mention}. They will assist you shortly.",
            color=discord.Color.blue()
        )
        await interaction.channel.send(embed=embed)


# Placeholder for PPPVotingView (no changes requested, keeping for context)
class PPPVotingView(discord.ui.View):
    def __init__(self, poll_id: int):
        super().__init__(timeout=None)
        self.poll_id = poll_id
        self.votes_yes = set()
        self.votes_no = set()

    async def update_poll_message(self, interaction: discord.Interaction):
        # This would fetch the current state of the poll from a database
        # and update the message. For this example, we'll just show current in-memory votes.
        yes_count = len(self.votes_yes)
        no_count = len(self.votes_no)
        
        embed = interaction.message.embeds[0]
        embed.set_field_at(0, name="Yes Votes", value=str(yes_count), inline=True)
        embed.set_field_at(1, name="No Votes", value=str(no_count), inline=True)
        await interaction.message.edit(embed=embed, view=self)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.green)
    async def yes_vote(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.votes_yes or interaction.user.id in self.votes_no:
            await interaction.response.send_message("You have already voted.", ephemeral=True)
            return

        # Check if user has PPP Manager role
        ppp_manager_role = get_role_by_name(interaction.guild, PPP_MANAGER_ROLE_NAME)
        if not ppp_manager_role or ppp_manager_role not in interaction.user.roles:
            await interaction.response.send_message("You need the P.P.P. Manager role to vote on this poll.", ephemeral=True)
            return

        self.votes_yes.add(interaction.user.id)
        await self.update_poll_message(interaction)
        await interaction.response.defer()

    @discord.ui.button(label="No", style=discord.ButtonStyle.red)
    async def no_vote(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.votes_yes or interaction.user.id in self.votes_no:
            await interaction.response.send_message("You have already voted.", ephemeral=True)
            return
        
        # Check if user has PPP Manager role
        ppp_manager_role = get_role_by_name(interaction.guild, PPP_MANAGER_ROLE_NAME)
        if not ppp_manager_role or ppp_manager_role not in interaction.user.roles:
            await interaction.response.send_message("You need the P.P.P. Manager role to vote on this poll.", ephemeral=True)
            return

        self.votes_no.add(interaction.user.id)
        await self.update_poll_message(interaction)
        await interaction.response.defer()

# --- Discord Commands ---

# ELO related commands
@bot.command(name="wins")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def add_win(ctx: commands.Context, member: discord.Member, mvp: Optional[bool] = False):
    await ctx.message.add_reaction("✅") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute("SELECT wins, losses, elo FROM users WHERE discord_id = %s", (member.id,))
                result = await cursor.fetchone()
                if not result:
                    await ctx.send(f"{member.display_name} is not registered.")
                    return

                current_wins, current_losses, current_elo = result
                elo_change = ADMIN_WIN_ELO_CHANGE
                if mvp:
                    elo_change += ADMIN_MVP_ELO_CHANGE

                new_elo = current_elo + elo_change
                new_wins = current_wins + 1

                await cursor.execute(
                    "UPDATE users SET wins = %s, elo = %s WHERE discord_id = %s",
                    (new_wins, new_elo, member.id)
                )
                await conn.commit()

                await update_streak(member.id, True) # Update streak
                await update_elo_role(member.id, new_elo) # Update ELO role and nickname

                await ctx.send(f"Added a win for {member.mention}. New ELO: {new_elo} (change: +{elo_change}). Wins: {new_wins}")
            except aiomysql.Error as e:
                await ctx.send(f"An error occurred: {e}")

@bot.command(name="loss")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def add_loss(ctx: commands.Context, member: discord.Member):
    await ctx.message.add_reaction("✅") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute("SELECT wins, losses, elo FROM users WHERE discord_id = %s", (member.id,))
                result = await cursor.fetchone()
                if not result:
                    await ctx.send(f"{member.display_name} is not registered.")
                    return

                current_wins, current_losses, current_elo = result
                elo_change = ADMIN_LOSS_ELO_CHANGE # This is already negative
                
                new_elo = current_elo + elo_change
                new_losses = current_losses + 1

                await cursor.execute(
                    "UPDATE users SET losses = %s, elo = %s WHERE discord_id = %s",
                    (new_losses, new_elo, member.id)
                )
                await conn.commit()

                await update_streak(member.id, False) # Update streak (reset on loss)
                await update_elo_role(member.id, new_elo) # Update ELO role and nickname

                await ctx.send(f"Added a loss for {member.mention}. New ELO: {new_elo} (change: {elo_change}). Losses: {new_losses}")
            except aiomysql.Error as e:
                await ctx.send(f"An error occurred: {e}")

@bot.command(name="info", aliases=["i"])
async def player_info(ctx: commands.Context, member: Optional[discord.Member] = None):
    await ctx.message.add_reaction("✅") # Add reaction
    member = member or ctx.author
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT minecraft_ign, elo, wins, losses, mvps, streak FROM users WHERE discord_id = %s",
                    (member.id,)
                )
                result = await cursor.fetchone()
                if not result:
                    await ctx.send(f"{member.display_name} is not registered. Please register first.")
                    return

                ign, elo, wins, losses, mvps, streak = result
                wlr = wins / losses if losses > 0 else wins # If no losses, W/L is just wins
                
                # Generate the custom player info image
                file = await generate_player_info_image(ign, elo, wins, losses, wlr, mvps, streak)
                
                embed = discord.Embed(
                    title=f"{ign}'s Stats",
                    description=f"Discord: {member.mention}",
                    color=discord.Color.blue()
                )
                embed.set_image(url=f"attachment://player_stats.png")
                embed.set_footer(text=".gg/asianrbw | asrbw.fun") # Updated footer
                
                await ctx.send(file=file, embed=embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while fetching player info: {e}")
            except Exception as e:
                await ctx.send(f"An unexpected error occurred: {e}")


# Strike, Ban, Mute commands - Ensure embeds are sent to logs
@bot.command(name="strike")
@commands.has_any_role(MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def strike(ctx: commands.Context, member: discord.Member, *, reason: str):
    await ctx.message.add_reaction("✅") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    # Send embed to user
    user_embed = create_embed(
        title="You have been Stripped of your Dignity (Strike)",
        description=f"You have received a strike on {ctx.guild.name} for: {reason}",
        color=discord.Color.red()
    )
    try:
        await member.send(embed=user_embed)
    except discord.Forbidden:
        await ctx.send(f"Could not DM {member.display_name} about the strike.")

    # Send embed to log channel
    log_embed = create_embed(
        title="Strike Issued (Log)",
        description=f"<@{member.id}> has received a strike.",
        color=discord.Color.red(),
        fields=[
            {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
            {"name": "Reason", "value": reason, "inline": True},
            {"name": "Issued By", "value": f"<@{ctx.author.id}>", "inline": True}
        ]
    )
    await send_log_embed(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME, log_embed)

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "INSERT INTO strikes (discord_id, reason, issued_by) VALUES (%s, %s, %s)",
                    (member.id, reason, ctx.author.id)
                )
                await conn.commit()
                await ctx.send(f"Successfully issued a strike to {member.mention}.")
            except aiomysql.Error as e:
                await ctx.send(f"An error occurred: {e}")

@bot.command(name="ban")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME) # Only Admin+ can ban
async def ban_user(ctx: commands.Context, member: discord.Member, duration_minutes: Optional[int] = None, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("✅") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    expires_at = None
    duration_text = "indefinitely"
    if duration_minutes:
        expires_at = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
        duration_text = f"for {duration_minutes} minutes"

    # Send embed to user
    user_embed = create_embed(
        title="You have been Banned!",
        description=f"You have been banned from {ctx.guild.name} {duration_text} for: {reason}",
        color=discord.Color.red()
    )
    if expires_at:
        user_embed.add_field(name="Expires At", value=expires_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=False)
    try:
        await member.send(embed=user_embed)
    except discord.Forbidden:
        await ctx.send(f"Could not DM {member.display_name} about the ban.")

    # Send embed to log channel
    log_embed = create_embed(
        title="User Banned (Log)",
        description=f"<@{member.id}> has been banned.",
        color=discord.Color.red(),
        fields=[
            {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
            {"name": "Reason", "value": reason, "inline": True},
            {"name": "Banned By", "value": f"<@{ctx.author.id}>", "inline": True},
            {"name": "Duration", "value": duration_text, "inline": True}
        ]
    )
    if expires_at:
        log_embed.add_field(name="Expires At", value=expires_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=False)
    await send_log_embed(ctx.guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME, log_embed)

    banned_role = get_role_by_name(ctx.guild, BANNED_ROLE_NAME)
    if not banned_role:
        await ctx.send("Banned role not found. Please configure it.")
        return

    try:
        await member.add_roles(banned_role, reason=reason)
        async with db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO bans (discord_id, reason, issued_by, expires_at) VALUES (%s, %s, %s, %s)",
                    (member.id, reason, ctx.author.id, expires_at)
                )
                await conn.commit()
        await ctx.send(f"Successfully banned {member.mention} {duration_text}.")
    except discord.Forbidden:
        await ctx.send("I don't have permissions to assign the 'Banned' role.")
    except aiomysql.Error as e:
        await ctx.send(f"An error occurred while banning: {e}")

@bot.command(name="mute")
@commands.has_any_role(MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def mute_user(ctx: commands.Context, member: discord.Member, duration_minutes: Optional[int] = None, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("✅") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    expires_at = None
    duration_text = "indefinitely"
    if duration_minutes:
        expires_at = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
        duration_text = f"for {duration_minutes} minutes"

    # Send embed to user
    user_embed = create_embed(
        title="You have been Muted!",
        description=f"You have been muted on {ctx.guild.name} {duration_text} for: {reason}",
        color=discord.Color.orange()
    )
    if expires_at:
        user_embed.add_field(name="Expires At", value=expires_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=False)
    try:
        await member.send(embed=user_embed)
    except discord.Forbidden:
        await ctx.send(f"Could not DM {member.display_name} about the mute.")

    # Send embed to log channel
    log_embed = create_embed(
        title="User Muted (Log)",
        description=f"<@{member.id}> has been muted.",
        color=discord.Color.orange(),
        fields=[
            {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
            {"name": "Reason", "value": reason, "inline": True},
            {"name": "Muted By", "value": f"<@{ctx.author.id}>", "inline": True},
            {"name": "Duration", "value": duration_text, "inline": True}
        ]
    )
    if expires_at:
        log_embed.add_field(name="Expires At", value=expires_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=False)
    await send_log_embed(ctx.guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME, log_embed)

    muted_role = get_role_by_name(ctx.guild, MUTED_ROLE_NAME)
    if not muted_role:
        await ctx.send("Muted role not found. Please configure it.")
        return

    try:
        await member.add_roles(muted_role, reason=reason)
        async with db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO mutes (discord_id, reason, issued_by, expires_at) VALUES (%s, %s, %s, %s)",
                    (member.id, reason, ctx.author.id, expires_at)
                )
                await conn.commit()
        await ctx.send(f"Successfully muted {member.mention} {duration_text}.")
    except discord.Forbidden:
        await ctx.send("I don't have permissions to assign the 'Muted' role.")
    except aiomysql.Error as e:
        await ctx.send(f"An error occurred while muting: {e}")

# Admin Commands (Restricted to Admin+)
@bot.command(name="purgechat")
@commands.has_permissions(manage_messages=True)
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def purge_chat(ctx: commands.Context, message_id: Optional[int] = None):
    await ctx.message.add_reaction("✅") # Add reaction
    if message_id:
        try:
            message = await ctx.channel.fetch_message(message_id)
            deleted_count = 0
            async for msg in ctx.channel.history(limit=None, after=message):
                if msg.id != ctx.message.id: # Don't delete the command message itself
                    await msg.delete()
                    deleted_count += 1
            await ctx.send(f"Purged {deleted_count} messages after message ID {message_id}.", delete_after=5)
        except discord.NotFound:
            await ctx.send("Message ID not found in this channel.", delete_after=5)
        except discord.Forbidden:
            await ctx.send("I don't have permissions to delete messages.", delete_after=5)
        except discord.HTTPException as e:
            await ctx.send(f"An error occurred while purging messages: {e}", delete_after=5)
    else:
        # Purge all messages up to the command message
        deleted_count = 0
        try:
            # Fetch messages, filter out the command message itself, and delete
            async for msg in ctx.channel.history(limit=None, before=ctx.message):
                await msg.delete()
                deleted_count += 1
            # Delete the command message itself last
            await ctx.message.delete()
            await ctx.send(f"Purged {deleted_count} messages in this channel.", delete_after=5)
        except discord.Forbidden:
            await ctx.send("I don't have permissions to delete messages.", delete_after=5)
        except discord.HTTPException as e:
            await ctx.send(f"An error occurred while purging messages: {e}", delete_after=5)


@bot.command(name="setqueuestatus")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def set_queue_status(ctx: commands.Context, status: str):
    await ctx.message.add_reaction("✅") # Add reaction
    global queue_status
    if status.lower() == "open":
        queue_status = True
        await ctx.send("Queues are now open.")
    elif status.lower() == "closed":
        queue_status = False
        await ctx.send("Queues are now closed.")
    else:
        await ctx.send("Invalid status. Use 'open' or 'closed'.")

# New command for deleting ticket channels
@bot.command(name="delete")
@commands.has_any_role(MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME) # Only Manager+
async def delete_ticket_channel(ctx: commands.Context, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("✅") # Add reaction
    if "ticket" not in ctx.channel.name and "screenshare" not in ctx.channel.name:
        await ctx.send("This command can only be used in a ticket or screenshare channel.", ephemeral=True)
        return

    try:
        embed = create_embed(
            title="Channel Deletion",
            description=f"This channel is being deleted by {ctx.author.mention} for: {reason}",
            color=discord.Color.dark_red()
        )
        await ctx.send(embed=embed)
        await asyncio.sleep(5) # Give time for the embed to be seen
        await ctx.channel.delete(reason=f"Channel deleted by {ctx.author.name}: {reason}")
    except discord.Forbidden:
        await ctx.send("I don't have permissions to delete this channel.")
    except Exception as e:
        await ctx.send(f"An error occurred while deleting the channel: {e}")

# Screenshare Command
@bot.command(name="ss")
@commands.has_any_role(MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def screenshare_command(ctx: commands.Context, member: discord.Member):
    await ctx.message.add_reaction("✅") # Add reaction
    # Check for attachment
    if not ctx.message.attachments:
        await ctx.send("Please attach an image to initiate a screenshare request.", ephemeral=True)
        return

    # Create ticket channel under the correct category
    ss_category = await get_channel_or_create_category(ctx.guild, TICKET_CATEGORY_ID, "Tickets", is_category=True)
    if not ss_category:
        await ctx.send("Error: Could not find or create a category for screenshare tickets. Please contact an administrator.", ephemeral=True)
        return

    frozen_role = get_role_by_name(ctx.guild, FROZEN_ROLE_NAME)
    if not frozen_role:
        await ctx.send("Error: 'Frozen' role not found. Please configure it.", ephemeral=True)
        return
    
    # Assign Frozen role
    try:
        await member.add_roles(frozen_role, reason="Initiated screenshare.")
    except discord.Forbidden:
        await ctx.send(f"I don't have permissions to assign the '{FROZEN_ROLE_NAME}' role to {member.display_name}.", ephemeral=True)
        return
    except Exception as e:
        await ctx.send(f"An error occurred while assigning the '{FROZEN_ROLE_NAME}' role: {e}", ephemeral=True)
        return

    overwrites = {
        ctx.guild.default_role: discord.PermissionOverwrite(read_messages=False),
        member: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    
    screenshare_role = get_role_by_name(ctx.guild, SCREENSHARING_TEAM_ROLE_NAME)
    if screenshare_role:
        overwrites[screenshare_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)


    ticket_channel = await ctx.guild.create_text_channel(
        f"screenshare-{member.name}",
        category=ss_category,
        overwrites=overwrites,
        topic=f"Screenshare for {member.display_name} initiated by {ctx.author.name}"
    )

    view = ScreenshareView(member)
    embed = create_embed(
        title="Screenshare Request",
        description=f"{member.mention} has been requested for a screenshare by {ctx.author.mention}.",
        color=discord.Color.orange(),
        fields=[
            {"name": "Instructions", "value": "A screensharing team member will claim this ticket shortly. Please be ready to screenshare. Failure to comply may result in a ban.", "inline": False}
        ]
    )
    
    # Forward the attached image to the new ticket channel
    if ctx.message.attachments:
        attached_file = await ctx.message.attachments[0].to_file()
        await ticket_channel.send(file=attached_file)
        embed.set_image(url=f"attachment://{attached_file.filename}") # Set image in embed

    message = await ticket_channel.send(embed=embed, view=view)
    active_screenshare_tickets[ticket_channel.id] = view
    view.message = message # Store message for timeout editing

    await ctx.send(f"Screenshare ticket created: {ticket_channel.mention}", ephemeral=True)

    log_embed = create_embed(
        title="Screenshare Ticket Created (Log)",
        description=f"A screenshare ticket for {member.mention} has been created by {ctx.author.mention}.",
        color=discord.Color.orange(),
        fields=[
            {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
            {"name": "Requested By", "value": f"<@{ctx.author.id}> ({ctx.author.id})", "inline": True},
            {"name": "Ticket Channel", "value": ticket_channel.mention, "inline": True}
        ]
    )
    await send_log_embed(ctx.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, log_embed)


# Slash Commands
@bot.tree.command(name="createticket", description="Create a new support ticket.")
@app_commands.describe(topic="The topic of your ticket.")
async def create_ticket_slash(interaction: discord.Interaction, topic: str):
    await interaction.response.defer(ephemeral=True) # Acknowledge immediately

    guild = interaction.guild
    member = interaction.user

    ticket_category = await get_channel_or_create_category(guild, TICKET_CATEGORY_ID, "Tickets", is_category=True)
    if not ticket_category:
        await interaction.followup.send("Error: Could not find or create a category for tickets. Please contact an administrator.")
        return

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        member: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }

    # Add staff roles to overwrites
    for role_name in [MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME, STAFF_ROLE_NAME]:
        role = get_role_by_name(guild, role_name)
        if role:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

    ticket_channel = await guild.create_text_channel(
        f"ticket-{member.name}",
        category=ticket_category,
        overwrites=overwrites,
        topic=topic
    )

    view = TicketView(member)
    embed = create_embed(
        title=f"New Ticket: {topic}",
        description=f"Welcome {member.mention}! A staff member will be with you shortly. Please explain your issue in detail.",
        color=discord.Color.blue()
    )
    await ticket_channel.send(embed=embed, view=view)
    await interaction.followup.send(f"Your ticket has been created: {ticket_channel.mention}")

    log_embed = create_embed(
        title="Ticket Created (Log)",
        description=f"A new ticket has been created by {member.mention}.",
        color=discord.Color.blue(),
        fields=[
            {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
            {"name": "Topic", "value": topic, "inline": True},
            {"name": "Ticket Channel", "value": ticket_channel.mention, "inline": True}
        ]
    )
    await send_log_embed(guild, TICKET_LOG_CHANNEL_ID, TICKET_LOG_CHANNEL_NAME, log_embed)


@bot.tree.command(name="addusertoticket", description="Add a user to the current ticket.")
@app_commands.describe(user="The user to add.")
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def add_user_to_ticket_slash(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True) # Acknowledge immediately
    
    if "ticket" not in interaction.channel.name and "screenshare" not in interaction.channel.name:
        await interaction.followup.send("This command can only be used in a ticket or screenshare channel.")
        return

    try:
        await interaction.channel.set_permissions(user, read_messages=True, send_messages=True)
        await interaction.followup.send(f"Added {user.mention} to this ticket.")
        await interaction.channel.send(f"{user.mention} has been added to the ticket by {interaction.user.mention}.")
    except discord.Forbidden:
        await interaction.followup.send("I don't have permissions to add users to this channel.")
    except Exception as e:
        await interaction.followup.send(f"An error occurred: {e}")

@bot.tree.command(name="removesticketuser", description="Remove a user from the current ticket.")
@app_commands.describe(user="The user to remove.")
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def remove_user_from_ticket_slash(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True) # Acknowledge immediately

    if "ticket" not in interaction.channel.name and "screenshare" not in interaction.channel.name:
        await interaction.followup.send("This command can only be used in a ticket or screenshare channel.")
        return

    try:
        await interaction.channel.set_permissions(user, read_messages=False, send_messages=False)
        await interaction.followup.send(f"Removed {user.mention} from this ticket.")
        await interaction.channel.send(f"{user.mention} has been removed from the ticket by {interaction.user.mention}.")
    except discord.Forbidden:
        await interaction.followup.send("I don't have permissions to remove users from this channel.")
    except Exception as e:
        await interaction.followup.send(f"An error occurred: {e}")

@bot.tree.command(name="addroletoticket", description="Add a role to the current ticket.")
@app_commands.describe(role="The role to add.")
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def add_role_to_ticket_slash(interaction: discord.Interaction, role: discord.Role):
    await interaction.response.defer(ephemeral=True) # Acknowledge immediately

    if "ticket" not in interaction.channel.name and "screenshare" not in interaction.channel.name:
        await interaction.followup.send("This command can only be used in a ticket or screenshare channel.")
        return

    try:
        await interaction.channel.set_permissions(role, read_messages=True, send_messages=True)
        await interaction.followup.send(f"Added {role.mention} to this ticket.")
        await interaction.channel.send(f"{role.mention} has been added to the ticket by {interaction.user.mention}.")
    except discord.Forbidden:
        await interaction.followup.send("I don't have permissions to add roles to this channel.")
    except Exception as e:
        await interaction.followup.send(f"An error occurred: {e}")

@bot.tree.command(name="removeroleticket", description="Remove a role from the current ticket.")
@app_commands.describe(role="The role to remove.")
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_ROLE_NAME, PI_ROLE_NAME)
async def remove_role_from_ticket_slash(interaction: discord.Interaction, role: discord.Role):
    await interaction.response.defer(ephemeral=True) # Acknowledge immediately

    if "ticket" not in interaction.channel.name and "screenshare" not in interaction.channel.name:
        await interaction.followup.send("This command can only be used in a ticket or screenshare channel.")
        return

    try:
        await interaction.channel.set_permissions(role, overwrite=None) # Reset to default permissions
        await interaction.followup.send(f"Removed {role.mention} from this ticket.")
        await interaction.channel.send(f"{role.mention} has been removed from the ticket by {interaction.user.mention}.")
    except discord.Forbidden:
        await interaction.followup.send("I don't have permissions to remove roles from this channel.")
    except Exception as e:
        await interaction.followup.send(f"An error occurred: {e}")


# Main bot run
if __name__ == "__main__":
    bot.run(DISCORD_BOT_TOKEN)
