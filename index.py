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
import json # Added for HTML logging content

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
REGISTER_CHANNEL_ID = 1376879395574124544 # ID of your registration channel (where =register is used)
REGISTER_LOG_CHANNEL_ID = 123456789012345678 # ID of your registration logs channel
BAN_LOG_CHANNEL_ID = 1377355353678811278 # ID of your ban logs channel
MUTE_LOG_CHANNEL_ID = 1377355376743153775 # ID of your mute logs channel
STRIKE_LOG_CHANNEL_ID = 1377355415284875425 # ID of your strike logs channel
TICKET_CHANNEL_ID = 1377617914177392640 # ID of the channel where users create tickets (for prefix command)
TICKET_LOG_CHANNEL_ID = 1377617800150913126 # ID of your ticket logs channel (HTML logs)
STRIKE_REQUEST_CHANNEL_ID = 1377351296868417647 # ID of the channel where users make strike requests
SCREENSNARE_LOG_CHANNEL_ID = 1377688164923343072 # ID of your screenshare ticket logs channel (HTML logs)
GAME_LOG_CHANNEL_ID = 1377611419234865152 # ID of your game logs channel
PPP_VOTING_CHANNEL_ID = 1378388708205527110 # ID of your #ppp-voting channel
STAFF_UPDATES_CHANNEL_ID = 1377306838793453578 # ID of your staff-updates channel
GAMES_DISPLAY_CHANNEL_ID = 1377353788226011246 # ID of the channel to display game results image
AFK_VOICE_CHANNEL_ID = 1380096256109707275 # ID of your AFK voice channel

# New Voice Channel IDs for queues - YOU MUST UPDATE THESE WITH YOUR ACTUAL VOICE CHANNEL IDs
QUEUE_3V3_VC_ID = 123456789012345678 # Placeholder ID for 3v3 Queue Voice Channel
QUEUE_4V4_VC_ID = 123456789012345679 # Placeholder ID for 4v4 Queue Voice Channel
QUEUE_3V3_PUPS_VC_ID = 123456789012345680 # Placeholder ID for 3v3 Pups Queue Voice Channel
QUEUE_4V4_PUPS_VC_ID = 123456789012345681 # Placeholder ID for 4v4 Pups Queue Voice Channel

# Map queue types to their respective voice channel IDs for easy lookup
QUEUE_VC_MAP = {
    "3v3": QUEUE_3V3_VC_ID,
    "4v4": QUEUE_4V4_VC_ID,
    "3v3_pups": QUEUE_3V3_PUPS_VC_ID, # Corrected typo
    "4v4_pups": QUEUE_4V4_PUPS_VC_ID # Corrected typo
}

# Channel Names (Fallback if IDs are None or channel not found by ID)
REGISTER_CHANNEL_NAME = "register"
REGISTER_LOG_CHANNEL_NAME = "registration-logs"
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
MANAGER_ROLE_NAME = "Manager" # Role for modify stats command (and above) - Standardized name
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

# active_games remains for tracking ongoing games
active_games: Dict[int, Dict[str, Any]] = {} # {game_id: {channel_id, voice_channel_id, players, queue_type, status, teams, captains, current_picker, picking_turn, db_game_id, last_activity_timestamp}}
game_counter = 1
party_size: Optional[int] = None # None for non-party season, 2, 3, or 4 for party size
queue_status = True # True if queues are open, False if closed
active_queues = ["3v3", "4v4"] # Queues active for the current season (these keys must match QUEUE_VC_MAP)

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
intents.members = True # Ensure member intents are enabled for role/nickname management
intents.voice_states = True # Required for checking voice channel activity
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
    embed.set_footer(text="asrbw.fun")
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
        else: # For text/voice channels, search within all channels
            target = discord.utils.get(guild.channels, id=id) # Try by ID first, even if not a category
            if not target:
                target = discord.utils.get(guild.channels, name=name) # Then by name
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

async def send_log_html(guild: discord.Guild, channel_id: Optional[int], channel_name: str, title: str, content_dict: Dict[str, Any]):
    """Sends an HTML formatted message to a specified log channel."""
    log_channel = await get_channel_by_config(guild, channel_id, channel_name)
    if not log_channel or not isinstance(log_channel, discord.TextChannel):
        print(f"Warning: HTML Log channel '{channel_name}' (ID: {channel_id}) not found or is not a text channel.")
        return

    # Basic HTML structure
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{html.escape(title)}</title>
        <style>
            body {{ font-family: sans-serif; background-color: #2c2f33; color: #ffffff; padding: 20px; }}
            .container {{ background-color: #36393f; border-radius: 8px; padding: 20px; margin-bottom: 10px; }}
            h2 {{ color: #7289da; border-bottom: 2px solid #7289da; padding-bottom: 5px; margin-top: 0; }}
            p {{ margin-bottom: 5px; }}
            strong {{ color: #99aab5; }}
            .field {{ margin-top: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>{html.escape(title)}</h2>
    """

    for key, value in content_dict.items():
        html_content += f'<p><strong>{html.escape(key)}:</strong> {html.escape(str(value))}</p>\n'
    
    html_content += """
        </div>
    </body>
    </html>
    """
    
    # Send as a file if content is too long for a single message, or as a code block
    try:
        if len(html_content) > 1900: # Discord message limit is 2000 chars, leave some buffer
            file = discord.File(io.BytesIO(html_content.encode('utf-8')), filename=f"{title.lower().replace(' ', '_')}_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.html")
            await log_channel.send(content=f"Log for '{title}':", file=file)
        else:
            await log_channel.send(f"```html\n{html_content}\n```")
    except discord.Forbidden:
        print(f"Bot lacks permissions to send messages/files in {log_channel.name}.")
    except Exception as e:
        print(f"Error sending HTML log to {log_channel.name}: {e}")

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
                # Ensure user exists before updating, or add if not present
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (player_id, f"User_{player_id}", 0, 0, 0, 0, 0, 0) # Default IGN if not already set
                )
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
                # Ensure user exists before updating
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (player_id, f"User_{player_id}", 0, 0, 0, 0, 0, 0)
                )
                
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

async def cleanup_game_channels(game_channel_id: int, game_voice_channel_id: int, reason: str):
    """Deletes game-related channels."""
    guild = bot.guilds[0] # Assuming bot operates in a single guild

    # Delete text channel
    try:
        channel = guild.get_channel(game_channel_id)
        if channel and isinstance(channel, discord.TextChannel):
            await channel.delete(reason=reason)
            print(f"Deleted text channel {channel.name} for game.")
    except discord.NotFound:
        print(f"Text channel for game {game_channel_id} already deleted.")
    except discord.Forbidden:
        print(f"Bot lacks permissions to delete text channel for game {game_channel_id}.")
    except Exception as e:
        print(f"Error deleting text channel for game {game_channel_id}: {e}")
    
    # Delete voice channel
    try:
        voice_channel = guild.get_channel(game_voice_channel_id)
        if voice_channel and isinstance(voice_channel, discord.VoiceChannel):
            await voice_channel.delete(reason=reason)
            print(f"Deleted voice channel {voice_channel.name} for game.")
    except discord.NotFound:
        print(f"Voice channel for game {game_voice_channel_id} already deleted.")
    except discord.Forbidden:
        print(f"Bot lacks permissions to delete voice channel for game {game_voice_channel_id}.")
    except Exception as e:
        print(f"Error deleting voice channel for game {game_voice_channel_id}: {e}")

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
    img_width, img_height = 600, 450 # Increased height for more space
    bg_color = (30, 30, 30) # Dark grey
    text_color = (220, 220, 220) # Light grey
    accent_color = (150, 150, 150) # Medium grey for highlights
    
    img = Image.new('RGB', (img_width, img_height), color=bg_color)
    draw = ImageDraw.Draw(img)
    
    try:
        font_large = ImageFont.truetype("arial.ttf", 40) # Larger font
        font_medium = ImageFont.truetype("arial.ttf", 28) # Larger font
        font_small = ImageFont.truetype("arial.ttf", 20) # Larger font
        font_mono = ImageFont.truetype("arial.ttf", 20) # Matching small for consistency
    except IOError:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
        font_mono = ImageFont.load_default()
    
    # Get Minecraft skin (head only)
    skin_img = None
    try:
        async with aiohttp.ClientSession() as session:
            # Use /avatar/ for head only
            async with session.get(f"https://mc-heads.net/avatar/{ign}/150.png") as resp:
                if resp.status == 200:
                    skin_data = io.BytesIO(await resp.read())
                    skin_img = Image.open(skin_data).convert("RGBA")
            if skin_img:
                # Make skin monochrome
                skin_img = skin_img.convert("L").convert("RGBA") # Convert to grayscale, then back to RGBA for alpha
                # Resize for consistency and position (square for head)
                skin_img = skin_img.resize((150, 150), Image.Resampling.LANCZOS)
                img.paste(skin_img, (30, 60), skin_img) # Adjusted position
    except Exception as e:
        print(f"Could not fetch or process skin for {ign}: {e}")
        # Draw a placeholder if skin fails
        draw.rectangle((30, 60, 180, 210), fill=accent_color, outline=text_color) # Adjusted placeholder size/pos
        draw.text((60, 120), "No Skin", font=font_small, fill=text_color)
    
    # Draw player info
    draw.text((200, 60), f"{ign}", font=font_large, fill=text_color)
    draw.text((200, 120), f"ELO: {elo}", font=font_medium, fill=accent_color)
    
    # Draw stats with better alignment
    stats_x_start = 200
    stats_y_start = 170 # Adjusted start Y for stats
    line_height = 35 # Increased line height for larger font
    
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
    cleanup_completed_games.start() # New task for game channel cleanup
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
        MANAGER_ROLE_NAME: 3, # Standardized name
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
    """Handles messages for game picking phase (if applicable)."""
    if message.author == bot.user:
        return
    
    # This part needs a proper `handle_pick` function if picking is manual
    # For now, I'll remove the call to handle_pick to prevent errors if it's not defined
    # If you intend to have manual picking, you need to define `handle_pick`
    # for game voice channels and other relevant conditions.
    # for game_id, game_data in active_games.items():
    #     if message.channel.id == game_data["channel_id"] and game_data["status"] == "picking":
    #         await handle_pick(message, game_id, game_data)
    #         break # Only handle one game per message
    
    await bot.process_commands(message)

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    """
    Tracks voice state changes for AFK detection in game voice channels.
    Updates last_activity_timestamp in active_games for members in game VCs.
    """
    # Check if the member is in an active game voice channel
    for game_id, game_data in list(active_games.items()): # Use list to avoid RuntimeError: dictionary changed size during iteration
        if game_data["voice_channel_id"] == (before.channel.id if before.channel else None) or \
           game_data["voice_channel_id"] == (after.channel.id if after.channel else None):
            
            # If member moved into or out of a relevant game VC, or state changed within it
            if member.id in game_data["players"]:
                # If they are in the game VC and not self-deafened, update activity
                if after.channel and after.channel.id == game_data["voice_channel_id"] and not after.self_deaf and not after.self_mute:
                    game_data["last_activity_timestamp"] = datetime.datetime.now()
                # If they leave the channel, or deafen, etc., their activity stops
                # The AFK check will handle moving them after a timeout.
                active_games[game_id] = game_data # Update the global dict


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
    """Checks active queues (voice channels) and starts games when enough players are present."""
    global game_counter
    
    if not queue_status:
        return
    
    guild = bot.guilds[0] # Assuming bot operates in a single guild

    for queue_type in active_queues:
        required_players = QUEUE_TYPES[queue_type]
        queue_vc_id = QUEUE_VC_MAP.get(queue_type)
        
        if not queue_vc_id:
            print(f"Warning: No voice channel ID configured for queue type: {queue_type}. Skipping.")
            continue

        queue_voice_channel = guild.get_channel(queue_vc_id)
        
        if not queue_voice_channel or not isinstance(queue_voice_channel, discord.VoiceChannel):
            print(f"Warning: Configured queue voice channel (ID: {queue_vc_id}) for {queue_type} not found or is not a voice channel.")
            continue

        # Get members currently in the voice channel
        players_in_queue_vc = [member for member in queue_voice_channel.members if not member.bot] # Exclude bots
        
        if len(players_in_queue_vc) >= required_players:
            players_for_game = players_in_queue_vc[:required_players] # Take exactly the required number of players
            
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
            game_voice_channel = await guild.create_voice_channel(
                f"Game #{game_counter:04d}",
                category=voice_category
            )
            
            teams: Dict[int, List[int]] = {1: [], 2: []}
            captains: List[int] = []
            description: str = ""
            color: discord.Color = discord.Color.blue()

            # Move players to the new game voice channel
            for player_member in players_for_game:
                try:
                    await player_member.move_to(game_voice_channel)
                    print(f"Moved {player_member.display_name} to {game_voice_channel.name}")
                except discord.Forbidden:
                    print(f"Bot lacks permissions to move {player_member.display_name} to {game_voice_channel.name}")
                except Exception as e:
                    print(f"Error moving {player_member.display_name}: {e}")
            
            # --- Party Season Logic vs. Captain Picking ---
            if party_size is not None:
                # Fair ELO matchmaking for party season
                players_with_elo = []
                for p_member in players_for_game: # Use actual member objects here
                    elo = await get_player_elo(p_member.id)
                    players_with_elo.append({"id": p_member.id, "elo": elo})
                
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
                for player_member in players_for_game: # Use actual member objects here
                    elo = await get_player_elo(player_member.id)
                    player_elos.append((player_member.id, elo))
                
                player_elos.sort(key=lambda x: x[1], reverse=True) # Sort by ELO descending
                
                # Select top 2 as captains
                captain1 = player_elos[0][0]
                captain2 = player_elos[1][0]
                
                captains = [captain1, captain2]
                teams[1].append(captain1)
                teams[2].append(captain2)
                
                description = "Captains have been selected by ELO! Time to pick teams."
                color = discord.Color.blue() # Corrected color

            # Store game data in active_games
            active_games[game_counter] = {
                "channel_id": game_channel.id,
                "voice_channel_id": game_voice_channel.id,
                "players": [p.id for p in players_for_game], # Store player IDs
                "queue_type": queue_type,
                "status": "picking",
                "teams": teams,
                "captains": captains,
                "current_picker": captains[0] if captains else None, # First captain picks first
                "picking_turn": 0, # Index for picking turns
                "db_game_id": None, # Will be set after DB insert
                "last_activity_timestamp": datetime.datetime.now() # Initialize for AFK check
            }

            # Insert game into database
            if db_pool is None: return
            async with db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    try:
                        await cursor.execute(
                            "INSERT INTO games (queue_type, status, text_channel_id, voice_channel_id) VALUES (%s, %s, %s, %s)",
                            (queue_type, "picking", game_channel.id, game_voice_channel.id) # Store channel IDs in DB
                        )
                        db_game_id = cursor.lastrowid
                        active_games[game_counter]["db_game_id"] = db_game_id

                        # Insert game players into database
                        for team_num, player_ids in teams.items():
                            for p_id in player_ids:
                                await cursor.execute(
                                    "INSERT INTO game_players (game_id, discord_id, team) VALUES (%s, %s, %s)",
                                    (db_game_id, p_id, team_num)
                                )
                        await conn.commit()
                        print(f"Game {game_counter} ({queue_type}) started. DB ID: {db_game_id}")
                    except aiomysql.Error as e:
                        print(f"Error inserting game into DB: {e}")
                        # Clean up created channels if DB insertion fails
                        await cleanup_game_channels(game_channel.id, game_voice_channel.id, "DB error on game start.")
                        del active_games[game_counter]
                        game_counter += 1 # Increment counter even on failure to avoid ID reuse immediately
                        continue
            
            # Announce game start
            game_announcement_channel = await get_channel_by_config(guild, GAMES_DISPLAY_CHANNEL_ID, GAMES_DISPLAY_CHANNEL_NAME)
            if game_announcement_channel:
                await game_announcement_channel.send(
                    f"A {queue_type} game has started in {game_channel.mention}! Game ID: `{game_counter:04d}`"
                )

            embed = create_embed(
                title=f"Game #{game_counter:04d} ({queue_type})",
                description=description,
                color=color,
                fields=[
                    {"name": "Text Channel", "value": game_channel.mention, "inline": True},
                    {"name": "Voice Channel", "value": game_voice_channel.mention, "inline": True}
                ]
            )

            if party_size is None: # Captain picking
                captain_mentions = [guild.get_member(c).mention for c in captains if guild.get_member(c)]
                embed.add_field(name="Captains", value=", ".join(captain_mentions), inline=False)
                embed.add_field(name="Instructions", value=f"Captains, use `@{bot.user.name} pick <player_name>` to pick players in {game_channel.mention}.", inline=False) # Updated to use bot mention
            else: # Auto-balancing
                 embed.add_field(name="Team 1", value="\n".join([guild.get_member(p_id).mention for p_id in teams[1]]), inline=True)
                 embed.add_field(name="Team 2", value="\n".join([guild.get_member(p_id).mention for p_id in teams[2]]), inline=True)


            await game_channel.send(embed=embed)
            
            # Increment game counter for the next game
            game_counter += 1

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

@tasks.loop(hours=168) # Changed from days=7 to hours=168
async def check_elo_decay():
    """Applies ELO decay to inactive players."""
    if db_pool is None: return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Implement ELO decay logic here based on your rules (e.g., last game played)
            pass

@tasks.loop(minutes=1)
async def check_afk_players():
    """Moves AFK players from game VCs to the AFK voice channel if idle for 5 minutes."""
    guild = bot.guilds[0]
    afk_channel = guild.get_channel(AFK_VOICE_CHANNEL_ID)

    if not afk_channel or not isinstance(afk_channel, discord.VoiceChannel):
        print("AFK voice channel not configured or not found.")
        return

    current_time = datetime.datetime.now()
    idle_threshold = datetime.timedelta(minutes=5)

    for game_id, game_data in list(active_games.items()):
        game_voice_channel_id = game_data.get("voice_channel_id")
        if not game_voice_channel_id:
            continue
        
        game_voice_channel = guild.get_channel(game_voice_channel_id)
        if not game_voice_channel or not isinstance(game_voice_channel, discord.VoiceChannel):
            continue

        for member_id in game_data["players"]:
            member = guild.get_member(member_id)
            if not member or not member.voice or member.voice.channel != game_voice_channel:
                continue # Member not in this game VC or no longer in VC

            # Check if member is idle (self-deafened or self-muted)
            # Or if their last_activity_timestamp exceeds the threshold
            if (member.voice.self_deaf or member.voice.self_mute) or \
               (game_data.get("last_activity_timestamp") and (current_time - game_data["last_activity_timestamp"]) > idle_threshold):
                try:
                    # Move to AFK channel if not already there
                    if member.voice.channel.id != afk_channel.id:
                        await member.move_to(afk_channel, reason="AFK in game voice channel.")
                        print(f"Moved {member.display_name} to AFK channel from game VC.")
                        # Reset last_activity_timestamp if moved to AFK
                        game_data["last_activity_timestamp"] = datetime.datetime.now() # Prevents immediate re-move
                except discord.Forbidden:
                    print(f"Bot lacks permissions to move {member.display_name} to AFK channel.")
                except Exception as e:
                    print(f"Error moving {member.display_name} to AFK channel: {e}")

@tasks.loop(minutes=2) # Check every 2 minutes for completed games
async def cleanup_completed_games():
    """Checks for completed games in DB and deletes their associated Discord channels."""
    if db_pool is None: return
    guild = bot.guilds[0]

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            try:
                await cursor.execute(
                    "SELECT game_id, text_channel_id, voice_channel_id FROM games WHERE status = 'completed'"
                )
                completed_games = await cursor.fetchall()

                for game in completed_games:
                    game_id = game['game_id']
                    text_channel_id = game['text_channel_id']
                    voice_channel_id = game['voice_channel_id']

                    # Ensure channels exist in active_games before proceeding to delete
                    # This prevents trying to delete channels already removed by manual cleanup
                    if game_id in active_games:
                        await cleanup_game_channels(text_channel_id, voice_channel_id, f"Game #{game_id} concluded and channels cleaned up.")
                        del active_games[game_id] # Remove from active_games tracking

                    # Optionally, update status in DB to 'cleaned_up' to avoid repeated attempts
                    await cursor.execute(
                        "UPDATE games SET status = 'cleaned_up' WHERE game_id = %s",
                        (game_id,)
                    )
                    await conn.commit()

            except aiomysql.Error as e:
                print(f"Error checking/cleaning up completed games: {e}")
            except Exception as e:
                print(f"Unexpected error in cleanup_completed_games: {e}")


# --- Views for interactions (StrikeRequestView, ScreenshareView, TicketView, PPPVotingView) ---

class StrikeRequestView(discord.ui.View):
    def __init__(self, target_user: discord.Member, reason: str, requestor: discord.Member):
        super().__init__(timeout=600) # Increased timeout to 10 minutes
        self.target_user = target_user
        self.reason = reason
        self.yes_votes = set()
        self.no_votes = set()
        self.requestor = requestor
        self.message = None # To store the message this view is attached to

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
        if self.message:
            await self.message.edit(embed=embed, view=self)
        else:
            await interaction.message.edit(embed=embed, view=self)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.green, emoji="👍")
    async def yes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.yes_votes or interaction.user.id in self.no_votes:
            await interaction.response.send_message("You have already voted.", ephemeral=True)
            return
        
        # Check if user has staff role (Moderator, Admin, Manager, PI)
        staff_roles = [get_role_by_name(interaction.guild, MODERATOR_ROLE_NAME),
                        get_role_by_name(interaction.guild, ADMIN_STAFF_ROLE_NAME),
                        get_role_by_name(interaction.guild, MANAGER_ROLE_NAME), # Standardized role name
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
                        # Before inserting strike, ensure user exists in `users` table
                        await cursor.execute(
                            "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                            (self.target_user.id, self.target_user.display_name, 0, 0, 0, 0, 0, 0)
                        )
                        
                        await cursor.execute(
                            "INSERT INTO strikes (discord_id, reason, issued_by, strike_date) VALUES (%s, %s, %s, %s)",
                            (self.target_user.id, self.reason, self.requestor.id, strike_time)
                        )
                        await conn.commit()

                        # Deduct ELO for strike
                        await update_player_elo_in_db(self.target_user.id, -40) # Deduct 40 ELO
                        current_elo = await get_player_elo(self.target_user.id)
                        await update_elo_role(self.target_user.id, current_elo) # Update ELO role and nickname

                        user_embed = create_embed(
                            title="You have been Stripped of your Dignity (Strike)", # Changed title
                            description=f"You have received a strike on {interaction.guild.name} for: {self.reason}\nYour ELO has been reduced by 40.",
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
                        get_role_by_name(interaction.guild, MANAGER_ROLE_NAME), # Standardized role name
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
        
        if self.message:
            if yes_count < required_votes:
                await self.message.channel.send("Strike request timed out due to insufficient positive votes. Ticket will now be deleted.")
                # Delete the ticket channel if it times out without enough votes
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
        self.message = None # To store the message this view is attached to

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
        if self.message and self.message.channel:
            try:
                await self.message.channel.delete(reason="Screenshare ticket timed out.")
            except discord.Forbidden:
                print(f"Bot lacks permissions to delete screenshare ticket channel on timeout.")
            except Exception as e:
                print(f"Error deleting screenshare ticket channel on timeout: {e}")
        
        log_content = {
            "User": f"<@{self.target_user.id}> ({self.target_user.id})",
            "Status": "Timed Out",
            "Reason": "Screenshare request timed out without being resolved."
        }
        await send_log_html(self.target_user.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, "Screenshare Ticket Timed Out", log_content)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.primary, emoji="✋", custom_id="screenshare_claim")
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
        
        # Enable the "Close Screenshare" button
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.custom_id == "screenshare_close":
                item.disabled = False
                break

        await interaction.message.edit(view=self)
        await interaction.response.send_message(f"You have claimed the screenshare ticket for {self.target_user.mention}.", ephemeral=True)

        log_content = {
            "User": f"<@{self.target_user.id}> ({self.target_user.id})",
            "Claimed By": f"<@{self.claimed_by.id}> ({self.claimed_by.id})",
            "Ticket Channel": interaction.channel.mention
        }
        await send_log_html(interaction.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, "Screenshare Ticket Claimed", log_content)

    @discord.ui.button(label="Close Screenshare", style=discord.ButtonStyle.red, emoji="❌", custom_id="screenshare_close", disabled=True)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Allow only the person who claimed or Admin+ to close
        is_admin_plus = any(role in interaction.user.roles for role in [
            get_role_by_name(interaction.guild, ADMIN_STAFF_ROLE_NAME),
            get_role_by_name(interaction.guild, MANAGER_ROLE_NAME), # Standardized role name
            get_role_by_name(interaction.guild, PI_ROLE_NAME)
        ] if role)

        if self.claimed_by and self.claimed_by != interaction.user and not is_admin_plus:
            await interaction.response.send_message("Only the person who claimed this ticket or an Administrator+ can close it.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True) # Acknowledge the interaction

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

        log_content = {
            "User": f"<@{self.target_user.id}> ({self.target_user.id})",
            "Closed By": f"<@{interaction.user.id}> ({interaction.user.id})",
            "Ticket Channel": interaction.channel.mention
        }
        await send_log_html(interaction.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, "Screenshare Ticket Closed", log_content)
        
        await interaction.followup.send(f"Screenshare ticket for {self.target_user.mention} has been closed.")
        
        # Delete the ticket channel
        if interaction.channel:
            try:
                await interaction.channel.delete(reason="Screenshare ticket closed.")
            except discord.Forbidden:
                print(f"Bot lacks permissions to delete screenshare ticket channel.")
            except Exception as e:
                print(f"Error deleting screenshare ticket channel: {e}")


class TicketView(discord.ui.View):
    def __init__(self, owner: discord.Member):
        super().__init__(timeout=None)
        self.owner = owner
        self.message = None # To store the message this view is attached to

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.red, emoji="🔒", custom_id="ticket_close")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only owner or staff can close
        is_staff = any(role in interaction.user.roles for role in [
            get_role_by_name(interaction.guild, MODERATOR_ROLE_NAME),
            get_role_by_name(interaction.guild, ADMIN_STAFF_ROLE_NAME),
            get_role_by_name(interaction.guild, MANAGER_ROLE_NAME), # Standardized role name
            get_role_by_name(interaction.guild, PI_ROLE_NAME),
            get_role_by_name(interaction.guild, STAFF_ROLE_NAME)
        ] if role)

        if interaction.user != self.owner and not is_staff:
            await interaction.response.send_message("You are not authorized to close this ticket.", ephemeral=True)
            return

        channel = interaction.channel
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

        log_content = {
            "Ticket Owner": f"<@{self.owner.id}> ({self.owner.id})",
            "Closed By": f"<@{interaction.user.id}> ({interaction.user.id})",
            "Channel Name": channel.name,
            "Channel ID": channel.id
        }
        await send_log_html(interaction.guild, TICKET_LOG_CHANNEL_ID, TICKET_LOG_CHANNEL_NAME, "Ticket Closed", log_content)


    @discord.ui.button(label="Claim Ticket", style=discord.ButtonStyle.blurple, emoji="🙋", custom_id="ticket_claim")
    async def claim_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = get_role_by_name(interaction.guild, STAFF_ROLE_NAME)
        if not staff_role or staff_role not in interaction.user.roles:
            await interaction.response.send_message("You must be a Staff member to claim this ticket.", ephemeral=True)
            return

        # Explicitly set permissions for the claiming staff member
        await interaction.channel.set_permissions(interaction.user, read_messages=True, send_messages=True, manage_channels=True)
        # Remove default read for general staff roles to make it clear who claimed
        for role_name in [MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME, STAFF_ROLE_NAME, SCREENSHARING_TEAM_ROLE_NAME]: # Standardized role name
            role = get_role_by_name(interaction.guild, role_name)
            if role and role != staff_role: # Don't remove the staff role itself if it's the claiming role
                await interaction.channel.set_permissions(role, read_messages=False, send_messages=False)

        await interaction.response.send_message(f"You have claimed this ticket. {self.owner.mention} can now see and reply.", ephemeral=True)

        embed = create_embed(
            title="Ticket Claimed",
            description=f"This ticket has been claimed by {interaction.user.mention}. They will assist you shortly.",
            color=discord.Color.blue()
        )
        await interaction.channel.send(embed=embed)
        
        log_content = {
            "Ticket Owner": f"<@{self.owner.id}> ({self.owner.id})",
            "Claimed By": f"<@{interaction.user.id}> ({interaction.user.id})",
            "Channel Name": interaction.channel.name,
            "Channel ID": interaction.channel.id
        }
        await send_log_html(interaction.guild, TICKET_LOG_CHANNEL_ID, TICKET_LOG_CHANNEL_NAME, "Ticket Claimed", log_content)


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
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def add_win(ctx: commands.Context, member: discord.Member, mvp: Optional[bool] = False):
    await ctx.message.add_reaction("⌛") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Ensure user exists in `users` table before proceeding
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, member.display_name, 0, 0, 0, 0, 0, 0)
                )

                await cursor.execute("SELECT wins, losses, elo FROM users WHERE discord_id = %s", (member.id,))
                result = await cursor.fetchone()
                if not result: # Should not happen with INSERT IGNORE, but as a safeguard
                    await ctx.send(f"{member.display_name} is not registered in the database, cannot add win.")
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
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def add_loss(ctx: commands.Context, member: discord.Member):
    await ctx.message.add_reaction("⌛") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Ensure user exists in `users` table before proceeding
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, member.display_name, 0, 0, 0, 0, 0, 0)
                )

                await cursor.execute("SELECT wins, losses, elo FROM users WHERE discord_id = %s", (member.id,))
                result = await cursor.fetchone()
                if not result: # Should not happen with INSERT IGNORE, but as a safeguard
                    await ctx.send(f"{member.display_name} is not registered in the database, cannot add loss.")
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

@bot.command(name="elochange")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def elo_change(ctx: commands.Context, member: discord.Member, value: int):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Ensure user exists in `users` table
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, member.display_name, 0, 0, 0, 0, 0, 0)
                )

                current_elo = await get_player_elo(member.id)
                new_elo = current_elo + value

                await cursor.execute(
                    "UPDATE users SET elo = %s WHERE discord_id = %s",
                    (new_elo, member.id)
                )
                await conn.commit()

                await update_elo_role(member.id, new_elo)
                await ctx.send(f"ELO for {member.mention} changed by {value}. New ELO: {new_elo}.")
            except aiomysql.Error as e:
                await ctx.send(f"An error occurred: {e}")

@bot.command(name="setelo", aliases=["elo"]) # Added 'elo' as an alias
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def set_elo(ctx: commands.Context, member: discord.Member, value: int):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Ensure user exists in `users` table
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, member.display_name, 0, 0, 0, 0, 0, 0)
                )

                await cursor.execute(
                    "UPDATE users SET elo = %s WHERE discord_id = %s",
                    (value, member.id)
                )
                await conn.commit()

                await update_elo_role(member.id, value)
                await ctx.send(f"ELO for {member.mention} set to {value}.")
            except aiomysql.Error as e:
                await ctx.send(f"An error occurred: {e}")

@bot.command(name="info", aliases=["i"])
async def player_info(ctx: commands.Context, member: Optional[discord.Member] = None):
    await ctx.message.add_reaction("⌛") # Add reaction
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
                    await ctx.send(f"{member.display_name} is not registered. Please register first, or use `=forceregister` if you are staff.")
                    return

                ign, elo, wins, losses, mvps, streak = result
                wlr = wins / losses if losses > 0 else wins # If no losses, W/L is just wins
                
                # Generate the custom player info image
                file = await generate_player_info_image(ign, elo, wins, losses, wlr, mvps, streak)
                
                # Send as a direct file attachment instead of inside an embed
                await ctx.send(file=file)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while fetching player info: {e}")
            except Exception as e:
                await ctx.send(f"An unexpected error occurred: {e}")


# Strike, Ban, Mute commands - Ensure embeds are sent to logs AND chat response
@bot.command(name="strike")
@commands.has_any_role(MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def strike(ctx: commands.Context, member: discord.Member, *, reason: str):
    await ctx.message.add_reaction("⌛") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Ensure user exists in `users` table before proceeding
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, member.display_name, 0, 0, 0, 0, 0, 0)
                )

                # Deduct ELO for strike
                await update_player_elo_in_db(member.id, -40) # Deduct 40 ELO
                current_elo = await get_player_elo(member.id)
                await update_elo_role(member.id, current_elo) # Update ELO role and nickname

                await cursor.execute(
                    "INSERT INTO strikes (discord_id, reason, issued_by) VALUES (%s, %s, %s)",
                    (member.id, reason, ctx.author.id)
                )
                strike_id = cursor.lastrowid # Get the ID of the inserted strike
                await conn.commit()
                
                # Create the detailed embed
                detailed_embed = create_embed(
                    title="User Stripped of Dignity (Strike)",
                    description=f"<@{member.id}> has received a strike.",
                    color=discord.Color.red(),
                    fields=[
                        {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
                        {"name": "Reason", "value": reason, "inline": True},
                        {"name": "Issued By", "value": f"<@{ctx.author.id}>", "inline": True},
                        {"name": "Strike ID", "value": str(strike_id), "inline": True},
                        {"name": "ELO Change", "value": "-40", "inline": True}
                    ]
                )
                
                # Send embed to user
                user_embed = create_embed(
                    title="You have been Stripped of your Dignity (Strike)",
                    description=f"You have received a strike on {ctx.guild.name} for: {reason}\nYour ELO has been reduced by 40.",
                    color=discord.Color.red()
                )
                try:
                    await member.send(embed=user_embed)
                except discord.Forbidden:
                    print(f"Could not DM {member.display_name} about the strike.")

                # Send detailed embed to log channel
                await send_log_embed(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME, detailed_embed)
                
                # Send detailed embed to current channel
                await ctx.send(embed=detailed_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred: {e}. Ensure the user exists in the database.")


@bot.command(name="strikeremove", aliases=["srem"])
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def remove_strike(ctx: commands.Context, strike_id: int, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Get strike details to inform user/logs
                await cursor.execute(
                    "SELECT discord_id, reason FROM strikes WHERE strike_id = %s",
                    (strike_id,)
                )
                strike_info = await cursor.fetchone()

                if not strike_info:
                    await ctx.send(f"Strike with ID `{strike_id}` not found.")
                    return
                
                target_discord_id, strike_reason = strike_info
                target_member = ctx.guild.get_member(target_discord_id)

                await cursor.execute(
                    "DELETE FROM strikes WHERE strike_id = %s",
                    (strike_id,)
                )
                await conn.commit()

                await update_player_elo_in_db(target_discord_id, 40) # Add 40 ELO back
                current_elo = await get_player_elo(target_discord_id)
                await update_elo_role(target_discord_id, current_elo) # Update ELO role and nickname

                # Create the detailed embed
                detailed_embed = create_embed(
                    title="Strike Removed",
                    description=f"Strike ID `{strike_id}` for <@{target_discord_id}> has been removed.",
                    color=discord.Color.green(),
                    fields=[
                        {"name": "Strike ID", "value": str(strike_id), "inline": True},
                        {"name": "User", "value": f"<@{target_discord_id}> ({target_discord_id})", "inline": True},
                        {"name": "Original Reason", "value": strike_reason, "inline": False},
                        {"name": "Removal Reason", "value": reason, "inline": False},
                        {"name": "Removed By", "value": f"<@{ctx.author.id}>", "inline": True},
                        {"name": "ELO Change", "value": "+40", "inline": True}
                    ]
                )

                user_embed = create_embed(
                    title="Strike Removed",
                    description=f"A strike (ID: `{strike_id}`) against you has been removed from {ctx.guild.name} for: {reason}\nYour ELO has been restored.",
                    color=discord.Color.green()
                )
                if target_member:
                    try:
                        await target_member.send(embed=user_embed)
                    except discord.Forbidden:
                        print(f"Could not DM {target_member.display_name} about strike removal.")

                # Send detailed embed to log channel
                await send_log_embed(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME, detailed_embed)
                
                # Send detailed embed to current channel
                await ctx.send(embed=detailed_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred: {e}")

@bot.command(name="ban")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME) # Only Admin+ can ban
async def ban_user(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("⌛") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    expires_at = None
    duration_text = "indefinitely"

    # Parse duration string (e.g., 1s, 5m, 2h, 1d, 1y)
    try:
        amount = int(duration[:-1])
        unit = duration[-1].lower()
        if unit == 's':
            expires_at = datetime.datetime.now() + datetime.timedelta(seconds=amount)
            duration_text = f"for {amount} second(s)"
        elif unit == 'm':
            expires_at = datetime.datetime.now() + datetime.timedelta(minutes=amount)
            duration_text = f"for {amount} minute(s)"
        elif unit == 'h':
            expires_at = datetime.datetime.now() + datetime.timedelta(hours=amount)
            duration_text = f"for {amount} hour(s)"
        elif unit == 'd':
            expires_at = datetime.datetime.now() + datetime.timedelta(days=amount)
            duration_text = f"for {amount} day(s)"
        elif unit == 'y':
            expires_at = datetime.datetime.now() + datetime.timedelta(days=amount*365) # Approximation for years
            duration_text = f"for {amount} year(s)"
        else:
            await ctx.send("Invalid duration format. Use 1s, 1m, 1h, 1d, 1y.", ephemeral=True)
            return
    except ValueError:
        if duration.lower() != "permanent":
            await ctx.send("Invalid duration format. Use 1s, 1m, 1h, 1d, 1y, or 'permanent'.", ephemeral=True)
            return
        # If 'permanent', expires_at remains None

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Ensure user exists in `users` table before proceeding
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, member.display_name, 0, 0, 0, 0, 0, 0)
                )

                await cursor.execute(
                    "INSERT INTO bans (discord_id, reason, issued_by, expires_at) VALUES (%s, %s, %s, %s)",
                    (member.id, reason, ctx.author.id, expires_at)
                )
                await conn.commit()
                
                ban_id = cursor.lastrowid # Get the ID of the inserted ban

                banned_role = get_role_by_name(ctx.guild, BANNED_ROLE_NAME)
                if not banned_role:
                    await ctx.send("Banned role not found. Please configure it.")
                    return

                try:
                    await member.add_roles(banned_role, reason=reason)
                except discord.Forbidden:
                    await ctx.send("I don't have permissions to assign the 'Banned' role.")
                    return

                # Create the detailed embed
                detailed_embed = create_embed(
                    title="User Banned",
                    description=f"<@{member.id}> has been banned {duration_text}.",
                    color=discord.Color.red(),
                    fields=[
                        {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
                        {"name": "Reason", "value": reason, "inline": True},
                        {"name": "Banned By", "value": f"<@{ctx.author.id}>", "inline": True},
                        {"name": "Duration", "value": duration_text, "inline": True},
                        {"name": "Ban ID", "value": str(ban_id), "inline": True}
                    ]
                )
                if expires_at:
                    detailed_embed.add_field(name="Expires At", value=expires_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=False)

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
                    print(f"Could not DM {member.display_name} about the ban.")

                # Send detailed embed to log channel
                await send_log_embed(ctx.guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME, detailed_embed)
                
                # Send detailed embed to current channel
                await ctx.send(embed=detailed_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while banning: {e}. Ensure the user exists in the database.")

@bot.command(name="unban")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def unban_user(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    banned_role = get_role_by_name(ctx.guild, BANNED_ROLE_NAME)
    if not banned_role:
        await ctx.send("Banned role not found. Please configure it.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "UPDATE bans SET active = FALSE, removed_by = %s, removed_reason = %s, removed_at = NOW() WHERE discord_id = %s AND active = TRUE",
                    (ctx.author.id, reason, member.id)
                )
                await conn.commit()

                if banned_role in member.roles:
                    try:
                        await member.remove_roles(banned_role, reason=f"Unbanned by {ctx.author.name}: {reason}")
                    except discord.Forbidden:
                        await ctx.send(f"I don't have permissions to remove the 'Banned' role from {member.display_name}.")
                        return

                # Create the detailed embed
                detailed_embed = create_embed(
                    title="User Unbanned",
                    description=f"<@{member.id}> has been unbanned.",
                    color=discord.Color.green(),
                    fields=[
                        {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
                        {"name": "Reason", "value": reason, "inline": True},
                        {"name": "Unbanned By", "value": f"<@{ctx.author.id}>", "inline": True}
                    ]
                )

                user_embed = create_embed(
                    title="You have been Unbanned!",
                    description=f"You have been unbanned from {ctx.guild.name} for: {reason}",
                    color=discord.Color.green()
                )
                try:
                    await member.send(embed=user_embed)
                except discord.Forbidden:
                    print(f"Could not DM {member.display_name} about the unban.")

                # Send detailed embed to log channel
                await send_log_embed(ctx.guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME, detailed_embed)
                
                # Send detailed embed to current channel
                await ctx.send(embed=detailed_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while unbanning: {e}")

@bot.command(name="mute")
@commands.has_any_role(MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def mute_user(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("⌛") # Add reaction
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    expires_at = None
    duration_text = "indefinitely"

    # Parse duration string (e.g., 1s, 5m, 2h, 1d, 1y)
    try:
        amount = int(duration[:-1])
        unit = duration[-1].lower()
        if unit == 's':
            expires_at = datetime.datetime.now() + datetime.timedelta(seconds=amount)
            duration_text = f"for {amount} second(s)"
        elif unit == 'm':
            expires_at = datetime.datetime.now() + datetime.timedelta(minutes=amount)
            duration_text = f"for {amount} minute(s)"
        elif unit == 'h':
            expires_at = datetime.datetime.now() + datetime.timedelta(hours=amount)
            duration_text = f"for {amount} hour(s)"
        elif unit == 'd':
            expires_at = datetime.datetime.now() + datetime.timedelta(days=amount)
            duration_text = f"for {amount} day(s)"
        elif unit == 'y':
            expires_at = datetime.datetime.now() + datetime.timedelta(days=amount*365) # Approximation for years
            duration_text = f"for {amount} year(s)"
        else:
            await ctx.send("Invalid duration format. Use 1s, 1m, 1h, 1d, 1y.", ephemeral=True)
            return
    except ValueError:
        if duration.lower() != "permanent":
            await ctx.send("Invalid duration format. Use 1s, 1m, 1h, 1d, 1y, or 'permanent'.", ephemeral=True)
            return
        # If 'permanent', expires_at remains None

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Ensure user exists in `users` table before proceeding
                await cursor.execute(
                    "INSERT IGNORE INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, member.display_name, 0, 0, 0, 0, 0, 0)
                )

                await cursor.execute(
                    "INSERT INTO mutes (discord_id, reason, issued_by, expires_at) VALUES (%s, %s, %s, %s)",
                    (member.id, reason, ctx.author.id, expires_at)
                )
                await conn.commit()
                
                mute_id = cursor.lastrowid # Get the ID of the inserted mute

                muted_role = get_role_by_name(ctx.guild, MUTED_ROLE_NAME)
                if not muted_role:
                    await ctx.send("Muted role not found. Please configure it.")
                    return

                try:
                    await member.add_roles(muted_role, reason=reason)
                except discord.Forbidden:
                    await ctx.send("I don't have permissions to assign the 'Muted' role.")
                    return

                # Create the detailed embed
                detailed_embed = create_embed(
                    title="User Muted",
                    description=f"<@{member.id}> has been muted {duration_text}.",
                    color=discord.Color.orange(),
                    fields=[
                        {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
                        {"name": "Reason", "value": reason, "inline": True},
                        {"name": "Muted By", "value": f"<@{ctx.author.id}>", "inline": True},
                        {"name": "Duration", "value": duration_text, "inline": True},
                        {"name": "Mute ID", "value": str(mute_id), "inline": True}
                    ]
                )
                if expires_at:
                    detailed_embed.add_field(name="Expires At", value=expires_at.strftime("%Y-%m-%d %H:%M:%S UTC"), inline=False)

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
                    print(f"Could not DM {member.display_name} about the mute.")

                # Send detailed embed to log channel
                await send_log_embed(ctx.guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME, detailed_embed)
                
                # Send detailed embed to current channel
                await ctx.send(embed=detailed_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while muting: {e}. Ensure the user exists in the database.")

@bot.command(name="unmute")
@commands.has_any_role(MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def unmute_user(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    muted_role = get_role_by_name(ctx.guild, MUTED_ROLE_NAME)
    if not muted_role:
        await ctx.send("Muted role not found. Please configure it.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "UPDATE mutes SET active = FALSE, removed_by = %s, removed_reason = %s, removed_at = NOW() WHERE discord_id = %s AND active = TRUE",
                    (ctx.author.id, reason, member.id)
                )
                await conn.commit()

                if muted_role in member.roles:
                    try:
                        await member.remove_roles(muted_role, reason=f"Unmuted by {ctx.author.name}: {reason}")
                    except discord.Forbidden:
                        await ctx.send(f"I don't have permissions to remove the 'Muted' role from {member.display_name}.")
                        return

                # Create the detailed embed
                detailed_embed = create_embed(
                    title="User Unmuted",
                    description=f"<@{member.id}> has been unmuted.",
                    color=discord.Color.green(),
                    fields=[
                        {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
                        {"name": "Reason", "value": reason, "inline": True},
                        {"name": "Unmuted By", "value": f"<@{ctx.author.id}>", "inline": True}
                    ]
                )

                user_embed = create_embed(
                    title="You have been Unmuted!",
                    description=f"You have been unmuted from {ctx.guild.name} for: {reason}",
                    color=discord.Color.green()
                )
                try:
                    await member.send(embed=user_embed)
                except discord.Forbidden:
                    print(f"Could not DM {member.display_name} about the unmute.")

                # Send detailed embed to log channel
                await send_log_embed(ctx.guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME, detailed_embed)
                
                # Send detailed embed to current channel
                await ctx.send(embed=detailed_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while unmuting: {e}")


# Admin Commands (Restricted to Admin+)
@bot.command(name="purgechat")
@commands.has_permissions(manage_messages=True)
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def purge_chat(ctx: commands.Context, message_id: Optional[int] = None):
    await ctx.message.add_reaction("⌛") # Add reaction
    if message_id:
        try:
            message = await ctx.channel.fetch_message(message_id)
            deleted_count = 0
            # Fetch messages AFTER the specified ID, then delete them
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
            # Fetch messages BEFORE the command message
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


@bot.group(name="admin", description="Admin commands for bot configuration.")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_group(ctx: commands.Context):
    # This serves as a placeholder for the base =admin command
    # Subcommands will be defined below using @admin_group.command()
    if ctx.invoked_subcommand is None:
        await ctx.send("Use subcommands like `=admin setpartysize`, `=admin queue`, `=admin queues`, `=admin purgeall`.")

@admin_group.command(name="setpartysize")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_set_party_size(ctx: commands.Context, size: str):
    await ctx.message.add_reaction("⌛")
    global party_size
    if size.lower() == "none":
        party_size = None
        await ctx.send("Party season set to None (captain picking enabled).")
    elif size.isdigit() and int(size) in [2, 3, 4]:
        party_size = int(size)
        await ctx.send(f"Party season set to size {party_size}.")
    else:
        await ctx.send("Invalid party size. Use 'none', '2', '3', or '4'.")

@admin_group.command(name="queue")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_set_queue_status(ctx: commands.Context, status_int: int):
    await ctx.message.add_reaction("⌛")
    global queue_status
    if status_int == 1:
        queue_status = True
        await ctx.send("Queues are now open globally.")
    elif status_int == 0:
        queue_status = False
        await ctx.send("Queues are now closed globally.")
    else:
        await ctx.send("Invalid status. Use `1` for open or `0` for closed.")

@admin_group.command(name="queues")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_set_active_queues(ctx: commands.Context, *queue_types_str: str):
    await ctx.message.add_reaction("⌛")
    global active_queues
    valid_types = ["3v3", "4v4", "3v3_pups", "4v4_pups"]
    
    new_active_queues = []
    for q_type_str in queue_types_str:
        if q_type_str.lower() in valid_types:
            if q_type_str.lower() not in QUEUE_VC_MAP:
                await ctx.send(f"Error: Voice channel ID for `{q_type_str}` is not configured in `QUEUE_VC_MAP`. Cannot activate this queue type.", ephemeral=True)
                return # Stop if a VC is not mapped
            new_active_queues.append(q_type_str.lower())
        else:
            await ctx.send(f"Invalid queue type provided: `{q_type_str}`. Valid types are: {', '.join(valid_types)}", ephemeral=True)
            return

    active_queues = new_active_queues
    if not active_queues:
        await ctx.send("All queues have been deactivated.")
    else:
        await ctx.send(f"Active queues set to: {', '.join(active_queues)}")

@admin_group.command(name="purgeall")
@commands.has_any_role(PI_ROLE_NAME) # Only PI can purge all
async def admin_purge_all_stats(ctx: commands.Context):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    # Add confirmation mechanism
    confirm_message = await ctx.send("ARE YOU ABSOLUTELY SURE YOU WANT TO PURGE ALL PLAYER STATS (ELO, wins, losses, mvps, streak, games, strikes, bans, mutes)? This action is irreversible. Reply `confirm purge` within 10 seconds to proceed.")

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() == "confirm purge"

    try:
        confirmation = await bot.wait_for('message', check=check, timeout=10.0)
    except asyncio.TimeoutError:
        await ctx.send("Purge cancelled. You did not confirm in time.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # ONLY RESET ELO AND STATS, DO NOT UNREGISTER
                await cursor.execute("UPDATE users SET elo = 0, wins = 0, losses = 0, mvps = 0, streak = 0")
                await cursor.execute("DELETE FROM games")
                await cursor.execute("DELETE FROM game_players")
                await cursor.execute("DELETE FROM strikes")
                await cursor.execute("DELETE FROM bans")
                await cursor.execute("DELETE FROM mutes")
                await conn.commit()

                # Also reset nicknames for all members (if they have ELO nicknames)
                guild = ctx.guild
                for member in guild.members:
                    if member.nick and '[' in member.nick and ']' in member.nick: # Heuristic for ELO nickname
                        try:
                            # Attempt to remove ELO prefix from nickname
                            if ']' in member.nick:
                                new_nick = member.nick.split(']', 1)[1].strip()
                                await member.edit(nick=new_nick if new_nick else None) # Reset to original username if only ELO was there
                            else:
                                await member.edit(nick=None) # Fallback to clear if format is unexpected
                        except discord.Forbidden:
                            print(f"Bot lacks permissions to reset nickname for {member.display_name}")
                        except Exception as e:
                            print(f"Error resetting nickname for {member.display_name}: {e}")

                await ctx.send("All player stats (ELO, wins, losses, mvps, streak), game data, strikes, bans, and mutes have been purged from the database. Nicknames adjusted.")
                log_embed = create_embed(
                    title="Database Purge (Stats Only)",
                    description=f"All player stats (ELO, wins, losses, mvps, streak), game data, strikes, bans, and mutes have been completely purged by {ctx.author.mention}.\n**User registration status remains unchanged.**",
                    color=discord.Color.dark_red()
                )
                await send_log_embed(ctx.guild, STAFF_UPDATES_CHANNEL_ID, STAFF_UPDATES_CHANNEL_NAME, log_embed) # Send to staff updates
            except aiomysql.Error as e:
                await ctx.send(f"An error occurred during purge: {e}")

# Game result modification commands (Admin+ only, not under =admin prefix)
@bot.command(name="vg")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def view_game_results(ctx: commands.Context, game_id: int):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            try:
                await cursor.execute(
                    """
                    SELECT g.*, 
                           GROUP_CONCAT(CASE WHEN gp.team = 1 THEN u1.minecraft_ign ELSE NULL END) AS team1_igns,
                           GROUP_CONCAT(CASE WHEN gp.team = 2 THEN u2.minecraft_ign ELSE NULL END) AS team2_igns
                    FROM games g
                    LEFT JOIN game_players gp ON g.game_id = gp.game_id
                    LEFT JOIN users u1 ON gp.discord_id = u1.discord_id AND gp.team = 1
                    LEFT JOIN users u2 ON gp.discord_id = u2.discord_id AND gp.team = 2
                    WHERE g.game_id = %s
                    GROUP BY g.game_id
                    """,
                    (game_id,)
                )
                game_data = await cursor.fetchone()

                if not game_data:
                    await ctx.send(f"Game ID `{game_id}` not found.")
                    return

                team1_players = [ign for ign in game_data['team1_igns'].split(',') if ign] if game_data['team1_igns'] else []
                team2_players = [ign for ign in game_data['team2_igns'].split(',') if ign] if game_data['team2_igns'] else []

                embed = create_embed(
                    title=f"Game #{game_id} Details",
                    description=f"**Queue Type:** {game_data['queue_type']}\n**Status:** {game_data['status']}",
                    color=discord.Color.blue()
                )
                embed.add_field(name="Team 1", value="\n".join(team1_players) if team1_players else "No players", inline=True)
                embed.add_field(name="Team 2", value="\n".join(team2_players) if team2_players else "No players", inline=True)
                
                if game_data['winning_team']:
                    winner_text = f"Team {game_data['winning_team']}"
                    if game_data['mvp_discord_id']:
                        mvp_user = bot.get_user(game_data['mvp_discord_id']) or await bot.fetch_user(game_data['mvp_discord_id'])
                        mvp_name = mvp_user.display_name if mvp_user else f"Unknown User ({game_data['mvp_discord_id']})"
                        winner_text += f" (MVP: {mvp_name})"
                    embed.add_field(name="Winner", value=winner_text, inline=False)
                    embed.add_field(name="Scored By", value=f"<@{game_data['scored_by']}> (Bot scored: {game_data['bot_scored']})", inline=False)
                else:
                    embed.add_field(name="Result", value="Not yet scored.", inline=False)

                await ctx.send(embed=embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while fetching game details: {e}")

@bot.command(name="undo")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def undo_game(ctx: commands.Context, game_id: int):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Check if game exists and was scored
                await cursor.execute(
                    "SELECT winning_team, mvp_discord_id, bot_scored FROM games WHERE game_id = %s",
                    (game_id,)
                )
                game_info = await cursor.fetchone()

                if not game_info:
                    await ctx.send(f"Game ID `{game_id}` not found.")
                    return
                
                winning_team, mvp_discord_id, bot_scored = game_info

                if not winning_team:
                    await ctx.send(f"Game ID `{game_id}` has not been scored yet, nothing to undo.")
                    return
                
                # Get all players in the game
                await cursor.execute(
                    "SELECT discord_id, team FROM game_players WHERE game_id = %s",
                    (game_id,)
                )
                players_in_game = await cursor.fetchall()

                # Revert ELO and stats for each player
                for player_id, player_team in players_in_game:
                    elo_change = 0
                    
                    if player_team == winning_team: # This player was on the winning team
                        elo_change = -ADMIN_WIN_ELO_CHANGE # Revert win ELO
                        if mvp_discord_id == player_id:
                            elo_change -= ADMIN_MVP_ELO_CHANGE # Revert MVP ELO
                        # Decrease wins
                        await cursor.execute("UPDATE users SET wins = wins - 1 WHERE discord_id = %s", (player_id,))
                        await update_streak(player_id, False) # This is tricky, a full streak revert might be complex. Reset for simplicity.
                    else: # This player was on the losing team
                        elo_change = -ADMIN_LOSS_ELO_CHANGE # Revert loss ELO (which is positive)
                        # Decrease losses
                        await cursor.execute("UPDATE users SET losses = losses - 1 WHERE discord_id = %s", (player_id,))
                        # No streak change needed for a loss revert as streak would have been reset.

                    await update_player_elo_in_db(player_id, elo_change) # Apply reverted ELO change
                    await update_elo_role(player_id, await get_player_elo(player_id)) # Update ELO role and nickname with actual new ELO

                # Update game status in DB
                await cursor.execute(
                    "UPDATE games SET status = 'picking', winning_team = NULL, mvp_discord_id = NULL, scored_by = NULL, bot_scored = FALSE WHERE game_id = %s",
                    (game_id,)
                )
                await conn.commit()

                await ctx.send(f"Game ID `{game_id}` has been successfully undone. Player stats and ELOs have been reverted.")
                
                log_embed = create_embed(
                    title="Game Undone",
                    description=f"Game ID `{game_id}` was undone by {ctx.author.mention}.",
                    color=discord.Color.orange()
                )
                await send_log_embed(ctx.guild, GAME_LOG_CHANNEL_ID, GAME_LOG_CHANNEL_NAME, log_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while undoing the game: {e}")

@bot.command(name="rescore")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def rescore_game(ctx: commands.Context, game_id: int, winning_team: int, mvp_player: discord.Member):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    # First, undo the game to reset stats
    # We pass ctx.channel and ctx.author to simulate the undo command context
    undo_result = await undo_game(ctx, game_id)
    if not undo_result: # if undo_game failed or didn't proceed
        await ctx.send(f"Rescoring aborted: Failed to undo Game ID `{game_id}` first.")
        return

    # Then, apply the new score
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Check if game exists and is in a state to be scored (status should be 'picking' after undo)
                await cursor.execute(
                    "SELECT status FROM games WHERE game_id = %s",
                    (game_id,)
                )
                game_status_result = await cursor.fetchone()

                if not game_status_result or game_status_result[0] != 'picking':
                    await ctx.send(f"Game ID `{game_id}` is not in a valid state for rescoring after undo (current status: {game_status_result[0]}).")
                    return

                # Get players in the game to apply scores
                await cursor.execute(
                    "SELECT discord_id, team FROM game_players WHERE game_id = %s",
                    (game_id,)
                )
                players_in_game = await cursor.fetchall()

                if not players_in_game:
                    await ctx.send(f"No players found for Game ID `{game_id}`.")
                    return

                # Apply new ELO and stats
                for player_id, player_team in players_in_game:
                    elo_change = 0
                    if player_team == winning_team:
                        elo_change = ADMIN_WIN_ELO_CHANGE
                        if mvp_player.id == player_id:
                            elo_change += ADMIN_MVP_ELO_CHANGE
                        # Increase wins
                        await cursor.execute("UPDATE users SET wins = wins + 1 WHERE discord_id = %s", (player_id,))
                        await update_streak(player_id, True)
                    else:
                        elo_change = ADMIN_LOSS_ELO_CHANGE
                        # Increase losses
                        await cursor.execute("UPDATE users SET losses = losses + 1 WHERE discord_id = %s", (player_id,))
                        await update_streak(player_id, False)

                    await update_player_elo_in_db(player_id, elo_change)
                    current_elo = await get_player_elo(player_id)
                    await update_elo_role(player_id, current_elo)

                # Update game record
                await cursor.execute(
                    "UPDATE games SET status = 'completed', winning_team = %s, mvp_discord_id = %s, scored_by = %s, bot_scored = FALSE WHERE game_id = %s",
                    (winning_team, mvp_player.id, ctx.author.id, game_id)
                )
                await conn.commit()

                await ctx.send(f"Game ID `{game_id}` has been successfully rescored. Team {winning_team} won with {mvp_player.mention} as MVP.")

                log_embed = create_embed(
                    title="Game Rescored",
                    description=f"Game ID `{game_id}` was rescored by {ctx.author.mention}.",
                    color=discord.Color.purple(),
                    fields=[
                        {"name": "Winning Team", "value": str(winning_team), "inline": True},
                        {"name": "MVP", "value": mvp_player.mention, "inline": True}
                    ]
                )
                await send_log_embed(ctx.guild, GAME_LOG_CHANNEL_ID, GAME_LOG_CHANNEL_NAME, log_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while rescoring the game: {e}")
            except Exception as e:
                await ctx.send(f"An unexpected error occurred during rescoring: {e}")


@bot.command(name="score")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def score_game(ctx: commands.Context, game_id: int, winning_team: int, mvp_player: discord.Member):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Check if game exists and is not already scored
                await cursor.execute(
                    "SELECT status FROM games WHERE game_id = %s",
                    (game_id,)
                )
                game_status_result = await cursor.fetchone()

                if not game_status_result:
                    await ctx.send(f"Game ID `{game_id}` not found for scoring.")
                    return
                
                if game_status_result[0] == 'completed':
                    await ctx.send(f"Game ID `{game_id}` is already scored. Use `=rescore` to change the result.")
                    return

                # Get players in the game to apply scores
                await cursor.execute(
                    "SELECT discord_id, team FROM game_players WHERE game_id = %s",
                    (game_id,)
                )
                players_in_game = await cursor.fetchall()

                if not players_in_game:
                    await ctx.send(f"No players found for Game ID `{game_id}`.")
                    return

                # Apply ELO and stats
                for player_id, player_team in players_in_game:
                    elo_change = 0
                    if player_team == winning_team:
                        elo_change = ADMIN_WIN_ELO_CHANGE
                        if mvp_player.id == player_id:
                            elo_change += ADMIN_MVP_ELO_CHANGE
                        # Increase wins
                        await cursor.execute("UPDATE users SET wins = wins + 1 WHERE discord_id = %s", (player_id,))
                        await update_streak(player_id, True)
                    else:
                        elo_change = ADMIN_LOSS_ELO_CHANGE
                        # Increase losses
                        await cursor.execute("UPDATE users SET losses = losses + 1 WHERE discord_id = %s", (player_id,))
                        await update_streak(player_id, False)

                    await update_player_elo_in_db(player_id, elo_change)
                    current_elo = await get_player_elo(player_id)
                    await update_elo_role(player_id, current_elo)

                # Update game record
                await cursor.execute(
                    "UPDATE games SET status = 'completed', winning_team = %s, mvp_discord_id = %s, scored_by = %s, bot_scored = FALSE WHERE game_id = %s",
                    (winning_team, mvp_player.id, ctx.author.id, game_id)
                )
                await conn.commit()

                await ctx.send(f"Game ID `{game_id}` has been successfully scored. Team {winning_team} won with {mvp_player.mention} as MVP.")

                log_embed = create_embed(
                    title="Game Scored",
                    description=f"Game ID `{game_id}` was scored by {ctx.author.mention}.",
                    color=discord.Color.green(),
                    fields=[
                        {"name": "Winning Team", "value": str(winning_team), "inline": True},
                        {"name": "MVP", "value": mvp_player.mention, "inline": True}
                    ]
                )
                await send_log_embed(ctx.guild, GAME_LOG_CHANNEL_ID, GAME_LOG_CHANNEL_NAME, log_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while scoring the game: {e}")
            except Exception as e:
                await ctx.send(f"An unexpected error occurred during scoring: {e}")

@bot.command(name="register")
async def register(ctx: commands.Context, ign: str):
    await ctx.message.add_reaction("⌛")
    if ctx.channel.id != REGISTER_CHANNEL_ID:
        await ctx.send(f"This command can only be used in <#{REGISTER_CHANNEL_ID}>.", ephemeral=True)
        return

    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Check if IGN is already taken
                await cursor.execute(
                    "SELECT discord_id FROM users WHERE minecraft_ign = %s",
                    (ign,)
                )
                if await cursor.fetchone():
                    await ctx.send(f"The Minecraft IGN `{ign}` is already registered to another user.")
                    return

                # Check if user is already registered by Discord ID
                await cursor.execute(
                    "SELECT verified FROM users WHERE discord_id = %s",
                    (ctx.author.id,)
                )
                existing_user = await cursor.fetchone()

                if existing_user and existing_user[0] == 1:
                    await ctx.send(f"You are already registered. Your IGN has been updated to `{ign}`.")
                    await cursor.execute(
                        "UPDATE users SET minecraft_ign = %s WHERE discord_id = %s",
                        (ign, ctx.author.id)
                    )
                else:
                    # Insert new registration or update existing unverified entry
                    await cursor.execute(
                        "INSERT INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE minecraft_ign = VALUES(minecraft_ign), verified = 1",
                        (ctx.author.id, ign, 0, 0, 0, 0, 0, 1) # Set verified to 1 (True)
                    )
                    await ctx.send(f"Congratulations {ctx.author.mention}, you have successfully registered with Minecraft IGN: `{ign}`!")

                await conn.commit()

                # Assign Registered role and remove Unregistered role
                registered_role = get_role_by_name(ctx.guild, REGISTERED_ROLE_NAME)
                unregistered_role = get_role_by_name(ctx.guild, UNREGISTERED_ROLE_NAME)

                if registered_role and registered_role not in ctx.author.roles:
                    try:
                        await ctx.author.add_roles(registered_role, reason="Self-registered.")
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to add {REGISTERED_ROLE_NAME} role to {ctx.author.display_name}.")
                
                if unregistered_role and unregistered_role in ctx.author.roles:
                    try:
                        await ctx.author.remove_roles(unregistered_role, reason="Self-registered.")
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to remove {UNREGISTERED_ROLE_NAME} role from {ctx.author.display_name}.")
                
                await update_elo_role(ctx.author.id, await get_player_elo(ctx.author.id)) # Update ELO role and nickname

                # Log to dedicated registration channel
                log_embed = create_embed(
                    title="New User Registered",
                    description=f"{ctx.author.mention} has registered.",
                    color=discord.Color.green(),
                    fields=[
                        {"name": "User", "value": f"<@{ctx.author.id}> ({ctx.author.id})", "inline": True},
                        {"name": "IGN", "value": ign, "inline": True}
                    ]
                )
                await send_log_embed(ctx.guild, REGISTER_LOG_CHANNEL_ID, REGISTER_LOG_CHANNEL_NAME, log_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred during registration: {e}")
            except Exception as e:
                await ctx.send(f"An unexpected error occurred during registration: {e}")


@bot.command(name="forceregister")
@commands.has_any_role(STAFF_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def force_register(ctx: commands.Context, member: discord.Member, ign: str):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Check if already registered
                await cursor.execute(
                    "SELECT verified FROM users WHERE discord_id = %s",
                    (member.id,)
                )
                result = await cursor.fetchone()
                
                if result and result[0] == 1:
                    await ctx.send(f"{member.mention} is already registered. Updating IGN if different.")
                    await cursor.execute(
                        "UPDATE users SET minecraft_ign = %s WHERE discord_id = %s",
                        (ign, member.id)
                    )
                    await conn.commit()
                    await ctx.send(f"Updated IGN for {member.mention} to `{ign}`.")
                    # Log update to staff updates (as requested, updates go here)
                    log_embed = create_embed(
                        title="User IGN Updated (Force)",
                        description=f"{member.mention}'s IGN was updated to `{ign}` by {ctx.author.mention}.",
                        color=discord.Color.blue(),
                        fields=[
                            {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
                            {"name": "New IGN", "value": ign, "inline": True},
                            {"name": "Updated By", "value": f"<@{ctx.author.id}>", "inline": True}
                        ]
                    )
                    await send_log_embed(ctx.guild, STAFF_UPDATES_CHANNEL_ID, STAFF_UPDATES_CHANNEL_NAME, log_embed)
                    return

                # Insert new entry if not registered
                await cursor.execute(
                    "INSERT INTO users (discord_id, minecraft_ign, elo, wins, losses, mvps, streak, verified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (member.id, ign, 0, 0, 0, 0, 0, 1) # Set verified to 1 (True)
                )
                await conn.commit()

                # Assign Registered role
                registered_role = get_role_by_name(ctx.guild, REGISTERED_ROLE_NAME)
                unregistered_role = get_role_by_name(ctx.guild, UNREGISTERED_ROLE_NAME)

                if registered_role:
                    try:
                        await member.add_roles(registered_role, reason=f"Force registered by {ctx.author.name}")
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to add {REGISTERED_ROLE_NAME} role.")
                if unregistered_role and unregistered_role in member.roles:
                    try:
                        await member.remove_roles(unregistered_role, reason=f"Force registered by {ctx.author.name}")
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to remove {UNREGISTERED_ROLE_NAME} role.")

                await update_elo_role(member.id, 0) # Set initial ELO role (Iron) and nickname

                await ctx.send(f"Successfully force-registered {member.mention} with IGN: `{ign}`.")
                # Log new force-registration to dedicated registration channel
                log_embed = create_embed(
                    title="User Force Registered",
                    description=f"{member.mention} has been force-registered by {ctx.author.mention}.",
                    color=discord.Color.blue(),
                    fields=[
                        {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
                        {"name": "IGN", "value": ign, "inline": True},
                        {"name": "Registered By", "value": f"<@{ctx.author.id}>", "inline": True}
                    ]
                )
                await send_log_embed(ctx.guild, REGISTER_LOG_CHANNEL_ID, REGISTER_LOG_CHANNEL_NAME, log_embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred during force registration: {e}")


# Screenshare Command
@bot.command(name="ss")
@commands.has_any_role(MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def screenshare_command(ctx: commands.Context, member: discord.Member, *, reason: str):
    await ctx.message.add_reaction("⌛") # Add reaction
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
        description=f"{member.mention} has been requested for a screenshare by {ctx.author.mention}.\nReason: {reason}",
        color=discord.Color.orange(),
        fields=[
            {"name": "Instructions", "value": "A screensharing team member will claim this ticket shortly. Please be ready to screenshare. Failure to comply may result in a ban.", "inline": False}
        ]
    )
    
    # Forward the attached image to the new ticket channel
    if ctx.message.attachments:
        attached_file = await ctx.message.attachments[0].to_file()
        sent_message = await ticket_channel.send(file=attached_file, embed=embed, view=view)
        # We need to set the message reference for the view after it's sent
        view.message = sent_message
    else:
        # Fallback if no attachment, though the check above should prevent this
        sent_message = await ticket_channel.send(embed=embed, view=view)
        view.message = sent_message # Still set message for timeout


    active_screenshare_tickets[ticket_channel.id] = view

    await ctx.send(f"Screenshare ticket created: {ticket_channel.mention}", ephemeral=True)

    log_content = {
        "User": f"<@{member.id}> ({member.id})",
        "Requested By": f"<@{ctx.author.id}> ({ctx.author.id})",
        "Reason": reason,
        "Ticket Channel": ticket_channel.mention,
        "Ticket Channel ID": ticket_channel.id
    }
    await send_log_html(ctx.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, "Screenshare Ticket Created", log_content)

# Prefix command for creating tickets (now works)
@bot.command(name="createticket")
@commands.has_permissions(send_messages=True) # Basic permission
async def create_ticket_prefix(ctx: commands.Context, *, topic: str):
    await ctx.message.add_reaction("⌛")
    guild = ctx.guild
    member = ctx.author

    ticket_category = await get_channel_or_create_category(guild, TICKET_CATEGORY_ID, "Tickets", is_category=True)
    if not ticket_category:
        await ctx.send("Error: Could not find or create a category for tickets. Please contact an administrator.", ephemeral=True)
        return

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        member: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }

    # Add staff roles to overwrites
    for role_name in [MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME, STAFF_ROLE_NAME]:
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
    sent_message = await ticket_channel.send(embed=embed, view=view)
    view.message = sent_message # Store message reference for the view

    await ctx.send(f"Your ticket has been created: {ticket_channel.mention}", ephemeral=True)

    log_content = {
        "User": f"<@{member.id}> ({member.id})",
        "Topic": topic,
        "Ticket Channel": ticket_channel.mention,
        "Ticket Channel ID": ticket_channel.id
    }
    await send_log_html(guild, TICKET_LOG_CHANNEL_ID, TICKET_LOG_CHANNEL_NAME, "Ticket Created", log_content)


# New command for deleting ticket channels
@bot.command(name="delete")
@commands.has_any_role(MANAGER_ROLE_NAME, PI_ROLE_NAME) # Only Manager+
async def delete_ticket_channel(ctx: commands.Context, *, reason: str = "No reason provided."):
    await ctx.message.add_reaction("⌛") # Add reaction
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

@bot.command(name="strikerequest", aliases=["sr"])
async def strike_request_command(ctx: commands.Context, member: discord.Member, *, reason: str):
    await ctx.message.add_reaction("⌛")
    if ctx.channel.id != STRIKE_REQUEST_CHANNEL_ID:
        await ctx.send(f"This command can only be used in <#{STRIKE_REQUEST_CHANNEL_ID}>.", ephemeral=True)
        return
    
    if not ctx.message.attachments:
        await ctx.send("Please attach proof (an image) for the strike request.", ephemeral=True)
        return

    # Create ticket channel under the strike requests category
    sr_category = await get_channel_or_create_category(ctx.guild, STRIKE_REQUESTS_CATEGORY_ID, "Strike Requests", is_category=True)
    if not sr_category:
        await ctx.send("Error: Could not find or create a category for strike requests. Please contact an administrator.", ephemeral=True)
        return

    overwrites = {
        ctx.guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        # Allow staff to see and vote
        get_role_by_name(ctx.guild, MODERATOR_ROLE_NAME): discord.PermissionOverwrite(read_messages=True, send_messages=False),
        get_role_by_name(ctx.guild, ADMIN_STAFF_ROLE_NAME): discord.PermissionOverwrite(read_messages=True, send_messages=False),
        get_role_by_name(ctx.guild, MANAGER_ROLE_NAME): discord.PermissionOverwrite(read_messages=True, send_messages=False), # Standardized role name
        get_role_by_name(ctx.guild, PI_ROLE_NAME): discord.PermissionOverwrite(read_messages=True, send_messages=False)
    }

    ticket_channel = await ctx.guild.create_text_channel(
        f"strike-req-{member.name}",
        category=sr_category,
        overwrites=overwrites,
        topic=f"Strike request for {member.display_name} by {ctx.author.name}"
    )

    view = StrikeRequestView(member, reason, ctx.author)
    embed = create_embed(
        title=f"Strike Request for {member.display_name}",
        description=f"Reason: {reason}\nRequested by: {ctx.author.mention}\n\nProof:",
        color=discord.Color.orange(),
        fields=[
            {"name": "👍 Votes", "value": "0", "inline": True},
            {"name": "👎 Votes", "value": "0", "inline": True},
            {"name": "Status", "value": "Awaiting votes...", "inline": False}
        ]
    )
    
    # Forward the attached image to the new ticket channel
    if ctx.message.attachments:
        attached_file = await ctx.message.attachments[0].to_file()
        sent_message = await ticket_channel.send(file=attached_file, embed=embed, view=view)
        # Store message for view timeout updates
        view.message = sent_message
    else:
        # Fallback if no attachment, though the check above should prevent this
        sent_message = await ticket_channel.send(embed=embed, view=view)
        view.message = sent_message

    active_strike_requests[ticket_channel.id] = view

    await ctx.send(f"Strike request ticket created: {ticket_channel.mention}", ephemeral=True)

    log_embed = create_embed(
        title="Strike Request Created (Log)",
        description=f"A strike request ticket for {member.mention} has been created by {ctx.author.mention}.",
        color=discord.Color.orange(),
        fields=[
            {"name": "User", "value": f"<@{member.id}> ({member.id})", "inline": True},
            {"name": "Requested By", "value": f"<@{ctx.author.id}> ({ctx.author.id})", "inline": True},
            {"name": "Reason", "value": reason, "inline": False},
            {"name": "Ticket Channel", "value": ticket_channel.mention, "inline": True}
        ]
    )
    await send_log_embed(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME, log_embed) # Send to strike logs channel

@bot.command(name="history", aliases=["h"])
async def show_history(ctx: commands.Context, member: Optional[discord.Member] = None):
    await ctx.message.add_reaction("⌛")
    member = member or ctx.author
    if db_pool is None:
        await ctx.send("Database not connected.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            try:
                history_data = {}
                
                # Fetch strikes
                await cursor.execute(
                    "SELECT strike_id, reason, issued_by, strike_date FROM strikes WHERE discord_id = %s ORDER BY strike_date DESC",
                    (member.id,)
                )
                history_data['strikes'] = await cursor.fetchall()

                # Fetch bans
                await cursor.execute(
                    "SELECT ban_id, reason, issued_by, issued_at, expires_at, active FROM bans WHERE discord_id = %s ORDER BY issued_at DESC",
                    (member.id,)
                )
                history_data['bans'] = await cursor.fetchall()

                # Fetch mutes
                await cursor.execute(
                    "SELECT mute_id, reason, issued_by, issued_at, expires_at, active FROM mutes WHERE discord_id = %s ORDER BY issued_at DESC",
                    (member.id,)
                )
                history_data['mutes'] = await cursor.fetchall()

                if not any(history_data.values()):
                    await ctx.send(f"No history found for {member.display_name}.")
                    return

                # Create pages for embeds
                pages = []

                # Strikes Page
                if history_data['strikes']:
                    strike_desc = ""
                    for strike in history_data['strikes']:
                        strike_desc += (
                            f"**ID:** `{strike['strike_id']}`\n"
                            f"**Reason:** {strike['reason']}\n"
                            f"**Issued By:** <@{strike['issued_by']}>\n"
                            f"**Date:** {strike['strike_date'].strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                        )
                    pages.append(create_embed(
                        title=f"{member.display_name}'s Strike History",
                        description=strike_desc,
                        color=discord.Color.red()
                    ))
                
                # Bans Page
                if history_data['bans']:
                    ban_desc = ""
                    for ban in history_data['bans']:
                        status = "Active" if ban['active'] else "Expired/Removed"
                        expires = ban['expires_at'].strftime('%Y-%m-%d %H:%M:%S') if ban['expires_at'] else "Permanent"
                        ban_desc += (
                            f"**ID:** `{ban['ban_id']}`\n"
                            f"**Status:** {status}\n"
                            f"**Reason:** {ban['reason']}\n"
                            f"**Issued By:** <@{ban['issued_by']}>\n"
                            f"**Issued On:** {ban['issued_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"**Expires On:** {expires}\n\n"
                        )
                    pages.append(create_embed(
                        title=f"{member.display_name}'s Ban History",
                        description=ban_desc,
                        color=discord.Color.dark_red()
                    ))

                # Mutes Page
                if history_data['mutes']:
                    mute_desc = ""
                    for mute in history_data['mutes']:
                        status = "Active" if mute['active'] else "Expired/Removed"
                        expires = mute['expires_at'].strftime('%Y-%m-%d %H:%M:%S') if mute['expires_at'] else "Permanent"
                        mute_desc += (
                            f"**ID:** `{mute['mute_id']}`\n"
                            f"**Status:** {status}\n"
                            f"**Reason:** {mute['reason']}\n"
                            f"**Issued By:** <@{mute['issued_by']}>\n"
                            f"**Issued On:** {mute['issued_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"**Expires On:** {expires}\n\n"
                        )
                    pages.append(create_embed(
                        title=f"{member.display_name}'s Mute History",
                        description=mute_desc,
                        color=discord.Color.orange()
                    ))
                
                if not pages:
                    await ctx.send(f"No disciplinary history found for {member.display_name}.")
                    return

                # Pagination logic for embeds
                current_page = 0
                message = await ctx.send(embed=pages[current_page])

                if len(pages) > 1:
                    await message.add_reaction("⬅️")
                    await message.add_reaction("➡️")

                    def check(reaction, user):
                        return user == ctx.author and str(reaction.emoji) in ["⬅️", "➡️"] and reaction.message.id == message.id

                    while True:
                        try:
                            reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)

                            if str(reaction.emoji) == "➡️":
                                current_page = (current_page + 1) % len(pages)
                            elif str(reaction.emoji) == "⬅️":
                                current_page = (current_page - 1) % len(pages)
                            
                            await message.edit(embed=pages[current_page])
                            await message.remove_reaction(reaction, user)
                        except asyncio.TimeoutError:
                            await message.clear_reactions()
                            break
                        except Exception as e:
                            print(f"Error handling reaction: {e}")
                            break

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while fetching history: {e}")


# Leaderboard commands
@bot.command(name="lb", aliases=["leaderboard"])
async def show_leaderboard(ctx: commands.Context, stat_type: str = "elo"):
    await ctx.message.add_reaction("⌛")
    if db_pool is None:
        await ctx.send("Database not connected.")
        return
    
    stat_type = stat_type.lower()
    valid_stats = {
        "wins": "wins", 
        "losses": "losses", 
        "elo": "elo", 
        "mvps": "mvps", 
        "streak": "streak",
        "games": "wins + losses" # Calculate total games played
    }
    
    if stat_type not in valid_stats:
        await ctx.send(f"Invalid leaderboard type. Choose from: {', '.join(valid_stats.keys())}")
        return

    order_by_column = valid_stats[stat_type]
    
    # Determine sorting order (ascending for losses, descending for others)
    order_direction = "ASC" if stat_type == "losses" else "DESC"

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            try:
                # For games, we need to calculate it first
                if stat_type == "games":
                    await cursor.execute(
                        f"SELECT discord_id, minecraft_ign, wins, losses, (wins + losses) AS total_games FROM users ORDER BY total_games {order_direction} LIMIT 10"
                    )
                else:
                    await cursor.execute(
                        f"SELECT discord_id, minecraft_ign, {order_by_column} FROM users ORDER BY {order_by_column} {order_direction} LIMIT 10"
                    )
                
                results = await cursor.fetchall()

                if not results:
                    await ctx.send(f"No data available for {stat_type} leaderboard.")
                    return

                leaderboard_text = ""
                for i, row in enumerate(results):
                    player_name = row['minecraft_ign'] or f"Unknown User ({row['discord_id']})"
                    if stat_type == "games":
                        value = row['total_games']
                    else:
                        value = row[stat_type] # Direct access for other stats
                    
                    leaderboard_text += f"{i+1}. {player_name}: **{value}** {stat_type.capitalize()}\n"

                embed = create_embed(
                    title=f"Top 10 {stat_type.capitalize()} Leaderboard",
                    description=leaderboard_text,
                    color=discord.Color.gold()
                )
                embed.set_footer(text="asrbw.fun")
                await ctx.send(embed=embed)

            except aiomysql.Error as e:
                await ctx.send(f"An error occurred while fetching the leaderboard: {e}")


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
    for role_name in [MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME, STAFF_ROLE_NAME]:
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
    sent_message = await ticket_channel.send(embed=embed, view=view)
    view.message = sent_message # Store message reference for the view

    await interaction.followup.send(f"Your ticket has been created: {ticket_channel.mention}")

    log_content = {
        "User": f"<@{member.id}> ({member.id})",
        "Topic": topic,
        "Ticket Channel": ticket_channel.mention,
        "Ticket Channel ID": ticket_channel.id
    }
    await send_log_html(guild, TICKET_LOG_CHANNEL_ID, TICKET_LOG_CHANNEL_NAME, "Ticket Created", log_content)


@bot.tree.command(name="addusertoticket", description="Add a user to the current ticket.")
@app_commands.describe(user="The user to add.")
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
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
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
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
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
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
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
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

