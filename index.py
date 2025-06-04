import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import mysql.connector
from mysql.connector import Error
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
GAME_CATEGORY_ID = None  # ID of your "Games" category
VOICE_CATEGORY_ID = None # ID of your "Voice Channels" category
TICKET_CATEGORY_ID = None # ID of your "Tickets" category
CLOSED_TICKETS_CATEGORY_ID = None # ID of your "Closed Tickets" category
STRIKE_REQUESTS_CATEGORY_ID = None # ID of your "Strike Requests" category

# Channel IDs (Optional, but recommended for specific channels)
REGISTER_CHANNEL_ID = None # ID of your registration channel
BAN_LOG_CHANNEL_ID = None # ID of your ban logs channel
MUTE_LOG_CHANNEL_ID = None # ID of your mute logs channel
STRIKE_LOG_CHANNEL_ID = None # ID of your strike logs channel
TICKET_CHANNEL_ID = None # ID of the channel where users create tickets
TICKET_LOG_CHANNEL_ID = None # ID of your ticket logs channel
STRIKE_REQUEST_CHANNEL_ID = None # ID of the channel where users make strike requests
SCREENSNARE_LOG_CHANNEL_ID = None # ID of your screenshare ticket logs channel
GAME_LOG_CHANNEL_ID = None # ID of your game logs channel
PPP_VOTING_CHANNEL_ID = None # ID of your #ppp-voting channel
STAFF_UPDATES_CHANNEL_ID = None # ID of your staff-updates channel
GAMES_DISPLAY_CHANNEL_ID = None # ID of the channel to display game results image
AFK_VOICE_CHANNEL_ID = None # ID of your AFK voice channel

# Channel Names (Fallback if IDs are None or channel not found by ID)
REGISTER_CHANNEL_NAME = "register"
BAN_LOG_CHANNEL_NAME = "ban-logs"
MUTE_LOG_CHANNEL_NAME = "mute-logs"
STRIKE_LOG_CHANNEL_NAME = "strike-logs"
TICKET_CHANNEL_NAME = "tickets"
TICKET_LOG_CHANNEL_NAME = "ticket-logs"
STRIKE_REQUEST_CHANNEL_NAME = "strike-requests"
SCREENSNARE_LOG_CHANNEL_NAME = "screenshare-ticket-logs"
GAME_LOG_CHANNEL_NAME = "game-logs"
PPP_VOTING_CHANNEL_NAME = "ppp-voting"
STAFF_UPDATES_CHANNEL_NAME = "staff-updates"
GAMES_DISPLAY_CHANNEL_NAME = "games"

# Role Names (Used for permissions and role management)
REGISTERED_ROLE_NAME = "Registered" # Role assigned upon successful registration
UNREGISTERED_ROLE_NAME = "Unregistered" # Role for new members, removed upon registration
BANNED_ROLE_NAME = "Banned"
MUTED_ROLE_NAME = "Muted"
FROZEN_ROLE_NAME = "Frozen" # Role assigned during screenshare
PPP_MANAGER_ROLE_NAME = "PPP Manager" # Role for poll command (Pups, Pugs, Premium)
MANAGER_ROLE_NAME = "Manager" # Role for modify stats command (and above)
ADMIN_STAFF_ROLE_NAME = "Admin Staff" # Role for game commands (and above)
STAFF_ROLE_NAME = "Staff" # Role for force register command, ticket claim (and above)
MODERATOR_ROLE_NAME = "Moderator" # Base role for staff commands (e.g., ban, mute, strike)
PI_ROLE_NAME = "PI" # Role for admin commands
SCREENSHARING_TEAM_ROLE_NAME = "Screensharing Team" # Role for screenshare ticket access

# Database connection details
DB_HOST = "localhost"
DB_USER = "your_username"
DB_PASSWORD = "your_password"
DB_NAME = "queue_bot_db"

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

ELO_REWARDS = {
    "Iron": {"win": 25, "loss": 10, "mvp": 20},
    "Bronze": {"win": 20, "loss": 10, "mvp": 15},
    "Silver": {"win": 20, "loss": 10, "mvp": 10},
    "Gold": {"win": 15, "loss": 10, "mvp": 10},
    "Topaz": {"win": 10, "loss": 15, "mvp": 10},
    "Platinum": {"win": 5, "loss": 20, "mvp": 10}
}

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


# --- Database Connection ---
def create_db_connection():
    """Establishes a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

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
    embed.set_footer(text="asrbw.net")
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

async def log_to_html_channel(guild: discord.Guild, channel_id: Optional[int], channel_name: str, title: str, content_html: str):
    """Sends an HTML file containing logs to a specified Discord channel."""
    log_channel = await get_channel_by_config(guild, channel_id, channel_name)
    if not log_channel or not isinstance(log_channel, discord.TextChannel):
        print(f"Warning: Log channel '{channel_name}' (ID: {channel_id}) not found or is not a text channel.")
        return

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{title}</title>
        <style>
            body {{ font-family: sans-serif; background-color: #36393f; color: #dcddde; }}
            .container {{ margin: 20px; padding: 20px; background-color: #2f3136; border-radius: 8px; }}
            h1 {{ color: #ffffff; }}
            pre {{ background-color: #202225; padding: 10px; border-radius: 4px; overflow-x: auto; }}
            p {{ margin-bottom: 5px; }}
            strong {{ color: #ffffff; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>{title}</h1>
            {content_html}
        </div>
    </body>
    </html>
    """
    
    file_name = f"{title.replace(' ', '_').lower()}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    file = discord.File(io.BytesIO(html_content.encode('utf-8')), filename=file_name)
    
    try:
        await log_channel.send(file=file)
    except discord.Forbidden:
        print(f"Bot lacks permissions to send files in {log_channel.name}.")
    except Exception as e:
        print(f"Error sending HTML log to {log_channel.name}: {e}")

# --- ELO and Role Management ---
async def get_player_elo(player_id: int) -> int:
    """Retrieves a player's ELO from the database."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT elo FROM users WHERE discord_id = %s",
                (player_id,)
            )
            result = cursor.fetchone()
            return result[0] if result else 0
        except Error as e:
            print(f"Error getting player ELO: {e}")
            return 0
        finally:
            connection.close()
    return 0

async def update_player_elo_in_db(player_id: int, elo_change: int) -> bool:
    """Updates a player's ELO in the database."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE users SET elo = elo + %s WHERE discord_id = %s",
                (elo_change, player_id)
            )
            connection.commit()
            return True
        except Error as e:
            print(f"Error updating player ELO in DB: {e}")
            return False
        finally:
            connection.close()
    return False

async def update_streak(player_id: int, won: bool):
    """Updates a player's win/loss streak."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            if won:
                cursor.execute(
                    "UPDATE users SET streak = streak + 1 WHERE discord_id = %s",
                    (player_id,)
                )
            else:
                cursor.execute(
                    "UPDATE users SET streak = 0 WHERE discord_id = %s",
                    (player_id,)
                )
            connection.commit()
        except Error as e:
            print(f"Error updating streak: {e}")
        finally:
            connection.close()

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
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT minecraft_ign FROM users WHERE discord_id = %s",
                (player_id,)
            )
            ign = cursor.fetchone()
            if ign:
                ign = ign[0]
                try:
                    await member.edit(nick=f"[{new_elo}] {ign}")
                except discord.Forbidden:
                    print(f"Bot lacks permissions to change nickname for {member.display_name}")
                except Exception as e:
                    print(f"Error changing nickname: {e}")
        except Error as e:
            print(f"Error getting IGN for nickname update: {e}")
        finally:
            connection.close()

async def get_user_ign(discord_id: int) -> Optional[str]:
    """Fetches a user's Minecraft IGN from the database."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT minecraft_ign FROM users WHERE discord_id = %s", (discord_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Error as e:
            print(f"Error fetching IGN: {e}")
            return None
        finally:
            connection.close()
    return None

async def is_registered(discord_id: int) -> bool:
    """Checks if a Discord user is registered in the database."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT verified FROM users WHERE discord_id = %s",
                (discord_id,)
            )
            result = cursor.fetchone()
            return result[0] if result else False
        except Error as e:
            print(f"Error checking registration: {e}")
            return False
        finally:
            connection.close()
    return False

async def is_banned(discord_id: int) -> bool:
    """Checks if a Discord user is currently banned."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT 1 FROM bans WHERE discord_id = %s AND active = TRUE AND (expires_at IS NULL OR expires_at > NOW())",
                (discord_id,)
            )
            return cursor.fetchone() is not None
        except Error as e:
            print(f"Error checking ban: {e}")
            return False
        finally:
            connection.close()
    return False

# --- Game Management Functions ---
async def get_game_data_from_db(game_id: int) -> Optional[Dict[str, Any]]:
    """Fetches comprehensive game data from the database."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
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
            game_data = cursor.fetchone()
            if game_data:
                # Convert comma-separated strings to lists of integers
                game_data['team1_players'] = [int(p) for p in game_data['team1_players'].split(',')] if game_data['team1_players'] else []
                game_data['team2_players'] = [int(p) for p in game_data['team2_players'].split(',')] if game_data['team2_players'] else []
            return game_data
        except Error as e:
            print(f"Error fetching game data from DB: {e}")
            return None
        finally:
            connection.close()
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
    """Periodically checks and ensures database connection is active."""
    connection = create_db_connection()
    if connection:
        connection.close()
        # print("Database connection checked and closed.") # For debugging

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
                color = discord.Color.blue()

            # Remove players from queue
            queues[queue_type] = queues[queue_type][required_players:]
            
            # Add game to active games
            active_games[game_counter] = {
                "channel_id": game_channel.id,
                "voice_channel_id": voice_channel.id,
                "players": players_in_queue, # All players initially in the game
                "queue_type": queue_type,
                "status": "queuing",
                "teams": teams,
                "captains": captains,
                "current_picker": captains[0] if captains else None, # For captain picking
                "picking_turn": 1 # 1 for team1, 2 for team2
            }
            
            # Add game to database
            connection = create_db_connection()
            if connection:
                try:
                    cursor = connection.cursor()
                    cursor.execute(
                        "INSERT INTO games (queue_type, status, channel_id, voice_channel_id) VALUES (%s, %s, %s, %s)",
                        (queue_type, "queuing", game_channel.id, voice_channel.id)
                    )
                    db_game_id = cursor.lastrowid
                    
                    for player_id in players_in_queue:
                        team_assigned = None
                        is_captain = False
                        if player_id in teams[1]:
                            team_assigned = 1
                        elif player_id in teams[2]:
                            team_assigned = 2
                        
                        if player_id in captains:
                            is_captain = True

                        cursor.execute(
                            "INSERT INTO game_players (game_id, discord_id, team, is_captain) VALUES (%s, %s, %s, %s)",
                            (db_game_id, player_id, team_assigned, is_captain)
                        )
                    
                    connection.commit()
                    active_games[game_counter]["db_game_id"] = db_game_id # Store DB game_id
                except Error as e:
                    print(f"Error adding game to database: {e}")
                finally:
                    connection.close()
            
            # Send initial message
            embed = create_embed(
                title=f"Game #{game_counter:04d} - {queue_type}",
                description=description,
                color=color,
                fields=[
                    {"name": "Voice Channel", "value": voice_channel.mention, "inline": True},
                    {"name": "Text Channel", "value": game_channel.mention, "inline": True}
                ]
            )
            
            await game_channel.send(embed=embed)
            
            if party_size is None: # Only start picking phase if not party season
                await asyncio.sleep(5) # Give players a moment to see the channel
                await start_picking_phase(game_counter, game_channel, players_in_queue, queue_type)
            else: # Directly start game if party season (teams already decided)
                await asyncio.sleep(5)
                await start_game(game_counter, game_channel)
            
            game_counter += 1

async def handle_pick(message: discord.Message, game_id: int, game_data: Dict[str, Any]):
    """Handles player picking during the team selection phase."""
    current_picker_id = game_data["current_picker"]
    
    if message.author.id != current_picker_id:
        return # Not the current picker's turn
    
    # Determine picking team
    picking_team = game_data["picking_turn"]

    # Check if mentioned a valid player
    if not message.mentions:
        await message.channel.send(embed=create_embed("Invalid Pick", "Please mention a player to pick!", discord.Color.orange()))
        return
    
    picked_player_id = message.mentions[0].id
    
    # Get all players who are not yet on a team
    players_not_on_team = [p for p in game_data["players"] if p not in game_data["teams"][1] + game_data["teams"][2]]
    
    if picked_player_id not in players_not_on_team:
        await message.channel.send(embed=create_embed("Invalid Player", "That player is not available to pick or is already on a team!", discord.Color.orange()))
        return
    
    # Add player to team
    game_data["teams"][picking_team].append(picked_player_id)
    
    # Update database
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE game_players SET team = %s WHERE game_id = %s AND discord_id = %s",
                (picking_team, game_data["db_game_id"], picked_player_id)
            )
            connection.commit()
        except Error as e:
            print(f"Error updating team: {e}")
        finally:
            connection.close()
    
    required_players = QUEUE_TYPES[game_data["queue_type"]]
    
    # Update picking turn
    if picking_team == 1:
        game_data["picking_turn"] = 2
        game_data["current_picker"] = game_data["captains"][1] # Team 2 captain
    else:
        game_data["picking_turn"] = 1
        game_data["current_picker"] = game_data["captains"][0] # Team 1 captain

    # Check if all players have been picked
    if len(game_data["teams"][1]) + len(game_data["teams"][2]) == required_players:
        await start_game(game_id, message.channel)
        return
    
    # Send updated teams
    embed = create_embed(
        title=f"Game #{game_id:04d} - Teams Updated",
        description="Here are the current teams:",
        color=discord.Color.green(),
        fields=[
            {"name": "Team 1", "value": "\n".join([f"<@{p}>" for p in game_data["teams"][1]]), "inline": True},
            {"name": "Team 2", "value": "\n".join([f"<@{p}>" for p in game_data["teams"][2]]), "inline": True}
        ]
    )
    
    await message.channel.send(embed=embed)
    await message.channel.send(f"<@{game_data['current_picker']}>, it's your turn to pick!")

async def start_picking_phase(game_id: int, channel: discord.TextChannel, players: List[int], queue_type: str):
    """Initiates the team picking phase for a game."""
    if game_id not in active_games:
        return
    
    game_data = active_games[game_id]
    game_data["status"] = "picking"
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE games SET status = 'picking' WHERE game_id = %s",
                (game_data["db_game_id"],)
            )
            connection.commit()
        except Error as e:
            print(f"Error updating game status: {e}")
        finally:
            connection.close()
    
    captain1, captain2 = game_data["captains"]
    remaining_players = [p for p in players if p not in game_data["teams"][1] + game_data["teams"][2]]
    
    embed = create_embed(
        title=f"Game #{game_id:04d} - Team Selection",
        description="Time to pick teams!",
        color=discord.Color.gold(),
        fields=[
            {"name": "Team 1 Captain", "value": f"<@{captain1}>", "inline": True},
            {"name": "Team 2 Captain", "value": f"<@{captain2}>", "inline": True},
            {"name": "Available Players", "value": "\n".join([f"<@{p}>" for p in remaining_players]) or "None", "inline": False}
        ]
    )
    
    await channel.send(embed=embed)
    await channel.send(f"<@{captain1}>, please pick a player by mentioning them!")

async def start_game(game_id: int, channel: discord.TextChannel):
    """Transitions a game to 'in_progress' status and simulates Minecraft integration."""
    if game_id not in active_games:
        return
    
    game_data = active_games[game_id]
    game_data["status"] = "in_progress"
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE games SET status = 'in_progress' WHERE game_id = %s",
                (game_data["db_game_id"],)
            )
            connection.commit()
        except Error as e:
            print(f"Error updating game status: {e}")
        finally:
            connection.close()
    
    embed = create_embed(
        title=f"Game #{game_id:04d} - Starting!",
        description="Teams are set! The game will now start in Minecraft. Good luck!",
        color=discord.Color.green(),
        fields=[
            {"name": "Team 1", "value": "\n".join([f"<@{p}>" for p in game_data["teams"][1]]), "inline": True},
            {"name": "Team 2", "value": "\n".join([f"<@{p}>" for p in game_data["teams"][2]]), "inline": True}
        ]
    )
    
    await channel.send(embed=embed)
    
    # --- Integration with Minecraft Server (Conceptual) ---
    # At this point, the Discord bot would ideally send a message/API call to the Minecraft server plugin.
    # This message would contain:
    # - game_id (from active_games["db_game_id"])
    # - queue_type (e.g., "3v3")
    # - teams (player IDs for Team 1 and Team 2)
    # - indication if it's a party game (for auto-teaming)
    # The Minecraft plugin would then:
    # 1. Select a random map from its pool (e.g., "invasion", "archway", "lectus", "antenna").
    # 2. Warp all players to the game.
    # 3. Auto-team them according to the provided teams.
    # 4. Start the Bedwars game.
    # 5. Monitor kills to determine MVP.
    # 6. Send game results back to the Discord bot (e.g., via a webhook or a command like `=score`).
    
    # For demonstration, we'll simulate game completion after a delay.
    await asyncio.sleep(30) # Simulate game duration
    
    # Simulate game completion and results from Minecraft server
    winning_team = random.randint(1, 2)
    mvp_player = random.choice(game_data["teams"][winning_team])
    
    # Call the end_game function as if results came from Minecraft server
    await end_game(game_id, winning_team, mvp_player, scored_by_bot=True)

async def end_game(game_id: int, winning_team: int, mvp_player_id: int, scored_by_bot: bool = True):
    """Processes game results, updates stats, and cleans up game channels."""
    if game_id not in active_games:
        print(f"Game {game_id} not found in active games for ending.")
        return
    
    game_data = active_games[game_id]
    channel = bot.get_channel(game_data["channel_id"])
    guild = channel.guild if channel else bot.guilds[0] # Fallback to first guild

    # Update status
    game_data["status"] = "completed"
    game_data["winning_team"] = winning_team
    game_data["mvp_player"] = mvp_player_id
    
    # Update database
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            scored_by_str = "bot" if scored_by_bot else str(bot.user.id) # If manually scored, use bot's ID
            cursor.execute(
                "UPDATE games SET status = 'completed', winning_team = %s, mvp_player = %s, completed_at = NOW(), scored_by = %s WHERE game_id = %s",
                (winning_team, mvp_player_id, scored_by_str, game_data["db_game_id"])
            )
            
            # Update player stats and ELO
            # Get all players involved in the game
            all_game_players = game_data["teams"][1] + game_data["teams"][2]

            for player_id in all_game_players:
                current_elo = await get_player_elo(player_id)
                elo_role_name = await get_elo_role_name(current_elo)
                
                elo_change = 0
                # Check if the player was in the winning team
                if (player_id in game_data["teams"][winning_team]):
                    elo_change = ELO_REWARDS[elo_role_name]["win"]
                    cursor.execute(
                        "UPDATE users SET wins = wins + 1, last_played = NOW() WHERE discord_id = %s",
                        (player_id,)
                    )
                    await update_streak(player_id, True)
                else:
                    elo_change = -ELO_REWARDS[elo_role_name]["loss"]
                    cursor.execute(
                        "UPDATE users SET losses = losses + 1, last_played = NOW() WHERE discord_id = %s",
                        (player_id,)
                    )
                    await update_streak(player_id, False)
                
                if player_id == mvp_player_id:
                    elo_change += ELO_REWARDS[elo_role_name]["mvp"]
                    cursor.execute(
                        "UPDATE users SET mvps = mvps + 1 WHERE discord_id = %s",
                        (player_id,)
                    )
                
                # Apply ELO change
                cursor.execute(
                    "UPDATE users SET elo = elo + %s WHERE discord_id = %s",
                    (elo_change, player_id)
                )
            
            connection.commit()
        except Error as e:
            print(f"Error updating game completion or ELO: {e}")
            if channel: await channel.send(embed=create_embed("Database Error", "An error occurred while processing game results.", discord.Color.red()))
        finally:
            connection.close()
    
    # Generate game results image (monochrome)
    try:
        game_image_file = await generate_game_results_image(game_data, winning_team, mvp_player_id)
        games_display_channel = await get_channel_by_config(guild, GAMES_DISPLAY_CHANNEL_ID, GAMES_DISPLAY_CHANNEL_NAME)
        if games_display_channel and isinstance(games_display_channel, discord.TextChannel):
            await games_display_channel.send(file=game_image_file)
    except Exception as e:
        print(f"Error generating game results image: {e}")

    # Send game result embed to game channel
    embed = create_embed(
        title=f"Game #{game_data['db_game_id']:04d} - Results",
        description=f"Team {winning_team} wins!",
        color=discord.Color.gold(),
        fields=[
            {"name": f"Team {winning_team} (Winners)", "value": "\n".join([f"<@{p}>" for p in game_data["teams"][winning_team]]), "inline": True},
            {"name": f"Team {2 if winning_team == 1 else 1}", "value": "\n".join([f"<@{p}>" for p in game_data["teams"][2 if winning_team == 1 else 1]]), "inline": True},
            {"name": "MVP", "value": f"<@{mvp_player_id}>", "inline": False}
        ]
    )
    
    if channel: await channel.send(embed=embed)
    
    # Log game chat to HTML
    if channel:
        messages = [msg async for msg in channel.history(limit=200)] # Fetch last 200 messages
        messages.reverse() # Oldest first
        
        chat_html_content = "<h2>Game Chat Log</h2>"
        for msg in messages:
            chat_html_content += f"<p><strong>{html.escape(str(msg.author.display_name))} ({msg.created_at.strftime('%Y-%m-%d %H:%M:%S')}):</strong> {html.escape(msg.content)}</p>"
        
        await log_to_html_channel(guild, GAME_LOG_CHANNEL_ID, GAME_LOG_CHANNEL_NAME, f"Game {game_data['db_game_id']} Chat Log", chat_html_content)

    # Clean up after delay
    await asyncio.sleep(300)  # 5 minutes
    await cleanup_game(game_id)

@tasks.loop(minutes=1)
async def check_expired_punishments():
    """Checks for and removes expired bans and mutes."""
    guild = bot.guilds[0] # Assuming bot operates in a single guild
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Check expired bans
            cursor.execute(
                "SELECT discord_id, ban_id FROM bans WHERE active = TRUE AND expires_at <= NOW()"
            )
            expired_bans = cursor.fetchall()
            
            for ban in expired_bans:
                cursor.execute(
                    "UPDATE bans SET active = FALSE, removed_at = NOW(), removed_reason = 'Expired' WHERE ban_id = %s",
                    (ban["ban_id"],)
                )
                member = guild.get_member(ban["discord_id"])
                if member:
                    banned_role = get_role_by_name(guild, BANNED_ROLE_NAME)
                    if banned_role and banned_role in member.roles:
                        try:
                            await member.remove_roles(banned_role)
                            print(f"Removed expired banned role from {member.display_name}")
                        except discord.Forbidden:
                            print(f"Bot lacks permissions to remove {BANNED_ROLE_NAME} from {member.display_name}")
                        except Exception as e:
                            print(f"Error removing expired banned role: {e}")
            
            # Check expired mutes
            cursor.execute(
                "SELECT discord_id, mute_id FROM mutes WHERE active = TRUE AND expires_at <= NOW()"
            )
            expired_mutes = cursor.fetchall()
            
            for mute in expired_mutes:
                cursor.execute(
                    "UPDATE mutes SET active = FALSE, removed_at = NOW(), removed_reason = 'Expired' WHERE mute_id = %s",
                    (mute["mute_id"],)
                )
                member = guild.get_member(mute["discord_id"])
                if member:
                    muted_role = get_role_by_name(guild, MUTED_ROLE_NAME)
                    if muted_role and muted_role in member.roles:
                        try:
                            await member.remove_roles(muted_role)
                            print(f"Removed expired muted role from {member.display_name}")
                        except discord.Forbidden:
                            print(f"Bot lacks permissions to remove {MUTED_ROLE_NAME} from {member.display_name}")
                        except Exception as e:
                            print(f"Error removing expired muted role: {e}")
            
            connection.commit()
        except Error as e:
            print(f"Error checking expired punishments: {e}")
        finally:
            connection.close()

@tasks.loop(hours=24)
async def check_elo_decay():
    """Applies ELO decay to Topaz+ players who haven't played recently."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            
            cursor.execute(
                "SELECT discord_id, elo FROM users WHERE elo >= 900 AND last_played <= DATE_SUB(NOW(), INTERVAL 4 DAY)"
            )
            decay_players = cursor.fetchall()
            
            for player in decay_players:
                new_elo = max(0, player["elo"] - 60)
                
                cursor.execute(
                    "UPDATE users SET elo = %s WHERE discord_id = %s",
                    (new_elo, player["discord_id"])
                )
                
                await update_elo_role(player["discord_id"], new_elo)
            
            connection.commit()
        except Error as e:
            print(f"Error processing ELO decay: {e}")
        finally:
            connection.close()

@tasks.loop(minutes=5)
async def check_afk_players():
    """Moves players from empty game voice channels to an AFK voice channel."""
    if not AFK_VOICE_CHANNEL_ID:
        return # AFK channel not configured

    guild = bot.guilds[0]
    afk_vc = guild.get_channel(AFK_VOICE_CHANNEL_ID)
    if not afk_vc or not isinstance(afk_vc, discord.VoiceChannel):
        print(f"AFK voice channel with ID {AFK_VOICE_CHANNEL_ID} not found or is not a voice channel.")
        return

    for game_id, game_data in list(active_games.items()): # Iterate over a copy to allow modification
        if game_data["status"] == "in_progress":
            voice_channel = guild.get_channel(game_data["voice_channel_id"])
            if voice_channel and isinstance(voice_channel, discord.VoiceChannel):
                if not voice_channel.members: # If game voice channel is empty
                    for player_id in game_data["players"]:
                        member = guild.get_member(player_id)
                        if member and member.voice and member.voice.channel and member.voice.channel.id == voice_channel.id:
                            try:
                                await member.move_to(afk_vc, reason="Game voice channel empty for too long.")
                                print(f"Moved {member.display_name} from game {game_id} to AFK VC.")
                            except discord.Forbidden:
                                print(f"Bot lacks permissions to move {member.display_name} to AFK VC.")
                            except Exception as e:
                                print(f"Error moving {member.display_name} to AFK VC: {e}")


# --- Discord Commands ---

# Queue commands
@bot.command(name="q")
async def join_queue(ctx: commands.Context, queue_type: str = "3v3"):
    """Allows a user to join a game queue."""
    if not queue_status:
        await ctx.send(embed=create_embed("Queue Status", "Queues are currently closed!", discord.Color.red()))
        return
    
    queue_type = queue_type.lower()
    if queue_type not in QUEUE_TYPES:
        await ctx.send(embed=create_embed("Invalid Queue Type", f"Invalid queue type! Available: {', '.join(QUEUE_TYPES.keys())}", discord.Color.red()))
        return
    
    if queue_type not in active_queues:
        await ctx.send(embed=create_embed("Queue Inactive", f"{queue_type} queue is not active this season!", discord.Color.red()))
        return
    
    if not await is_registered(ctx.author.id):
        register_channel = await get_channel_by_config(ctx.guild, REGISTER_CHANNEL_ID, REGISTER_CHANNEL_NAME)
        channel_mention = register_channel.mention if register_channel else "the registration channel"
        await ctx.send(embed=create_embed("Registration Required", f"You need to register first! Use `=register <yourign>` in {channel_mention}.", discord.Color.orange()))
        return
    
    if await is_banned(ctx.author.id):
        await ctx.send(embed=create_embed("Banned", "You are banned and cannot queue!", discord.Color.red()))
        return
    
    for q_type, q_players in queues.items():
        if ctx.author.id in q_players:
            await ctx.send(embed=create_embed("Already in Queue", f"You are already in the {q_type} queue!", discord.Color.orange()))
            return
    
    for game_id, game_data in active_games.items():
        if ctx.author.id in game_data["players"] and game_data["status"] != "completed":
            await ctx.send(embed=create_embed("Already in Game", "You are already in an active game!", discord.Color.orange()))
            return
    
    # Party logic check (simplified)
    if party_size is not None:
        # A more robust party system would involve a command to create/join parties
        # and checking if all party members are in the queue here.
        # For this implementation, if party_size is active, it just means teams are ELO-balanced.
        await ctx.send(embed=create_embed("Party Season Active", f"It's a party season! Teams will be ELO-balanced. Please ensure your party members also queue for {queue_type}.", discord.Color.blue()))

    queues[queue_type].append(ctx.author.id)
    
    embed = create_embed(
        title="Queue Joined",
        description=f"You joined the {queue_type} queue!",
        color=discord.Color.green(),
        fields=[
            {"name": "Players in queue", "value": f"{len(queues[queue_type])}/{QUEUE_TYPES[queue_type]}", "inline": False}
        ]
    )
    await ctx.send(embed=embed)

@bot.command(name="leaveq")
async def leave_queue(ctx: commands.Context):
    """Allows a user to leave all active queues."""
    left = False
    for q_type in queues.keys():
        if ctx.author.id in queues[q_type]:
            queues[q_type].remove(ctx.author.id)
            left = True
    
    if left:
        await ctx.send(embed=create_embed("Queue Left", "You left all queues!", discord.Color.green()))
    else:
        await ctx.send(embed=create_embed("Not in Queue", "You weren't in any queue!", discord.Color.orange()))

# Registration system
@bot.command(name="register")
@commands.has_role(UNREGISTERED_ROLE_NAME)
async def register(ctx: commands.Context, minecraft_ign: str):
    """Registers a user with their Minecraft IGN and provides a verification code."""
    register_channel = await get_channel_by_config(ctx.guild, REGISTER_CHANNEL_ID, REGISTER_CHANNEL_NAME)
    if ctx.channel.id != (register_channel.id if register_channel else None):
        channel_mention = register_channel.mention if register_channel else "the registration channel"
        await ctx.send(embed=create_embed("Wrong Channel", f"Please use this command in the {channel_mention} channel!", discord.Color.red()), delete_after=5)
        return
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT discord_id FROM users WHERE minecraft_ign = %s",
                (minecraft_ign,)
            )
            if cursor.fetchone():
                await ctx.send(embed=create_embed("IGN Taken", "This Minecraft IGN is already registered!", discord.Color.red()))
                return
        except Error as e:
            print(f"Error checking IGN: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred during registration. Please try again.", discord.Color.red()))
            return
        finally:
            connection.close()
    
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO users (discord_id, minecraft_ign, verification_code, elo, wins, losses, mvps, streak, verified, last_played, registered_at) VALUES (%s, %s, %s, 0, 0, 0, 0, 0, FALSE, NOW(), NOW()) "
                "ON DUPLICATE KEY UPDATE minecraft_ign = %s, verification_code = %s, verified = FALSE", # Reset verified if re-registering
                (ctx.author.id, minecraft_ign, code, minecraft_ign, code)
            )
            connection.commit()
        except Error as e:
            print(f"Error registering user: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred during registration. Please try again.", discord.Color.red()))
            return
        finally:
            connection.close()
    
    embed = create_embed(
        title="Registration Started",
        description=f"Please go to Minecraft and type:\n`/link {code}`\n\n"
                    f"This will link your Minecraft account to your Discord account.",
        color=discord.Color.blue()
    )
    await ctx.send(embed=embed)

# This command would be called by the Minecraft plugin (simulated here for demonstration)
@bot.command(name="link_minecraft", hidden=True) # Hidden from help command
async def link_minecraft(ctx: commands.Context, discord_id: int, code: str):
    """
    (Internal/Minecraft Plugin) Links a Minecraft account to a Discord account.
    This command would typically be called by the Minecraft server plugin, not directly by users.
    """
    # In a real scenario, this command would likely be triggered by an API call from your Minecraft plugin
    # to your bot's backend, or via a direct message from the Minecraft server.
    # For this demonstration, it's a Discord command.
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT minecraft_ign, verified FROM users WHERE discord_id = %s AND verification_code = %s",
                (discord_id, code)
            )
            result = cursor.fetchone()
            
            if result:
                minecraft_ign, is_verified = result
                if not is_verified:
                    cursor.execute(
                        "UPDATE users SET verified = TRUE, verification_code = NULL WHERE discord_id = %s",
                        (discord_id,)
                    )
                    connection.commit()
                    
                    guild = ctx.guild # Assuming this is called from a context where guild is available
                    member = guild.get_member(discord_id)
                    if member:
                        # Assign Registered role and ELO role, remove Unregistered role
                        registered_role = get_role_by_name(guild, REGISTERED_ROLE_NAME)
                        unregistered_role = get_role_by_name(guild, UNREGISTERED_ROLE_NAME)
                        
                        if registered_role and registered_role not in member.roles:
                            try:
                                await member.add_roles(registered_role)
                            except discord.Forbidden:
                                print(f"Bot lacks permissions to add {REGISTERED_ROLE_NAME} to {member.display_name}")
                        
                        if unregistered_role and unregistered_role in member.roles:
                            try:
                                await member.remove_roles(unregistered_role)
                            except discord.Forbidden:
                                print(f"Bot lacks permissions to remove {UNREGISTERED_ROLE_NAME} from {member.display_name}")

                        await update_elo_role(discord_id, 0) # Assign initial ELO role (Iron)
                        
                        await member.send(embed=create_embed("Registration Complete!", 
                                                            f"Your Minecraft IGN '{minecraft_ign}' is now linked to your Discord account. You can now queue for games!", 
                                                            discord.Color.green()))
                        print(f"User {member.display_name} ({discord_id}) successfully registered.")
                else:
                    print(f"User {discord_id} already verified.")
            else:
                print(f"Invalid Discord ID or verification code for {discord_id}.")
        except Error as e:
            print(f"Database error during Minecraft linking: {e}")
        finally:
            connection.close()

@bot.command(name="forceregister")
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def force_register(ctx: commands.Context, user: discord.User, minecraft_ign: str):
    """Forces registration for a user, bypassing Minecraft verification."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO users (discord_id, minecraft_ign, verified, elo, wins, losses, mvps, streak, last_played, registered_at) VALUES (%s, %s, TRUE, 0, 0, 0, 0, 0, NOW(), NOW()) "
                "ON DUPLICATE KEY UPDATE minecraft_ign = %s, verified = TRUE, elo = 0, wins = 0, losses = 0, mvps = 0, streak = 0, last_played = NOW()",
                (user.id, minecraft_ign, minecraft_ign)
            )
            connection.commit()
            
            # Assign roles and update nickname
            guild = ctx.guild
            member = guild.get_member(user.id)
            if member:
                registered_role = get_role_by_name(guild, REGISTERED_ROLE_NAME)
                unregistered_role = get_role_by_name(guild, UNREGISTERED_ROLE_NAME)
                
                if registered_role and registered_role not in member.roles:
                    try:
                        await member.add_roles(registered_role)
                    except discord.Forbidden:
                        await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to add the '{REGISTERED_ROLE_NAME}' role to {member.mention}.", discord.Color.red()))
                
                if unregistered_role and unregistered_role in member.roles:
                    try:
                        await member.remove_roles(unregistered_role)
                    except discord.Forbidden:
                        await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to remove the '{UNREGISTERED_ROLE_NAME}' role from {member.mention}.", discord.Color.red()))

                await update_elo_role(user.id, 0) # Assign initial ELO role (Iron)
                
            await ctx.send(embed=create_embed("Force Registered", f"{user.mention} has been force registered with IGN: `{minecraft_ign}`.", discord.Color.green()))
        except Error as e:
            print(f"Error force registering user: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred during force registration. Please try again.", discord.Color.red()))
        finally:
            connection.close()

# Moderation commands
@bot.command(name="ban")
@commands.has_any_role(MODERATOR_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def ban_user(ctx: commands.Context, user: discord.User, duration: str, *, reason: str = "No reason provided"):
    """Bans a user for a specified duration and logs the action."""
    try:
        duration_amount = int(duration[:-1])
        duration_unit = duration[-1].lower()
        
        if duration_unit == 's': delta = datetime.timedelta(seconds=duration_amount)
        elif duration_unit == 'm': delta = datetime.timedelta(minutes=duration_amount)
        elif duration_unit == 'h': delta = datetime.timedelta(hours=duration_amount)
        elif duration_unit == 'd': delta = datetime.timedelta(days=duration_amount)
        elif duration_unit == 'y': delta = datetime.timedelta(days=duration_amount*365)
        else: raise ValueError("Invalid duration unit")
        
        expires_at = datetime.datetime.now() + delta
    except ValueError:
        await ctx.send(embed=create_embed("Invalid Duration", "Invalid duration format! Use something like `1s`, `1m`, `1h`, `1d`, `1y`.", discord.Color.red()))
        return
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO bans (discord_id, reason, duration, issued_by, expires_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (user.id, reason, duration, ctx.author.id, expires_at)
            )
            connection.commit()
        except Error as e:
            print(f"Error adding ban: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while banning the user.", discord.Color.red()))
            return
        finally:
            connection.close()
    
    guild = ctx.guild
    banned_role = get_role_by_name(guild, BANNED_ROLE_NAME)
    if banned_role:
        member = guild.get_member(user.id)
        if member:
            try:
                await member.add_roles(banned_role)
            except discord.Forbidden:
                await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to add the '{BANNED_ROLE_NAME}' role to {user.mention}.", discord.Color.red()))
            except Exception as e:
                await ctx.send(embed=create_embed("Role Error", f"An error occurred while adding the '{BANNED_ROLE_NAME}' role: {e}", discord.Color.red()))
    
    embed = create_embed(
        title="User Banned",
        description=f"{user.mention} has been banned.",
        color=discord.Color.red(),
        fields=[
            {"name": "Reason", "value": reason, "inline": False},
            {"name": "Duration", "value": duration, "inline": True},
            {"name": "Expires At", "value": expires_at.strftime('%Y-%m-%d %H:%M:%S UTC'), "inline": True}
        ]
    )
    await ctx.send(embed=embed)
    
    ban_log_channel = await get_channel_by_config(guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME)
    if ban_log_channel:
        log_html = f"""
        <p><strong>User:</strong> {user.display_name} ({user.id})</p>
        <p><strong>Action:</strong> Banned</p>
        <p><strong>Reason:</strong> {html.escape(reason)}</p>
        <p><strong>Duration:</strong> {html.escape(duration)}</p>
        <p><strong>Expires At:</strong> {expires_at.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        <p><strong>Issued By:</strong> {ctx.author.display_name} ({ctx.author.id})</p>
        <p><strong>Issued At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        """
        await log_to_html_channel(guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME, f"Ban Log - {user.display_name}", log_html)


@bot.command(name="unban")
@commands.has_any_role(MODERATOR_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def unban_user(ctx: commands.Context, user: discord.User, *, reason: str = "No reason provided"):
    """Unbans a user and removes their banned role."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE bans SET active = FALSE, removed_at = NOW(), removed_reason = %s WHERE discord_id = %s AND active = TRUE",
                (reason, user.id)
            )
            connection.commit()
            if cursor.rowcount == 0:
                await ctx.send(embed=create_embed("Unban Failed", f"{user.mention} is not currently banned or no active ban record found.", discord.Color.orange()))
                return
        except Error as e:
            print(f"Error unbanning user: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while unbanning the user.", discord.Color.red()))
            return
        finally:
            connection.close()
    
    guild = ctx.guild
    banned_role = get_role_by_name(guild, BANNED_ROLE_NAME)
    if banned_role:
        member = guild.get_member(user.id)
        if member:
            if banned_role in member.roles:
                try:
                    await member.remove_roles(banned_role)
                except discord.Forbidden:
                    await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to remove the '{BANNED_ROLE_NAME}' role from {user.mention}.", discord.Color.red()))
                except Exception as e:
                    await ctx.send(embed=create_embed("Role Error", f"An error occurred while removing the '{BANNED_ROLE_NAME}' role: {e}", discord.Color.red()))
            else:
                await ctx.send(embed=create_embed("Role Not Found", f"{user.mention} does not have the '{BANNED_ROLE_NAME}' role.", discord.Color.orange()))
    
    embed = create_embed(
        title="User Unbanned",
        description=f"{user.mention} has been unbanned.",
        color=discord.Color.green(),
        fields=[
            {"name": "Reason", "value": reason, "inline": False},
            {"name": "Unbanned By", "value": ctx.author.mention, "inline": True}
        ]
    )
    await ctx.send(embed=embed)
    
    ban_log_channel = await get_channel_by_config(guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME)
    if ban_log_channel:
        log_html = f"""
        <p><strong>User:</strong> {user.display_name} ({user.id})</p>
        <p><strong>Action:</strong> Unbanned</p>
        <p><strong>Reason:</strong> {html.escape(reason)}</p>
        <p><strong>Unbanned By:</strong> {ctx.author.display_name} ({ctx.author.id})</p>
        <p><strong>Unbanned At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        """
        await log_to_html_channel(guild, BAN_LOG_CHANNEL_ID, BAN_LOG_CHANNEL_NAME, f"Unban Log - {user.display_name}", log_html)


@bot.command(name="mute")
@commands.has_any_role(MODERATOR_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def mute_user(ctx: commands.Context, user: discord.User, duration: str, *, reason: str = "No reason provided"):
    """Mutes a user for a specified duration and logs the action."""
    try:
        duration_amount = int(duration[:-1])
        duration_unit = duration[-1].lower()
        
        if duration_unit == 's': delta = datetime.timedelta(seconds=duration_amount)
        elif duration_unit == 'm': delta = datetime.timedelta(minutes=duration_amount)
        elif duration_unit == 'h': delta = datetime.timedelta(hours=duration_amount)
        elif duration_unit == 'd': delta = datetime.timedelta(days=duration_amount)
        elif duration_unit == 'y': delta = datetime.timedelta(days=duration_amount*365)
        else: raise ValueError("Invalid duration unit")
        
        expires_at = datetime.datetime.now() + delta
    except ValueError:
        await ctx.send(embed=create_embed("Invalid Duration", "Invalid duration format! Use something like `1s`, `1m`, `1h`, `1d`, `1y`.", discord.Color.red()))
        return
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO mutes (discord_id, reason, duration, issued_by, expires_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (user.id, reason, duration, ctx.author.id, expires_at)
            )
            connection.commit()
        except Error as e:
            print(f"Error adding mute: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while muting the user.", discord.Color.red()))
            return
        finally:
            connection.close()
    
    guild = ctx.guild
    muted_role = get_role_by_name(guild, MUTED_ROLE_NAME)
    if muted_role:
        member = guild.get_member(user.id)
        if member:
            try:
                await member.add_roles(muted_role)
            except discord.Forbidden:
                await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to add the '{MUTED_ROLE_NAME}' role to {user.mention}.", discord.Color.red()))
            except Exception as e:
                await ctx.send(embed=create_embed("Role Error", f"An error occurred while adding the '{MUTED_ROLE_NAME}' role: {e}", discord.Color.red()))
    
    embed = create_embed(
        title="User Muted",
        description=f"{user.mention} has been muted.",
        color=discord.Color.red(),
        fields=[
            {"name": "Reason", "value": reason, "inline": False},
            {"name": "Duration", "value": duration, "inline": True},
            {"name": "Expires At", "value": expires_at.strftime('%Y-%m-%d %H:%M:%S UTC'), "inline": True}
        ]
    )
    await ctx.send(embed=embed)
    
    mute_log_channel = await get_channel_by_config(guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME)
    if mute_log_channel:
        log_html = f"""
        <p><strong>User:</strong> {user.display_name} ({user.id})</p>
        <p><strong>Action:</strong> Muted</p>
        <p><strong>Reason:</strong> {html.escape(reason)}</p>
        <p><strong>Duration:</strong> {html.escape(duration)}</p>
        <p><strong>Expires At:</strong> {expires_at.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        <p><strong>Issued By:</strong> {ctx.author.display_name} ({ctx.author.id})</p>
        <p><strong>Issued At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        """
        await log_to_html_channel(guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME, f"Mute Log - {user.display_name}", log_html)


@bot.command(name="unmute")
@commands.has_any_role(MODERATOR_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def unmute_user(ctx: commands.Context, user: discord.User, *, reason: str = "No reason provided"):
    """Unmutes a user and removes their muted role."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE mutes SET active = FALSE, removed_at = NOW(), removed_reason = %s WHERE discord_id = %s AND active = TRUE",
                (reason, user.id)
            )
            connection.commit()
            if cursor.rowcount == 0:
                await ctx.send(embed=create_embed("Unmute Failed", f"{user.mention} is not currently muted or no active mute record found.", discord.Color.orange()))
                return
        except Error as e:
            print(f"Error unmuting user: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while unmuting the user.", discord.Color.red()))
            return
        finally:
            connection.close()
    
    guild = ctx.guild
    muted_role = get_role_by_name(guild, MUTED_ROLE_NAME)
    if muted_role:
        member = guild.get_member(user.id)
        if member:
            if muted_role in member.roles:
                try:
                    await member.remove_roles(muted_role)
                except discord.Forbidden:
                    await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to remove the '{MUTED_ROLE_NAME}' role from {user.mention}.", discord.Color.red()))
                except Exception as e:
                    await ctx.send(embed=create_embed("Role Error", f"An error occurred while removing the '{MUTED_ROLE_NAME}' role: {e}", discord.Color.red()))
            else:
                await ctx.send(embed=create_embed("Role Not Found", f"{user.mention} does not have the '{MUTED_ROLE_NAME}' role.", discord.Color.orange()))
    
    embed = create_embed(
        title="User Unmuted",
        description=f"{user.mention} has been unmuted.",
        color=discord.Color.green(),
        fields=[
            {"name": "Reason", "value": reason, "inline": False},
            {"name": "Unmuted By", "value": ctx.author.mention, "inline": True}
        ]
    )
    await ctx.send(embed=embed)
    
    mute_log_channel = await get_channel_by_config(guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME)
    if mute_log_channel:
        log_html = f"""
        <p><strong>User:</strong> {user.display_name} ({user.id})</p>
        <p><strong>Action:</strong> Unmuted</p>
        <p><strong>Reason:</strong> {html.escape(reason)}</p>
        <p><strong>Unmuted By:</strong> {ctx.author.display_name} ({ctx.author.id})</p>
        <p><strong>Unmuted At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        """
        await log_to_html_channel(guild, MUTE_LOG_CHANNEL_ID, MUTE_LOG_CHANNEL_NAME, f"Unmute Log - {user.display_name}", log_html)


@bot.command(name="strike")
@commands.has_any_role(MODERATOR_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def strike_user(ctx: commands.Context, user: discord.User, *, reason: str):
    """Issues a strike to a user, deducts ELO, and logs the action."""
    # Deduct ELO
    current_elo = await get_player_elo(user.id)
    new_elo = max(0, current_elo - 40)
    await update_player_elo_in_db(user.id, -40)
    await update_elo_role(user.id, new_elo)

    # Add strike to database
    connection = create_db_connection()
    strike_id = None
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO strikes (discord_id, reason, issued_by) VALUES (%s, %s, %s)",
                (user.id, reason, ctx.author.id)
            )
            strike_id = cursor.lastrowid
            connection.commit()
        except Error as e:
            print(f"Error adding strike to database: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while issuing the strike.", discord.Color.red()))
            return
        finally:
            connection.close()

    embed = create_embed(
        title="User Striked",
        description=f"{user.mention} has received a strike.",
        color=discord.Color.orange(),
        fields=[
            {"name": "Reason", "value": reason, "inline": False},
            {"name": "ELO Change", "value": "-40", "inline": True},
            {"name": "New ELO", "value": new_elo, "inline": True},
            {"name": "Strike ID", "value": strike_id, "inline": False}
        ]
    )
    await ctx.send(embed=embed)

    strike_log_channel = await get_channel_by_config(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME)
    if strike_log_channel:
        log_html = f"""
        <p><strong>User:</strong> {user.display_name} ({user.id})</p>
        <p><strong>Action:</strong> Striked</p>
        <p><strong>Reason:</strong> {html.escape(reason)}</p>
        <p><strong>ELO Change:</strong> -40</p>
        <p><strong>New ELO:</strong> {new_elo}</p>
        <p><strong>Strike ID:</strong> {strike_id}</p>
        <p><strong>Issued By:</strong> {ctx.author.display_name} ({ctx.author.id})</p>
        <p><strong>Issued At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        """
        await log_to_html_channel(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME, f"Strike Log - {user.display_name}", log_html)


@bot.command(name="strikeremove", aliases=["srem"])
@commands.has_any_role(MODERATOR_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def strike_remove(ctx: commands.Context, strike_id: int, *, reason: str):
    """Removes a strike from a user's history."""
    connection = create_db_connection()
    success = False
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE strikes SET removed_at = NOW(), removed_reason = %s WHERE strike_id = %s AND removed_at IS NULL",
                (reason, strike_id)
            )
            connection.commit()
            success = cursor.rowcount > 0 # Return True if a row was updated
        except Error as e:
            print(f"Error removing strike from database: {e}")
        finally:
            connection.close()
    
    if success:
        embed = create_embed(
            title="Strike Removed",
            description=f"Strike with ID `{strike_id}` has been removed.",
            color=discord.Color.green(),
            fields=[
                {"name": "Reason for Removal", "value": reason, "inline": False},
                {"name": "Removed By", "value": ctx.author.mention, "inline": True}
            ]
        )
        await ctx.send(embed=embed)

        strike_log_channel = await get_channel_by_config(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME)
        if strike_log_channel:
            log_html = f"""
            <p><strong>Strike ID:</strong> {strike_id}</p>
            <p><strong>Action:</strong> Removed</p>
            <p><strong>Reason for Removal:</strong> {html.escape(reason)}</p>
            <p><strong>Removed By:</strong> {ctx.author.display_name} ({ctx.author.id})</p>
            <p><strong>Removed At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            """
            await log_to_html_channel(ctx.guild, STRIKE_LOG_CHANNEL_ID, STRIKE_LOG_CHANNEL_NAME, f"Strike Removal Log - ID {strike_id}", log_html)
    else:
        await ctx.send(embed=create_embed("Strike Not Found", f"Strike with ID `{strike_id}` not found or already removed.", discord.Color.orange()))

# Strike Request Command
class StrikeRequestView(discord.ui.View):
    def __init__(self, bot_instance: commands.Bot, request_id: int, target_id: int, requester_id: int, channel_id: int):
        super().__init__(timeout=3600) # 60 minutes timeout for voting
        self.bot_instance = bot_instance
        self.request_id = request_id
        self.target_id = target_id
        self.requester_id = requester_id
        self.channel_id = channel_id
        self.upvotes = set()
        self.downvotes = set()
        self.message: Optional[discord.Message] = None # Will be set after sending

    async def on_timeout(self):
        """Called when the view times out."""
        channel = self.bot_instance.get_channel(self.channel_id)
        if channel and isinstance(channel, discord.TextChannel):
            await self.close_strike_request(channel, "Poll timed out.")

    async def close_strike_request(self, channel: discord.TextChannel, reason: str):
        """Closes the strike request, processes votes, and logs the outcome."""
        # Disable buttons
        if self.message:
            for item in self.children:
                item.disabled = True
            await self.message.edit(view=self)

        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor()
                # Determine outcome based on votes
                upvote_count = len(self.upvotes)
                downvote_count = len(self.downvotes)

                strike_approved = False
                # Criteria: at least 3 upvotes AND (upvotes > downvotes AND upvotes - downvotes >= 2)
                if upvote_count >= 3 and (upvote_count > downvote_count and (upvote_count - downvote_count) >= 2):
                    strike_approved = True

                status = "approved" if strike_approved else "denied"
                
                cursor.execute(
                    "UPDATE strike_requests SET status = %s, closed_at = NOW() WHERE request_id = %s",
                    (status, self.request_id)
                )
                connection.commit()

                if strike_approved:
                    # Apply strike
                    strike_reason = f"Strike Request Approved (ID: {self.request_id}). Original Reason: {reason}"
                    # Simulate ctx for strike_user, as it's not a direct command call
                    mock_ctx = type('obj', (object,), {'author': self.bot_instance.user, 'guild': channel.guild, 'send': channel.send})()
                    await strike_user(mock_ctx, self.bot_instance.get_user(self.target_id) or await self.bot_instance.fetch_user(self.target_id), strike_reason)
                    await channel.send(embed=create_embed("Strike Request Result", f"The strike request for <@{self.target_id}> has been **APPROVED** and a strike has been issued!", discord.Color.green()))
                else:
                    await channel.send(embed=create_embed("Strike Request Result", f"The strike request for <@{self.target_id}> has been **DENIED**.", discord.Color.red()))

                # Log to HTML
                target_user_obj = self.bot_instance.get_user(self.target_id) or await self.bot_instance.fetch_user(self.target_id)
                requester_user_obj = self.bot_instance.get_user(self.requester_id) or await self.bot_instance.fetch_user(self.requester_id)

                log_html = f"""
                <p><strong>Strike Request ID:</strong> {self.request_id}</p>
                <p><strong>Target User:</strong> {target_user_obj.display_name if target_user_obj else 'Unknown'} ({self.target_id})</p>
                <p><strong>Requester:</strong> {requester_user_obj.display_name if requester_user_obj else 'Unknown'} ({self.requester_id})</p>
                <p><strong>Reason:</strong> {html.escape(reason)}</p>
                <p><strong>Votes:</strong> Upvotes: {upvote_count}, Downvotes: {downvote_count}</p>
                <p><strong>Result:</strong> {status.upper()}</p>
                <p><strong>Closed By:</strong> Bot (Timeout)</p>
                <p><strong>Closed At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
                """
                await log_to_html_channel(channel.guild, STRIKE_REQUEST_CHANNEL_ID, STRIKE_REQUEST_CHANNEL_NAME, f"Strike Request Log - ID {self.request_id}", log_html)

                await channel.delete(reason="Strike request poll concluded.")

            except Error as e:
                print(f"Error closing strike request in DB: {e}")
            finally:
                connection.close()
        
        if self.message and self.message.id in active_strike_requests:
            del active_strike_requests[self.message.id]

    @discord.ui.button(label="Upvote", style=discord.ButtonStyle.green, emoji="⬆️")
    async def upvote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handles upvote button clicks for strike requests."""
        # Ensure only staff or specific roles can vote if needed, but request implies anyone can vote
        # For now, allowing any user to vote as per "people to judge"
        
        # Prevent self-voting
        if interaction.user.id == self.requester_id or interaction.user.id == self.target_id:
            await interaction.response.send_message("You cannot vote on your own request or if you are the target.", ephemeral=True)
            return

        if interaction.user.id in self.downvotes:
            self.downvotes.remove(interaction.user.id)
        self.upvotes.add(interaction.user.id)
        await interaction.response.send_message("You upvoted!", ephemeral=True)
        await self.update_vote_display(interaction.message)

    @discord.ui.button(label="Downvote", style=discord.ButtonStyle.red, emoji="⬇️")
    async def downvote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handles downvote button clicks for strike requests."""
        # Prevent self-voting
        if interaction.user.id == self.requester_id or interaction.user.id == self.target_id:
            await interaction.response.send_message("You cannot vote on your own request or if you are the target.", ephemeral=True)
            return

        if interaction.user.id in self.upvotes:
            self.upvotes.remove(interaction.user.id)
        self.downvotes.add(interaction.user.id)
        await interaction.response.send_message("You downvoted!", ephemeral=True)
        await self.update_vote_display(interaction.message)

    async def update_vote_display(self, message: discord.Message):
        """Updates the vote count displayed on the strike request embed."""
        embed = message.embeds[0]
        found_votes_field = False
        for i, field in enumerate(embed.fields):
            if field.name == "Current Votes":
                embed.set_field_at(i, name="Current Votes", value=f"Upvotes: {len(self.upvotes)}\nDownvotes: {len(self.downvotes)}", inline=False)
                found_votes_field = True
                break
        if not found_votes_field:
            embed.add_field(name="Current Votes", value=f"Upvotes: {len(self.upvotes)}\nDownvotes: {len(self.downvotes)}", inline=False)
        
        await message.edit(embed=embed)


@bot.command(name="strikerequest", aliases=["sr"])
async def strike_request(ctx: commands.Context, user: discord.User, reason: str, proof: Optional[discord.Attachment] = None):
    """Allows players to request a strike against another user."""
    strike_request_channel = await get_channel_by_config(ctx.guild, STRIKE_REQUEST_CHANNEL_ID, STRIKE_REQUEST_CHANNEL_NAME)
    if ctx.channel.id != (strike_request_channel.id if strike_request_channel else None):
        channel_mention = strike_request_channel.mention if strike_request_channel else "the strike request channel"
        await ctx.send(embed=create_embed("Wrong Channel", f"Please use this command in the {channel_mention} channel!", discord.Color.red()), delete_after=5)
        return

    proof_url = proof.url if proof else "No proof provided."

    guild = ctx.guild
    strike_requests_category = await get_channel_or_create_category(guild, STRIKE_REQUESTS_CATEGORY_ID, "Strike Requests", is_category=True)

    if not strike_requests_category:
        await ctx.send(embed=create_embed("Error", "Could not find or create 'Strike Requests' category.", discord.Color.red()))
        return

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    # Add staff roles to overwrites
    staff_roles_to_add = [MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME]
    for role_name in staff_roles_to_add:
        role = get_role_by_name(guild, role_name)
        if role:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

    request_channel = await guild.create_text_channel(
        f"strike-request-{user.name}-{random.randint(100,999)}",
        category=strike_requests_category,
        overwrites=overwrites
    )

    # Add to database
    connection = create_db_connection()
    request_id = None
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO strike_requests (requester_id, target_id, reason, proof_url, channel_id) VALUES (%s, %s, %s, %s, %s)",
                (ctx.author.id, user.id, reason, proof_url, request_channel.id)
            )
            request_id = cursor.lastrowid
            connection.commit()
        except Error as e:
            print(f"Error creating strike request in DB: {e}")
            await request_channel.send(embed=create_embed("Database Error", "An error occurred while creating the strike request.", discord.Color.red()))
            await request_channel.delete()
            return
        finally:
            connection.close()
    
    if not request_id:
        await ctx.send(embed=create_embed("Error", "Could not create strike request.", discord.Color.red()))
        return

    embed = create_embed(
        title=f"Strike Request for {user.display_name}",
        description=f"**Requester:** {ctx.author.mention}\n**Target:** {user.mention}\n**Reason:** {reason}",
        color=discord.Color.blue()
    )
    if proof:
        embed.set_image(url=proof.url)
    embed.add_field(name="Proof", value=proof_url, inline=False)
    embed.add_field(name="Instructions", value="Staff and eligible members, please vote using the buttons below. The poll will close in 60 minutes.", inline=False)
    embed.add_field(name="Current Votes", value="Upvotes: 0\nDownvotes: 0", inline=False) # Initial vote display

    view = StrikeRequestView(bot, request_id, user.id, ctx.author.id, request_channel.id)
    poll_message = await request_channel.send(embed=embed, view=view)
    view.message = poll_message # Store message for later editing

    # Update poll message ID in DB
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE strike_requests SET poll_message_id = %s WHERE request_id = %s",
                (poll_message.id, request_id)
            )
            connection.commit()
        except Error as e:
            print(f"Error updating strike request poll message ID: {e}")
        finally:
            connection.close()

    active_strike_requests[poll_message.id] = view # Store the view instance for interaction
    
    await ctx.send(embed=create_embed("Strike Request Created", f"Your strike request for {user.mention} has been created in {request_channel.mention}.", discord.Color.green()))


# Screenshare Command
class ScreenshareView(discord.ui.View):
    def __init__(self, bot_instance: commands.Bot, ticket_id: int, requester_id: int, target_id: int, channel_id: int):
        super().__init__(timeout=600) # 10 minutes timeout for claim
        self.bot_instance = bot_instance
        self.ticket_id = ticket_id
        self.requester_id = requester_id
        self.target_id = target_id
        self.channel_id = channel_id
        self.message: Optional[discord.Message] = None # Will be set after sending
        self.timer_task: Optional[asyncio.Task] = None # To store the asyncio task for timeout

    async def on_timeout(self):
        """Called when the screenshare request is not accepted within 10 minutes."""
        channel = self.bot_instance.get_channel(self.channel_id)
        if channel and isinstance(channel, discord.TextChannel):
            await self.release_frozen_role(channel.guild, self.target_id)
            await channel.send(embed=create_embed("Screenshare Request Timed Out", "No screensharer accepted the request in 10 minutes. The Frozen role has been removed.", discord.Color.red()))
            await self.close_ticket(channel, "Screenshare request timed out (no claim).")

    async def release_frozen_role(self, guild: discord.Guild, user_id: int):
        """Removes the 'Frozen' role from a user."""
        member = guild.get_member(user_id)
        if member:
            frozen_role = get_role_by_name(guild, FROZEN_ROLE_NAME)
            if frozen_role and frozen_role in member.roles:
                try:
                    await member.remove_roles(frozen_role)
                    print(f"Removed Frozen role from {member.display_name}.")
                except discord.Forbidden:
                    print(f"Bot lacks permissions to remove {FROZEN_ROLE_NAME} from {member.display_name}.")
                except Exception as e:
                    print(f"Error removing Frozen role: {e}")

    async def close_ticket(self, channel: discord.TextChannel, reason: str, closed_by_id: Optional[int] = None):
        """Closes the screenshare ticket, moves it to closed category, and logs."""
        # Disable buttons
        if self.message:
            for item in self.children:
                item.disabled = True
            await self.message.edit(view=self)

        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "UPDATE tickets SET status = 'closed', closed_by = %s, closed_at = NOW(), close_reason = %s WHERE ticket_id = %s",
                    (closed_by_id or self.bot_instance.user.id, reason, self.ticket_id)
                )
                connection.commit()
            except Error as e:
                print(f"Error closing screenshare ticket in DB: {e}")
            finally:
                connection.close()

        closed_tickets_category = await get_channel_or_create_category(channel.guild, CLOSED_TICKETS_CATEGORY_ID, "Closed Tickets", is_category=True)

        if not closed_tickets_category:
            print("Warning: Could not find or create 'Closed Tickets' category for screenshare log.")

        try:
            # Overwrites for closed tickets: only staff can read
            overwrites = {
                channel.guild.default_role: discord.PermissionOverwrite(read_messages=False),
                self.bot_instance.get_user(self.requester_id) or await self.bot_instance.fetch_user(self.requester_id): discord.PermissionOverwrite(read_messages=False),
                self.bot_instance.get_user(self.target_id) or await self.bot_instance.fetch_user(self.target_id): discord.PermissionOverwrite(read_messages=False),
                channel.guild.me: discord.PermissionOverwrite(read_messages=True)
            }
            screensharing_role = get_role_by_name(channel.guild, SCREENSHARING_TEAM_ROLE_NAME)
            if screensharing_role:
                overwrites[screensharing_role] = discord.PermissionOverwrite(read_messages=True)
            
            await channel.edit(category=closed_tickets_category, overwrites=overwrites)
            await channel.send(embed=create_embed("Ticket Closed", f"This screenshare ticket has been closed. Reason: {reason}", discord.Color.red()))
        except Exception as e:
            print(f"Error moving/editing screenshare channel: {e}")

        # Log to HTML
        target_user_obj = self.bot_instance.get_user(self.target_id) or await self.bot_instance.fetch_user(self.target_id)
        requester_user_obj = self.bot_instance.get_user(self.requester_id) or await self.bot_instance.fetch_user(self.requester_id)
        closed_by_user_obj = self.bot_instance.get_user(closed_by_id) or await self.bot_instance.fetch_user(closed_by_id) if closed_by_id else None

        log_html = f"""
        <p><strong>Screenshare Ticket ID:</strong> {self.ticket_id}</p>
        <p><strong>Target User:</strong> {target_user_obj.display_name if target_user_obj else 'Unknown'} ({self.target_id})</p>
        <p><strong>Requester:</strong> {requester_user_obj.display_name if requester_user_obj else 'Unknown'} ({self.requester_id})</p>
        <p><strong>Reason for Closure:</strong> {html.escape(reason)}</p>
        <p><strong>Closed By:</strong> {closed_by_user_obj.display_name if closed_by_user_obj else 'Bot'} ({closed_by_id or self.bot_instance.user.id})</p>
        <p><strong>Closed At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        """
        await log_to_html_channel(channel.guild, SCREENSNARE_LOG_CHANNEL_ID, SCREENSNARE_LOG_CHANNEL_NAME, f"Screenshare Ticket Log - ID {self.ticket_id}", log_html)

        if self.channel_id in active_screenshare_tickets:
            del active_screenshare_tickets[self.channel_id]


    @discord.ui.button(label="Accept", style=discord.ButtonStyle.green)
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Allows Screensharing Team to accept a screenshare request."""
        screensharing_role = get_role_by_name(interaction.guild, SCREENSHARING_TEAM_ROLE_NAME)
        if not screensharing_role or screensharing_role not in interaction.user.roles:
            await interaction.response.send_message("You do not have permission to accept this screenshare.", ephemeral=True)
            return

        if self.timer_task:
            self.timer_task.cancel() # Cancel the timeout task

        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "UPDATE tickets SET status = 'claimed', claimed_by = %s, claimed_at = NOW() WHERE ticket_id = %s",
                    (interaction.user.id, self.ticket_id)
                )
                connection.commit()
            except Error as e:
                print(f"Error claiming screenshare ticket in DB: {e}")
                await interaction.response.send_message("An error occurred while claiming the ticket.", ephemeral=True)
                return
            finally:
                connection.close()

        await interaction.response.send_message(f"{interaction.user.mention} has accepted this screenshare request. The channel will remain open until closed by a staff member.", ephemeral=False)
        button.disabled = True
        self.children[1].disabled = True # Disable decline button as well
        await interaction.message.edit(view=self)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.red)
    async def decline_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Allows Screensharing Team to decline a screenshare request."""
        screensharing_role = get_role_by_name(interaction.guild, SCREENSHARING_TEAM_ROLE_NAME)
        if not screensharing_role or screensharing_role not in interaction.user.roles:
            await interaction.response.send_message("You do not have permission to decline this screenshare.", ephemeral=True)
            return

        if self.timer_task:
            self.timer_task.cancel() # Cancel the timeout task

        await self.release_frozen_role(interaction.guild, self.target_id)
        await interaction.response.send_message(f"{interaction.user.mention} has declined this screenshare request. The Frozen role has been removed.", ephemeral=False)
        await self.close_ticket(interaction.channel, f"Declined by {interaction.user.display_name}.", interaction.user.id)


@bot.command(name="ss")
async def screenshare_request(ctx: commands.Context, user: discord.User, reason: str, proof: Optional[discord.Attachment] = None):
    """Initiates a screenshare request, assigns 'Frozen' role, and creates a private ticket."""
    # Assign Frozen role
    guild = ctx.guild
    frozen_role = get_role_by_name(guild, FROZEN_ROLE_NAME)
    if frozen_role:
        member = guild.get_member(user.id)
        if member:
            if frozen_role not in member.roles:
                try:
                    await member.add_roles(frozen_role)
                    await ctx.send(embed=create_embed("User Frozen", f"{user.mention} has been assigned the '{FROZEN_ROLE_NAME}' role.", discord.Color.orange()))
                except discord.Forbidden:
                    await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to add the '{FROZEN_ROLE_NAME}' role to {user.mention}.", discord.Color.red()))
                    return # Stop if role cannot be added
                except Exception as e:
                    await ctx.send(embed=create_embed("Role Error", f"An error occurred while adding the '{FROZEN_ROLE_NAME}' role: {e}", discord.Color.red()))
                    return
            else:
                await ctx.send(embed=create_embed("Already Frozen", f"{user.mention} already has the '{FROZEN_ROLE_NAME}' role.", discord.Color.orange()))
        else:
            await ctx.send(embed=create_embed("User Not Found", "Could not find the specified user in this guild.", discord.Color.red()))
            return
    else:
        await ctx.send(embed=create_embed("Role Not Found", f"The '{FROZEN_ROLE_NAME}' role was not found. Please ensure it exists.", discord.Color.red()))
        return

    proof_url = proof.url if proof else "No proof provided."

    ticket_category = await get_channel_or_create_category(guild, TICKET_CATEGORY_ID, "Tickets", is_category=True)
    if not ticket_category:
        await ctx.send(embed=create_embed("Error", "Could not find or create 'Tickets' category.", discord.Color.red()))
        return

    screensharing_role = get_role_by_name(guild, SCREENSHARING_TEAM_ROLE_NAME)
    if not screensharing_role:
        await ctx.send(embed=create_embed("Role Error", f"The '{SCREENSHARING_TEAM_ROLE_NAME}' role was not found. Cannot create screenshare ticket.", discord.Color.red()))
        return

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        screensharing_role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }

    ticket_channel = await guild.create_text_channel(
        f"ss-ticket-{user.name}-{random.randint(100,999)}",
        category=ticket_category,
        overwrites=overwrites
    )

    connection = create_db_connection()
    ticket_id = None
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO tickets (requester_id, target_id, type, channel_id) VALUES (%s, %s, %s, %s)",
                (ctx.author.id, user.id, 'screenshareappeal', ticket_channel.id) # Using screenshareappeal type for SS tickets
            )
            ticket_id = cursor.lastrowid
            connection.commit()
        except Error as e:
            print(f"Error creating screenshare ticket in DB: {e}")
            await ticket_channel.send(embed=create_embed("Database Error", "An error occurred while creating the screenshare ticket.", discord.Color.red()))
            await ticket_channel.delete()
            return
        finally:
            connection.close()
    
    if not ticket_id:
        await ctx.send(embed=create_embed("Error", "Could not create screenshare ticket.", discord.Color.red()))
        return

    embed = create_embed(
        title=f"Screenshare Request for {user.display_name}",
        description=f"**Requester:** {ctx.author.mention}\n**Target:** {user.mention}\n**Reason:** {reason}",
        color=discord.Color.blue()
    )
    if proof:
        embed.set_image(url=proof.url)
    embed.add_field(name="Proof", value=proof_url, inline=False)
    embed.add_field(name="Instructions", value=f"A screensharing staff member will accept or decline this request. If no one accepts in 10 minutes, the '{FROZEN_ROLE_NAME}' role will be removed.", inline=False)

    view = ScreenshareView(bot, ticket_id, ctx.author.id, user.id, ticket_channel.id)
    ticket_message = await ticket_channel.send(embed=embed, view=view)
    view.message = ticket_message # Store message for later editing
    view.timer_task = bot.loop.create_task(view.on_timeout()) # Start the timeout task

    active_screenshare_tickets[ticket_channel.id] = view # Store the view instance
    
    await ctx.send(embed=create_embed("Screenshare Ticket Created", f"A screenshare ticket for {user.mention} has been created in {ticket_channel.mention}.", discord.Color.green()))


@bot.command(name="ssclose")
@commands.has_any_role(SCREENSHARING_TEAM_ROLE_NAME)
async def screenshare_close(ctx: commands.Context, *, reason: str = "No reason provided."):
    """Closes an active screenshare ticket."""
    ticket_category = await get_channel_by_config(ctx.guild, TICKET_CATEGORY_ID, "Tickets")
    if ctx.channel.category and (ctx.channel.category.id == (ticket_category.id if ticket_category else None) or ctx.channel.category.name == "Tickets"):
        if ctx.channel.id in active_screenshare_tickets:
            view: ScreenshareView = active_screenshare_tickets[ctx.channel.id]
            
            # Ensure frozen role is removed when closing
            guild = ctx.guild
            member = guild.get_member(view.target_id)
            if member:
                frozen_role = get_role_by_name(guild, FROZEN_ROLE_NAME)
                if frozen_role and frozen_role in member.roles:
                    try:
                        await member.remove_roles(frozen_role)
                        await ctx.send(embed=create_embed("Frozen Role Removed", f"The '{FROZEN_ROLE_NAME}' role has been removed from {member.mention}.", discord.Color.green()))
                    except discord.Forbidden:
                        await ctx.send(embed=create_embed("Permissions Error", f"I don't have permission to remove the '{FROZEN_ROLE_NAME}' role from {member.mention}.", discord.Color.red()))
                    except Exception as e:
                        await ctx.send(embed=create_embed("Role Error", f"An error occurred while removing the '{FROZEN_ROLE_NAME}' role: {e}", discord.Color.red()))

            await view.close_ticket(ctx.channel, reason, ctx.author.id)
            await ctx.send(embed=create_embed("Screenshare Closed", "This screenshare ticket has been closed.", discord.Color.green()))
        else:
            await ctx.send(embed=create_embed("Not an Active Screenshare Ticket", "This is not an active screenshare ticket channel.", discord.Color.orange()))
    else:
        await ctx.send(embed=create_embed("Wrong Channel", "This command can only be used in a screenshare ticket channel.", discord.Color.red()))


# Poll Command
class PollView(discord.ui.View):
    def __init__(self, bot_instance: commands.Bot, poll_id: int, kind: str, target_id: Optional[int], creator_id: int, channel_id: int):
        super().__init__(timeout=None) # No timeout for manual close
        self.bot_instance = bot_instance
        self.poll_id = poll_id
        self.kind = kind
        self.target_id = target_id
        self.creator_id = creator_id
        self.channel_id = channel_id
        self.upvotes: set[int] = set()
        self.downvotes: set[int] = set()
        self.message: Optional[discord.Message] = None # Will be set after sending

    async def update_vote_display(self, message: discord.Message):
        """Updates the vote count displayed on the poll embed."""
        embed = message.embeds[0]
        found_votes_field = False
        for i, field in enumerate(embed.fields):
            if field.name == "Current Votes":
                embed.set_field_at(i, name="Current Votes", value=f"Upvotes: {len(self.upvotes)}\nDownvotes: {len(self.downvotes)}", inline=False)
                found_votes_field = True
                break
        if not found_votes_field:
            embed.add_field(name="Current Votes", value=f"Upvotes: {len(self.upvotes)}\nDownvotes: {len(self.downvotes)}", inline=False)
        
        await message.edit(embed=embed)

    @discord.ui.button(label="Upvote", style=discord.ButtonStyle.green, emoji="⬆️")
    async def upvote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handles upvote button clicks for polls."""
        ppp_manager_role = get_role_by_name(interaction.guild, PPP_MANAGER_ROLE_NAME)
        if not ppp_manager_role or ppp_manager_role not in interaction.user.roles:
            await interaction.response.send_message("Only PPP Managers can vote on this poll.", ephemeral=True)
            return

        # Record vote in DB
        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "INSERT INTO poll_votes (poll_id, user_id, vote_type) VALUES (%s, %s, 'upvote') "
                    "ON DUPLICATE KEY UPDATE vote_type = 'upvote'",
                    (self.poll_id, interaction.user.id)
                )
                connection.commit()
            except Error as e:
                print(f"Error recording poll vote in DB: {e}")
                await interaction.response.send_message("An error occurred while recording your vote.", ephemeral=True)
                return
            finally:
                connection.close()

        if interaction.user.id in self.downvotes:
            self.downvotes.remove(interaction.user.id)
        self.upvotes.add(interaction.user.id)
        await interaction.response.send_message("You upvoted!", ephemeral=True)
        await self.update_vote_display(interaction.message)

    @discord.ui.button(label="Downvote", style=discord.ButtonStyle.red, emoji="⬇️")
    async def downvote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handles downvote button clicks for polls."""
        ppp_manager_role = get_role_by_name(interaction.guild, PPP_MANAGER_ROLE_NAME)
        if not ppp_manager_role or ppp_manager_role not in interaction.user.roles:
            await interaction.response.send_message("Only PPP Managers can vote on this poll.", ephemeral=True)
            return

        # Record vote in DB
        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "INSERT INTO poll_votes (poll_id, user_id, vote_type) VALUES (%s, %s, 'downvote') "
                    "ON DUPLICATE KEY UPDATE vote_type = 'downvote'",
                    (self.poll_id, interaction.user.id)
                )
                connection.commit()
            except Error as e:
                print(f"Error recording poll vote in DB: {e}")
                await interaction.response.send_message("An error occurred while recording your vote.", ephemeral=True)
                return
            finally:
                connection.close()

        if interaction.user.id in self.upvotes:
            self.upvotes.remove(interaction.user.id)
        self.downvotes.add(interaction.user.id)
        await interaction.response.send_message("You downvoted!", ephemeral=True)
        await self.update_vote_display(interaction.message)


@bot.command(name="poll")
@commands.has_role(PPP_MANAGER_ROLE_NAME)
async def create_poll(ctx: commands.Context, kind: str, user: Optional[discord.User] = None):
    """Starts a new poll in the PPP voting channel."""
    ppp_voting_channel = await get_channel_by_config(ctx.guild, PPP_VOTING_CHANNEL_ID, PPP_VOTING_CHANNEL_NAME)
    if ctx.channel.id != (ppp_voting_channel.id if ppp_voting_channel else None):
        channel_mention = ppp_voting_channel.mention if ppp_voting_channel else "the PPP voting channel"
        await ctx.send(embed=create_embed("Wrong Channel", f"Please use this command in the {channel_mention} channel!", discord.Color.red()), delete_after=5)
        return

    target_id = user.id if user else None
    target_mention = user.mention if user else "N/A"
    
    embed = create_embed(
        title=f"New {kind.capitalize()} Poll",
        description=f"**Target:** {target_mention}\n**Created by:** {ctx.author.mention}\n\nVote using the buttons below!",
        color=discord.Color.blue(),
        fields=[
            {"name": "Status", "value": "Open", "inline": True},
            {"name": "Current Votes", "value": "Upvotes: 0\nDownvotes: 0", "inline": False}
        ]
    )

    connection = create_db_connection()
    poll_id = None
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO polls (kind, target_id, creator_id, channel_id, status) VALUES (%s, %s, %s, %s, %s)",
                (kind, target_id, ctx.author.id, ctx.channel.id, 'open')
            )
            poll_id = cursor.lastrowid
            connection.commit()
        except Error as e:
            print(f"Error creating poll in DB: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while creating the poll.", discord.Color.red()))
            return
        finally:
            connection.close()

    if not poll_id:
        await ctx.send(embed=create_embed("Error", "Could not create poll.", discord.Color.red()))
        return

    view = PollView(bot, poll_id, kind, target_id, ctx.author.id, ctx.channel.id)
    poll_message = await ctx.send(embed=embed, view=view)
    view.message = poll_message

    # Update poll message ID in DB
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE polls SET message_id = %s WHERE poll_id = %s",
                (poll_message.id, poll_id)
            )
            connection.commit()
        except Error as e:
            print(f"Error updating poll message ID: {e}")
        finally:
            connection.close()

    active_polls[poll_message.id] = view # Store the view instance for interaction


@bot.command(name="pollclose")
@commands.has_role(PPP_MANAGER_ROLE_NAME)
async def close_poll(ctx: commands.Context, kind: str, user: Optional[discord.User] = None):
    """Closes an active poll."""
    target_id = user.id if user else None
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                "SELECT poll_id, message_id, channel_id FROM polls WHERE kind = %s AND target_id <=> %s AND status = 'open' ORDER BY created_at DESC LIMIT 1",
                (kind, target_id)
            )
            poll_data = cursor.fetchone()
            
            if not poll_data:
                await ctx.send(embed=create_embed("Poll Not Found", f"No open '{kind}' poll found for {user.mention if user else 'anyone'}.", discord.Color.orange()))
                return

            poll_id = poll_data['poll_id']
            message_id = poll_data['message_id']
            poll_channel_id = poll_data['channel_id']

            # Update DB status
            cursor.execute(
                "UPDATE polls SET status = 'closed', closed_at = NOW() WHERE poll_id = %s",
                (poll_id,)
            )
            connection.commit()

            # Disable buttons on the message
            poll_channel = bot.get_channel(poll_channel_id)
            if poll_channel and isinstance(poll_channel, discord.TextChannel):
                try:
                    poll_message = await poll_channel.fetch_message(message_id)
                    if poll_message:
                        view = active_polls.get(message_id) # Retrieve the active view
                        if view:
                            for item in view.children:
                                item.disabled = True
                            await poll_message.edit(view=view)
                            # Update embed status
                            embed = poll_message.embeds[0]
                            for i, field in enumerate(embed.fields):
                                if field.name == "Status":
                                    embed.set_field_at(i, name="Status", value="Closed", inline=True)
                                    break
                            await poll_message.edit(embed=embed)
                            del active_polls[message_id] # Remove from active polls
                        else:
                            await ctx.send(embed=create_embed("Error", "Could not find active poll view. Buttons might not be disabled.", discord.Color.red()))
                except discord.NotFound:
                    await ctx.send(embed=create_embed("Error", "Poll message not found. It might have been deleted.", discord.Color.red()))
                except Exception as e:
                    print(f"Error disabling poll buttons: {e}")
            
            await ctx.send(embed=create_embed("Poll Closed", f"The '{kind}' poll for {user.mention if user else 'everyone'} has been closed.", discord.Color.green()))

        except Error as e:
            print(f"Error closing poll in DB: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while closing the poll.", discord.Color.red()))
        finally:
            connection.close()


@bot.command(name="mypoll")
async def my_poll(ctx: commands.Context, kind: str):
    """Shows the status of the user's most recent poll of a given kind."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                "SELECT * FROM polls WHERE kind = %s AND creator_id = %s ORDER BY created_at DESC LIMIT 1",
                (kind, ctx.author.id)
            )
            poll_data = cursor.fetchone()

            if not poll_data:
                await ctx.send(embed=create_embed("No Poll Found", f"You have no recent '{kind}' poll.", discord.Color.orange()))
                return
            
            status = poll_data['status'].capitalize()
            target_mention = f"<@{poll_data['target_id']}>" if poll_data['target_id'] else "N/A"
            
            embed = create_embed(
                title=f"Your {kind.capitalize()} Poll Status",
                description=f"**Target:** {target_mention}\n**Created:** {poll_data['created_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}",
                color=discord.Color.blue(),
                fields=[
                    {"name": "Status", "value": status, "inline": True}
                ]
            )
            if status == "Closed":
                embed.add_field(name="Closed At", value=poll_data['closed_at'].strftime('%Y-%m-%d %H:%M:%S UTC'), inline=True)
            
            await ctx.send(embed=embed)

        except Error as e:
            print(f"Error fetching my poll: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while fetching your poll.", discord.Color.red()))
        finally:
            connection.close()


@bot.command(name="myvote")
async def my_vote(ctx: commands.Context, kind: str):
    """Shows who voted on a specific poll (visible to poll creator and PPP Manager)."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            # Get the latest poll of that kind
            cursor.execute(
                "SELECT poll_id, message_id, creator_id FROM polls WHERE kind = %s ORDER BY created_at DESC LIMIT 1",
                (kind,)
            )
            poll_info = cursor.fetchone()

            if not poll_info:
                await ctx.send(embed=create_embed("No Poll Found", f"No '{kind}' poll found.", discord.Color.orange()))
                return
            
            poll_id = poll_info['poll_id']
            poll_creator_id = poll_info['creator_id']

            # Check if user is PPP Manager or the poll creator
            is_ppp_manager = get_role_by_name(ctx.guild, PPP_MANAGER_ROLE_NAME) in ctx.author.roles if get_role_by_name(ctx.guild, PPP_MANAGER_ROLE_NAME) else False
            is_creator = ctx.author.id == poll_creator_id

            if not (is_ppp_manager or is_creator):
                await ctx.send(embed=create_embed("Permission Denied", "You need to be the poll creator or a PPP Manager to see who voted.", discord.Color.red()))
                return

            # Fetch votes
            cursor.execute(
                "SELECT user_id, vote_type FROM poll_votes WHERE poll_id = %s",
                (poll_id,)
            )
            votes = cursor.fetchall()

            upvoters = []
            downvoters = []
            for vote in votes:
                user = bot.get_user(vote['user_id']) or await bot.fetch_user(vote['user_id'])
                if user:
                    if vote['vote_type'] == 'upvote':
                        upvoters.append(user.display_name)
                    else:
                        downvoters.append(user.display_name)
            
            embed = create_embed(
                title=f"Votes for {kind.capitalize()} Poll (ID: {poll_id})",
                description=f"Poll created by <@{poll_creator_id}>",
                color=discord.Color.blue(),
                fields=[
                    {"name": "Upvotes", "value": "\n".join(upvoters) or "None", "inline": True},
                    {"name": "Downvotes", "value": "\n".join(downvoters) or "None", "inline": True}
                ]
            )
            await ctx.send(embed=embed)

        except Error as e:
            print(f"Error fetching my vote: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while fetching vote details.", discord.Color.red()))
        finally:
            connection.close()


# History Command
@bot.command(name="h", aliases=["history"])
async def show_history(ctx: commands.Context, user: Optional[discord.User] = None):
    """Shows all past strikes, bans, and mutes of a person in paginated embeds."""
    target_user = user or ctx.author
    discord_id = target_user.id
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Fetch bans
            cursor.execute(
                "SELECT 'Ban' as type, reason, issued_at, expires_at, active, removed_at, removed_reason FROM bans WHERE discord_id = %s ORDER BY issued_at DESC",
                (discord_id,)
            )
            bans = cursor.fetchall()

            # Fetch mutes
            cursor.execute(
                "SELECT 'Mute' as type, reason, issued_at, expires_at, active, removed_at, removed_reason FROM mutes WHERE discord_id = %s ORDER BY issued_at DESC",
                (discord_id,)
            )
            mutes = cursor.fetchall()

            # Fetch strikes
            cursor.execute(
                "SELECT 'Strike' as type, reason, issued_at, removed_at, removed_reason FROM strikes WHERE discord_id = %s ORDER BY issued_at DESC",
                (discord_id,)
            )
            strikes = cursor.fetchall()

            all_history = []
            for item in bans + mutes + strikes:
                all_history.append(item)
            
            # Sort by issued_at (latest first)
            all_history.sort(key=lambda x: x['issued_at'], reverse=True)

            if not all_history:
                await ctx.send(embed=create_embed("History", f"No history found for {target_user.mention}.", discord.Color.blue()))
                return
            
            # Pagination
            page_size = 5
            pages = [all_history[i:i + page_size] for i in range(0, len(all_history), page_size)]
            
            current_page = 0

            async def send_history_page(page_num: int):
                page = pages[page_num]
                embed = create_embed(
                    title=f"History for {target_user.display_name} (Page {page_num + 1}/{len(pages)})",
                    description="Past punishments and strikes.",
                    color=discord.Color.blue()
                )
                
                for item in page:
                    title = f"{item['type']} - {item['issued_at'].strftime('%Y-%m-%d')}"
                    description_lines = [f"**Reason:** {item['reason']}"]
                    if 'expires_at' in item and item['expires_at']:
                        description_lines.append(f"**Expires:** {item['expires_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
                    if 'active' in item:
                        description_lines.append(f"**Status:** {'Active' if item['active'] else 'Inactive'}")
                    if item.get('removed_at'):
                        description_lines.append(f"**Removed:** {item['removed_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
                        if item.get('removed_reason'):
                            description_lines.append(f"**Removed Reason:** {item['removed_reason']}")
                    
                    embed.add_field(name=title, value="\n".join(description_lines), inline=False)
                
                return embed

            message = await ctx.send(embed=await send_history_page(current_page))
            
            if len(pages) > 1:
                await message.add_reaction("⬅️")
                await message.add_reaction("➡️")

                def check_reaction(reaction: discord.Reaction, user_react: discord.User):
                    return user_react == ctx.author and str(reaction.emoji) in ["⬅️", "➡️"] and reaction.message == message

                while True:
                    try:
                        reaction, user_react = await bot.wait_for("reaction_add", timeout=60.0, check=check_reaction)
                        
                        if str(reaction.emoji) == "➡️":
                            current_page = (current_page + 1) % len(pages)
                        elif str(reaction.emoji) == "⬅️":
                            current_page = (current_page - 1) % len(pages)
                        
                        await message.edit(embed=await send_history_page(current_page))
                        await message.remove_reaction(reaction, user_react)
                    except asyncio.TimeoutError:
                        await message.clear_reactions()
                        break
        except Error as e:
            print(f"Error fetching history: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while fetching history.", discord.Color.red()))
        finally:
            connection.close()

# Player Info Card
@bot.command(name="i")
async def player_info(ctx: commands.Context, user: Optional[discord.User] = None):
    """Generates and displays a monochrome info card for a player."""
    target_user = user or ctx.author
    discord_id = target_user.id
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                "SELECT minecraft_ign, elo, wins, losses, mvps, streak FROM users WHERE discord_id = %s AND verified = TRUE",
                (discord_id,)
            )
            stats = cursor.fetchone()
            
            if not stats:
                await ctx.send(embed=create_embed("Player Not Found", "Player not found or not registered.", discord.Color.orange()))
                return
            
            wlr = stats["wins"] / stats["losses"] if stats["losses"] > 0 else stats["wins"]
            
            image_file = await generate_player_info_image(stats["minecraft_ign"], stats["elo"], 
                                             stats["wins"], stats["losses"], wlr, 
                                             stats["mvps"], stats["streak"])
            
            await ctx.send(file=image_file)
        except Error as e:
            print(f"Error getting player stats: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while fetching player stats.", discord.Color.red()))
        except Exception as e:
            print(f"Error generating player info image: {e}")
            await ctx.send(embed=create_embed("Image Error", "An error occurred while generating the info card.", discord.Color.red()))
        finally:
            connection.close()

# Leaderboard command
@bot.command(name="lb", aliases=["leaderboard"])
async def leaderboard(ctx: commands.Context, category: str = "elo"):
    """Displays various leaderboards (ELO, wins, losses, MVPs, streaks, games played)."""
    valid_categories = ["elo", "wins", "losses", "mvps", "streaks", "games"]
    
    if category not in valid_categories:
        await ctx.send(embed=create_embed("Invalid Category", f"Invalid category! Available: {', '.join(valid_categories)}", discord.Color.red()))
        return
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            
            if category == "games":
                query = """
                    SELECT discord_id, minecraft_ign, elo, (wins + losses) as games_played 
                    FROM users 
                    WHERE verified = TRUE 
                    ORDER BY games_played DESC 
                    LIMIT 10
                """
                cursor.execute(query)
                top_players = cursor.fetchall()
                display_key = "games_played"
            else:
                query = f"""
                    SELECT discord_id, minecraft_ign, elo, {category} 
                    FROM users 
                    WHERE verified = TRUE 
                    ORDER BY {category} DESC 
                    LIMIT 10
                """
                cursor.execute(query)
                top_players = cursor.fetchall()
                display_key = category
            
            if not top_players:
                await ctx.send(embed=create_embed("Leaderboard Empty", "No players found in the leaderboard!", discord.Color.orange()))
                return
            
            embed = create_embed(
                title=f"Leaderboard - {category.replace('_', ' ').capitalize()}",
                description="Top 10 Players:",
                color=discord.Color.gold()
            )
            
            for i, player in enumerate(top_players, 1):
                embed.add_field(
                    name=f"{i}. {player['minecraft_ign']}",
                    value=f"**{player[display_key]}** (ELO: {player['elo']})",
                    inline=False
                )
            
            await ctx.send(embed=embed)
        except Error as e:
            print(f"Error getting leaderboard: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while fetching the leaderboard.", discord.Color.red()))
        finally:
            connection.close()

# Admin Commands Group
@bot.group(name="admin")
@commands.has_role(PI_ROLE_NAME)
async def admin_commands(ctx: commands.Context):
    """Group of commands for server administration, restricted to PI role."""
    if ctx.invoked_subcommand is None:
        await ctx.send(embed=create_embed("Admin Commands", "Available subcommands: `set`, `purgeall`, `wins`, `losses`, `elochange`, `elo`, `vg`, `undo`, `rescore`, `score`.", discord.Color.blue()))

@admin_commands.command(name="set")
async def admin_set(ctx: commands.Context, setting: str, value: str):
    """Sets various game mechanics settings (party size, queue status, active queues)."""
    global party_size, queue_status, active_queues
    
    setting = setting.lower()
    
    if setting == "partysize":
        if value.lower() == "none":
            party_size = None
        else:
            try:
                new_party_size = int(value)
                if new_party_size not in [2, 3, 4]:
                    raise ValueError
                party_size = new_party_size
            except ValueError:
                await ctx.send(embed=create_embed("Invalid Value", "Invalid party size! Use `none`, `2`, `3`, or `4`.", discord.Color.red()))
                return
            
        await ctx.send(embed=create_embed("Setting Updated", f"Party size set to: `{value}`", discord.Color.green()))
            
    elif setting == "queue":
        if value == "0":
            queue_status = False
            status_str = "closed"
        elif value == "1":
            queue_status = True
            status_str = "open"
        else:
            await ctx.send(embed=create_embed("Invalid Value", "Invalid value for queue status! Use `0` (closed) or `1` (open).", discord.Color.red()))
            return
            
        await ctx.send(embed=create_embed("Setting Updated", f"Queues are now `{status_str}`!", discord.Color.green()))
                
    elif setting == "queues":
        valid_queue_settings = ["3s4s", "3s", "4s"]
        if value.lower() not in valid_queue_settings:
            await ctx.send(embed=create_embed("Invalid Value", f"Invalid queue setting! Use: {', '.join(valid_queue_settings)}", discord.Color.red()))
            return
            
        if value.lower() == "3s4s":
            active_queues = ["3v3", "4v4"]
        elif value.lower() == "3s":
            active_queues = ["3v3"]
        elif value.lower() == "4s":
            active_queues = ["4v4"]
            
        await ctx.send(embed=create_embed("Setting Updated", f"Active queues set to: `{', '.join(active_queues)}`", discord.Color.green()))
    else:
        await ctx.send(embed=create_embed("Unknown Setting", "Unknown setting. Available settings: `partysize`, `queue`, `queues`.", discord.Color.red()))

@admin_commands.command(name="purgeall")
async def admin_purgeall(ctx: commands.Context):
    """Wipes all player stats and ELO from the database."""
    confirm_msg = await ctx.send(embed=create_embed("Confirm Purge", "Are you sure you want to purge ALL stats? This cannot be undone! React with ✅ to confirm.", discord.Color.orange()))
    await confirm_msg.add_reaction("✅")
    
    def check(reaction: discord.Reaction, user: discord.User):
        return user == ctx.author and str(reaction.emoji) == "✅" and reaction.message.id == confirm_msg.id
        
    try:
        await bot.wait_for('reaction_add', timeout=30.0, check=check)
    except asyncio.TimeoutError:
        await confirm_msg.edit(embed=create_embed("Purge Cancelled", "Purge cancelled due to timeout.", discord.Color.red()))
        return
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("UPDATE users SET elo = 0, wins = 0, losses = 0, mvps = 0, streak = 0")
            connection.commit()
            
            guild = ctx.guild
            for member in guild.members:
                if await is_registered(member.id): # Only update registered members
                    await update_elo_role(member.id, 0) # Reset to Iron role
            
            await ctx.send(embed=create_embed("Stats Purged", "All player stats have been purged and ELO roles reset!", discord.Color.green()))
        except Error as e:
            print(f"Error purging stats: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while purging stats.", discord.Color.red()))
        finally:
            connection.close()

# Modify Stats Commands (under admin group)
@admin_commands.command(name="wins")
@commands.has_any_role(MANAGER_ROLE_NAME, "Administrator", PI_ROLE_NAME)
async def admin_wins(ctx: commands.Context, user: discord.User, amount: int, mvp: int = 0):
    """Adds a specified number of wins to a user's stats."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE users SET wins = wins + %s WHERE discord_id = %s",
                (amount, user.id)
            )
            if mvp == 1:
                cursor.execute(
                    "UPDATE users SET mvps = mvps + 1 WHERE discord_id = %s",
                    (user.id,)
                )
            connection.commit()
            
            current_elo = await get_player_elo(user.id)
            await update_elo_role(user.id, current_elo) # Update role/nickname
            
            await ctx.send(embed=create_embed("Stats Modified", f"Added {amount} wins to {user.mention}. MVP: {'Yes' if mvp == 1 else 'No'}.", discord.Color.green()))
        except Error as e:
            print(f"Error modifying wins: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while modifying wins.", discord.Color.red()))
        finally:
            connection.close()

@admin_commands.command(name="losses")
@commands.has_any_role(MANAGER_ROLE_NAME, "Administrator", PI_ROLE_NAME)
async def admin_losses(ctx: commands.Context, user: discord.User, amount: int, mvp: int = 0):
    """Adds a specified number of losses to a user's stats."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE users SET losses = losses + %s WHERE discord_id = %s",
                (amount, user.id)
            )
            if mvp == 1:
                cursor.execute(
                    "UPDATE users SET mvps = mvps + 1 WHERE discord_id = %s",
                    (user.id,)
                )
            connection.commit()
            
            current_elo = await get_player_elo(user.id)
            await update_elo_role(user.id, current_elo) # Update role/nickname
            
            await ctx.send(embed=create_embed("Stats Modified", f"Added {amount} losses to {user.mention}. MVP: {'Yes' if mvp == 1 else 'No'}.", discord.Color.green()))
        except Error as e:
            print(f"Error modifying losses: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while modifying losses.", discord.Color.red()))
        finally:
            connection.close()

@admin_commands.command(name="elochange")
@commands.has_any_role(MANAGER_ROLE_NAME, "Administrator", PI_ROLE_NAME)
async def admin_elochange(ctx: commands.Context, user: discord.User, amount: int):
    """Changes a user's ELO by a specified amount."""
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE users SET elo = elo + %s WHERE discord_id = %s",
                (amount, user.id)
            )
            connection.commit()
            
            current_elo = await get_player_elo(user.id)
            await update_elo_role(user.id, current_elo) # Update role/nickname
            
            await ctx.send(embed=create_embed("ELO Modified", f"Changed ELO for {user.mention} by {amount}. New ELO: {current_elo}", discord.Color.green()))
        except Error as e:
            print(f"Error modifying ELO: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while modifying ELO.", discord.Color.red()))
        finally:
            connection.close()

@admin_commands.command(name="elo")
@commands.has_any_role(MANAGER_ROLE_NAME, "Administrator", PI_ROLE_NAME)
async def admin_elo_set(ctx: commands.Context, user: discord.User, set_keyword: str, value: int):
    """Sets a user's ELO to a specific value."""
    if set_keyword.lower() != "set":
        await ctx.send(embed=create_embed("Invalid Usage", "Usage: `=admin elo <user> set <value>`", discord.Color.red()))
        return
    
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE users SET elo = %s WHERE discord_id = %s",
                (value, user.id)
            )
            connection.commit()
            
            await update_elo_role(user.id, value) # Update role/nickname
            
            await ctx.send(embed=create_embed("ELO Set", f"Set ELO for {user.mention} to {value}.", discord.Color.green()))
        except Error as e:
            print(f"Error setting ELO: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while setting ELO.", discord.Color.red()))
        finally:
            connection.close()

# Modify Game Results Commands (under admin group)
@admin_commands.command(name="vg")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_view_game(ctx: commands.Context, game_no: int):
    """Displays details of a specific game."""
    game_data = await get_game_data_from_db(game_no)
    
    if not game_data:
        await ctx.send(embed=create_embed("Game Not Found", f"Game #{game_no} not found in the database.", discord.Color.orange()))
        return
    
    team1_mentions = [f"<@{p}>" for p in game_data['team1_players']]
    team2_mentions = [f"<@{p}>" for p in game_data['team2_players']]
    
    embed = create_embed(
        title=f"Game #{game_no:04d} Details",
        description=f"**Queue Type:** {game_data['queue_type']}\n**Status:** {game_data['status'].capitalize()}",
        color=discord.Color.blue(),
        fields=[
            {"name": "Team 1", "value": "\n".join(team1_mentions) or "No players", "inline": True},
            {"name": "Team 2", "value": "\n".join(team2_mentions) or "No players", "inline": True}
        ]
    )
    
    if game_data['status'] == 'completed':
        mvp_mention = f"<@{game_data['mvp_player']}>" if game_data['mvp_player'] else "N/A"
        
        scored_by_display = "Bot"
        if game_data['scored_by'] and game_data['scored_by'].isdigit():
            scored_by_user = bot.get_user(int(game_data['scored_by'])) or await bot.fetch_user(int(game_data['scored_by']))
            scored_by_display = scored_by_user.display_name if scored_by_user else f"Unknown User ({game_data['scored_by']})"
        elif game_data['scored_by']:
            scored_by_display = game_data['scored_by'] # "bot" string

        embed.add_field(name="Winning Team", value=f"Team {game_data['winning_team']}", inline=True)
        embed.add_field(name="MVP", value=mvp_mention, inline=True)
        embed.add_field(name="Scored By", value=scored_by_display, inline=False)
        embed.add_field(name="Completed At", value=game_data['completed_at'].strftime('%Y-%m-%d %H:%M:%S UTC'), inline=False)

    await ctx.send(embed=embed)


@admin_commands.command(name="undo")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_undo_game(ctx: commands.Context, game_no: int):
    """Undoes a completed game, reverting player stats and ELO."""
    game_data = await get_game_data_from_db(game_no)

    if not game_data or game_data['status'] != 'completed':
        await ctx.send(embed=create_embed("Undo Failed", f"Game #{game_no} is not completed or does not exist.", discord.Color.orange()))
        return

    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            
            winning_team = game_data['winning_team']
            mvp_player_id = game_data['mvp_player']
            all_game_players = game_data['team1_players'] + game_data['team2_players']

            for player_id in all_game_players:
                current_elo = await get_player_elo(player_id)
                elo_role_name = await get_elo_role_name(current_elo)
                
                elo_change = 0
                # Revert ELO, wins, losses, mvps
                if player_id in game_data['teams'][winning_team]:
                    elo_change = -ELO_REWARDS[elo_role_name]["win"] # Revert win ELO
                    cursor.execute(
                        "UPDATE users SET wins = wins - 1 WHERE discord_id = %s",
                        (player_id,)
                    )
                else:
                    elo_change = ELO_REWARDS[elo_role_name]["loss"] # Revert loss ELO (add back)
                    cursor.execute(
                        "UPDATE users SET losses = losses - 1 WHERE discord_id = %s",
                        (player_id,)
                    )
                
                if player_id == mvp_player_id:
                    elo_change -= ELO_REWARDS[elo_role_name]["mvp"] # Revert MVP ELO
                    cursor.execute(
                        "UPDATE users SET mvps = mvps - 1 WHERE discord_id = %s",
                        (player_id,)
                    )
                
                cursor.execute(
                    "UPDATE users SET elo = elo + %s WHERE discord_id = %s",
                    (elo_change, player_id)
                )
                
                # Revert streak - Simplification: reset streak to 0.
                cursor.execute(
                    "UPDATE users SET streak = 0 WHERE discord_id = %s",
                    (player_id,)
                )
            
            # Update game status to cancelled
            cursor.execute(
                "UPDATE games SET status = 'cancelled', winning_team = NULL, mvp_player = NULL, completed_at = NULL, scored_by = %s WHERE game_id = %s",
                (str(ctx.author.id), game_no) # Mark as cancelled by admin
            )
            connection.commit()

            # Update ELO roles for all players involved
            for player_id in all_game_players:
                new_elo = await get_player_elo(player_id)
                await update_elo_role(player_id, new_elo)
            
            await ctx.send(embed=create_embed("Game Undo", f"Game #{game_no} has been successfully undone. Player stats and ELOs reverted.", discord.Color.green()))
        except Error as e:
            print(f"Error undoing game: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while undoing the game.", discord.Color.red()))
        finally:
            connection.close()


@admin_commands.command(name="rescore")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_rescore_game(ctx: commands.Context, game_no: int, winning_team: int, mvp_user: discord.User):
    """Rescores a completed game with new results."""
    game_data = await get_game_data_from_db(game_no)

    if not game_data:
        await ctx.send(embed=create_embed("Game Not Found", f"Game #{game_no} not found in the database.", discord.Color.orange()))
        return

    if winning_team not in [1, 2]:
        await ctx.send(embed=create_embed("Invalid Team", "Winning team must be 1 or 2.", discord.Color.red()))
        return
    
    all_game_players = game_data['team1_players'] + game_data['team2_players']
    if mvp_user.id not in all_game_players:
        await ctx.send(embed=create_embed("Invalid MVP", f"{mvp_user.mention} was not a player in Game #{game_no}.", discord.Color.red()))
        return

    # First, undo the game if it was already scored
    if game_data['status'] == 'completed':
        await admin_undo_game(ctx, game_no)
        await asyncio.sleep(1) # Give a moment for undo to process

    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            
            # Set game status to completed with new results
            cursor.execute(
                "UPDATE games SET status = 'completed', winning_team = %s, mvp_player = %s, completed_at = NOW(), scored_by = %s WHERE game_id = %s",
                (winning_team, mvp_user.id, str(ctx.author.id), game_no) # Scored by admin
            )

            # Recalculate ELO, wins, losses, mvps, streak based on new results
            for player_id in all_game_players: # Iterate all players from the game
                current_elo = await get_player_elo(player_id)
                elo_role_name = await get_elo_role_name(current_elo)
                
                elo_change = 0
                # Determine if player was in the winning team based on the new winning_team
                player_was_in_winning_team = (player_id in game_data[f'team{winning_team}_players'])

                if player_was_in_winning_team:
                    elo_change = ELO_REWARDS[elo_role_name]["win"]
                    cursor.execute(
                        "UPDATE users SET wins = wins + 1, last_played = NOW() WHERE discord_id = %s",
                        (player_id,)
                    )
                    await update_streak(player_id, True)
                else:
                    elo_change = -ELO_REWARDS[elo_role_name]["loss"]
                    cursor.execute(
                        "UPDATE users SET losses = losses + 1, last_played = NOW() WHERE discord_id = %s",
                        (player_id,)
                    )
                    await update_streak(player_id, False)
                
                if player_id == mvp_user.id:
                    elo_change += ELO_REWARDS[elo_role_name]["mvp"]
                    cursor.execute(
                        "UPDATE users SET mvps = mvps + 1 WHERE discord_id = %s",
                        (player_id,)
                    )
                
                cursor.execute(
                    "UPDATE users SET elo = elo + %s WHERE discord_id = %s",
                    (elo_change, player_id)
                )
            
            connection.commit()

            # Update ELO roles for all players involved
            for player_id in all_game_players:
                new_elo = await get_player_elo(player_id)
                await update_elo_role(player_id, new_elo)
            
            await ctx.send(embed=create_embed("Game Rescored", f"Game #{game_no} has been successfully rescored. Winning Team: {winning_team}, MVP: {mvp_user.mention}.", discord.Color.green()))
        except Error as e:
            print(f"Error rescoring game: {e}")
            await ctx.send(embed=create_embed("Database Error", "An error occurred while rescoring the game.", discord.Color.red()))
        finally:
            connection.close()


@admin_commands.command(name="score")
@commands.has_any_role(ADMIN_STAFF_ROLE_NAME, "Administrator", MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def admin_score_game(ctx: commands.Context, game_no: int, winning_team: int, mvp_user: discord.User):
    """Manually scores an uncompleted game."""
    game_data = await get_game_data_from_db(game_no)

    if not game_data:
        await ctx.send(embed=create_embed("Game Not Found", f"Game #{game_no} not found in the database.", discord.Color.orange()))
        return

    if game_data['status'] == 'completed':
        await ctx.send(embed=create_embed("Game Already Scored", f"Game #{game_no} is already scored. Use `=admin rescore` to change results.", discord.Color.orange()))
        return

    if winning_team not in [1, 2]:
        await ctx.send(embed=create_embed("Invalid Team", "Winning team must be 1 or 2.", discord.Color.red()))
        return
    
    all_game_players = game_data['team1_players'] + game_data['team2_players']
    if mvp_user.id not in all_game_players:
        await ctx.send(embed=create_embed("Invalid MVP", f"{mvp_user.mention} was not a player in Game #{game_no}.", discord.Color.red()))
        return

    # To use the existing end_game logic, we need to temporarily put the game data into active_games
    temp_active_game_data = {
        "channel_id": game_data['channel_id'],
        "voice_channel_id": game_data['voice_channel_id'],
        "players": all_game_players, # Reconstruct players list
        "queue_type": game_data['queue_type'],
        "status": game_data['status'],
        "teams": {1: game_data['team1_players'], 2: game_data['team2_players']},
        "captains": [], # Not relevant for scoring
        "db_game_id": game_no # Crucial for DB updates
    }
    active_games[game_no] = temp_active_game_data

    try:
        # Call end_game with scored_by_bot=False to indicate manual scoring
        await end_game(game_no, winning_team, mvp_user.id, scored_by_bot=False) 
        await ctx.send(embed=create_embed("Game Scored", f"Game #{game_no} has been manually scored. Winning Team: {winning_team}, MVP: {mvp_user.mention}.", discord.Color.green()))
    except Exception as e:
        print(f"Error manually scoring game: {e}")
        await ctx.send(embed=create_embed("Error", "An error occurred while manually scoring the game.", discord.Color.red()))
    finally:
        # Clean up temp entry from active_games
        if game_no in active_games and active_games[game_no] == temp_active_game_data:
            del active_games[game_no] 

# Ticket System
class TicketView(discord.ui.View):
    def __init__(self, bot_instance: commands.Bot, ticket_id: int, requester_id: int, channel_id: int):
        super().__init__(timeout=None) # No timeout for manual close
        self.bot_instance = bot_instance
        self.ticket_id = ticket_id
        self.requester_id = requester_id
        self.channel_id = channel_id
        self.message: Optional[discord.Message] = None # Will be set after sending

    async def close_ticket(self, channel: discord.TextChannel, reason: str, closed_by_id: Optional[int] = None):
        """Closes the ticket, moves it to the closed category, and logs."""
        # Disable buttons
        if self.message:
            for item in self.children:
                item.disabled = True
            await self.message.edit(view=self)

        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "UPDATE tickets SET status = 'closed', closed_by = %s, closed_at = NOW(), close_reason = %s WHERE ticket_id = %s",
                    (closed_by_id or self.bot_instance.user.id, reason, self.ticket_id)
                )
                connection.commit()
            except Error as e:
                print(f"Error closing ticket in DB: {e}")
            finally:
                connection.close()

        closed_tickets_category = await get_channel_or_create_category(channel.guild, CLOSED_TICKETS_CATEGORY_ID, "Closed Tickets", is_category=True)

        if not closed_tickets_category:
            print("Warning: Could not find or create 'Closed Tickets' category for ticket log.")

        try:
            # Overwrites for closed tickets: only staff can read
            overwrites = {
                channel.guild.default_role: discord.PermissionOverwrite(read_messages=False),
                self.bot_instance.get_user(self.requester_id) or await self.bot_instance.fetch_user(self.requester_id): discord.PermissionOverwrite(read_messages=False),
                channel.guild.me: discord.PermissionOverwrite(read_messages=True)
            }
            # Add all staff roles to overwrites for closed tickets
            staff_roles_to_add = [STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME]
            for role_name in staff_roles_to_add:
                role = get_role_by_name(channel.guild, role_name)
                if role:
                    overwrites[role] = discord.PermissionOverwrite(read_messages=True)

            await channel.edit(category=closed_tickets_category, overwrites=overwrites)
            await channel.send(embed=create_embed("Ticket Closed", f"This ticket has been closed. Reason: {reason}", discord.Color.red()))
        except Exception as e:
            print(f"Error moving/editing ticket channel: {e}")

        # Log to HTML
        requester_user_obj = self.bot_instance.get_user(self.requester_id) or await self.bot_instance.fetch_user(self.requester_id)
        closed_by_user_obj = self.bot_instance.get_user(closed_by_id) or await self.bot_instance.fetch_user(closed_by_id) if closed_by_id else None

        log_html = f"""
        <p><strong>Ticket ID:</strong> {self.ticket_id}</p>
        <p><strong>Requester:</strong> {requester_user_obj.display_name if requester_user_obj else 'Unknown'} ({self.requester_id})</p>
        <p><strong>Reason for Closure:</strong> {html.escape(reason)}</p>
        <p><strong>Closed By:</strong> {closed_by_user_obj.display_name if closed_by_user_obj else 'Bot'} ({closed_by_id or self.bot_instance.user.id})</p>
        <p><strong>Closed At:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        """
        await log_to_html_channel(channel.guild, TICKET_LOG_CHANNEL_ID, TICKET_LOG_CHANNEL_NAME, f"Ticket Log - ID {self.ticket_id}", log_html)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.green)
    async def claim_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Allows staff members to claim an open ticket."""
        staff_role = get_role_by_name(interaction.guild, STAFF_ROLE_NAME)
        if not staff_role or staff_role not in interaction.user.roles:
            await interaction.response.send_message("You do not have permission to claim tickets.", ephemeral=True)
            return

        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "UPDATE tickets SET status = 'claimed', claimed_by = %s, claimed_at = NOW() WHERE ticket_id = %s AND status = 'open'",
                    (interaction.user.id, self.ticket_id)
                )
                connection.commit()
                if cursor.rowcount == 0:
                    await interaction.response.send_message("This ticket is already claimed or closed.", ephemeral=True)
                    return
            except Error as e:
                print(f"Error claiming ticket in DB: {e}")
                await interaction.response.send_message("An error occurred while claiming the ticket.", ephemeral=True)
                return
            finally:
                connection.close()

        await interaction.response.send_message(f"{interaction.user.mention} has claimed this ticket.", ephemeral=False)
        button.disabled = True
        await interaction.message.edit(view=self)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.red)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Allows the requester or staff to close a ticket."""
        is_staff = any(get_role_by_name(interaction.guild, role_name) in interaction.user.roles for role_name in [STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME])
        if interaction.user.id != self.requester_id and not is_staff:
            await interaction.response.send_message("You do not have permission to close this ticket.", ephemeral=True)
            return
        
        # Ask for reason if not staff or if staff wants to provide one
        if not is_staff: # If it's the requester, or staff who wants to provide a reason via modal
            await interaction.response.send_modal(TicketCloseModal(self))
        else: # If staff closes without modal (e.g., direct close)
            await self.close_ticket(interaction.channel, f"Closed by {interaction.user.display_name}.", interaction.user.id)
            await interaction.response.send_message("Ticket closed.", ephemeral=True)


class TicketCloseModal(discord.ui.Modal, title="Close Ticket"):
    """Modal for providing a reason when closing a ticket."""
    def __init__(self, view: TicketView):
        super().__init__()
        self.view = view
        self.reason = discord.ui.TextInput(
            label="Reason for closing",
            placeholder="Optional reason...",
            required=False,
            style=discord.TextStyle.paragraph
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        reason_text = self.reason.value if self.reason.value else "No reason provided."
        await self.view.close_ticket(interaction.channel, reason_text, interaction.user.id)
        await interaction.response.send_message("Ticket closed.", ephemeral=True)


@bot.group(name="ticket")
async def ticket_group(ctx: commands.Context):
    """Group of commands for the ticket system."""
    if ctx.invoked_subcommand is None:
        await ctx.send(embed=create_embed("Ticket Commands", "Available subcommands: `create`", discord.Color.blue()))


@ticket_group.command(name="create")
async def ticket_create(ctx: commands.Context, ticket_type: str):
    """Creates a new support ticket."""
    allowed_types = ['general', 'appeal', 'store', 'screenshareappeal', 'ssappeal']
    ticket_type = ticket_type.lower()

    ticket_creation_channel = await get_channel_by_config(ctx.guild, TICKET_CHANNEL_ID, TICKET_CHANNEL_NAME)
    if ctx.channel.id != (ticket_creation_channel.id if ticket_creation_channel else None):
        channel_mention = ticket_creation_channel.mention if ticket_creation_channel else "the tickets channel"
        await ctx.send(embed=create_embed("Wrong Channel", f"Please use this command in the {channel_mention} channel!", discord.Color.red()), delete_after=5)
        return

    if ticket_type not in allowed_types:
        embed = create_embed(
            "Invalid Ticket Type",
            f"Available ticket types: {', '.join(allowed_types)}",
            discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=3)
        return

    # Use 'screenshareappeal' as the canonical type if 'ssappeal' is used
    if ticket_type == 'ssappeal':
        ticket_type = 'screenshareappeal'

    guild = ctx.guild
    ticket_category = await get_channel_or_create_category(guild, TICKET_CATEGORY_ID, "Tickets", is_category=True)
    if not ticket_category:
        await ctx.send(embed=create_embed("Error", "Could not find or create 'Tickets' category.", discord.Color.red()))
        return

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    # Add staff roles to overwrites
    staff_roles_to_add = [STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME]
    for role_name in staff_roles_to_add:
        role = get_role_by_name(guild, role_name)
        if role:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

    ticket_channel = await guild.create_text_channel(
        f"{ticket_type}-{ctx.author.name}-{random.randint(100,999)}",
        category=ticket_category,
        overwrites=overwrites
    )

    connection = create_db_connection()
    ticket_id = None
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "INSERT INTO tickets (requester_id, type, channel_id) VALUES (%s, %s, %s)",
                (ctx.author.id, ticket_type, ticket_channel.id)
            )
            ticket_id = cursor.lastrowid
            connection.commit()
        except Error as e:
            print(f"Error creating ticket in DB: {e}")
            await ticket_channel.send(embed=create_embed("Database Error", "An error occurred while creating your ticket.", discord.Color.red()))
            await ticket_channel.delete()
            return
        finally:
            connection.close()
    
    if not ticket_id:
        await ctx.send(embed=create_embed("Error", "Could not create ticket.", discord.Color.red()))
        return

    embed = create_embed(
        title=f"{ticket_type.capitalize()} Ticket Created",
        description=f"Thank you for creating a ticket, {ctx.author.mention}! Please describe your issue here. A staff member will be with you shortly.",
        color=discord.Color.blue(),
        fields=[
            {"name": "Ticket ID", "value": ticket_id, "inline": True},
            {"name": "Type", "value": ticket_type.capitalize(), "inline": True}
        ]
    )
    view = TicketView(bot, ticket_id, ctx.author.id, ticket_channel.id)
    ticket_message = await ticket_channel.send(embed=embed, view=view)
    view.message = ticket_message # Store message for later editing

    # Update ticket message ID in DB (for later retrieval if bot restarts)
    connection = create_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE tickets SET message_id = %s WHERE ticket_id = %s",
                (ticket_message.id, ticket_id)
            )
            connection.commit()
        except Error as e:
            print(f"Error updating ticket message ID: {e}")
        finally:
            connection.close()

    await ctx.send(embed=create_embed("Ticket Created", f"Your ticket has been created in {ticket_channel.mention}.", discord.Color.green()))


@bot.command(name="claim")
@commands.has_any_role(STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME)
async def claim_ticket_command(ctx: commands.Context, *, reason: str = "No reason provided."):
    """Claims an open ticket."""
    ticket_category = await get_channel_by_config(ctx.guild, TICKET_CATEGORY_ID, "Tickets")
    if ctx.channel.category and (ctx.channel.category.id == (ticket_category.id if ticket_category else None) or ctx.channel.category.name == "Tickets"):
        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor(dictionary=True)
                cursor.execute(
                    "SELECT ticket_id, requester_id, message_id FROM tickets WHERE channel_id = %s AND status = 'open'",
                    (ctx.channel.id,)
                )
                ticket_data = cursor.fetchone()

                if not ticket_data:
                    await ctx.send(embed=create_embed("Claim Failed", "This is not an open ticket channel.", discord.Color.orange()))
                    return
                
                ticket_id = ticket_data['ticket_id']
                requester_id = ticket_data['requester_id']
                message_id = ticket_data['message_id']

                view = TicketView(bot, ticket_id, requester_id, ctx.channel.id)
                original_message = None
                if message_id:
                    try:
                        original_message = await ctx.channel.fetch_message(message_id)
                        view.message = original_message
                    except discord.NotFound:
                        await ctx.send(embed=create_embed("Error", "Original ticket message not found. Cannot claim via button interaction.", discord.Color.red()))
                        # Proceed with DB update even if message not found
                
                # Update DB status
                cursor.execute(
                    "UPDATE tickets SET status = 'claimed', claimed_by = %s, claimed_at = NOW() WHERE ticket_id = %s",
                    (ctx.author.id, ticket_id)
                )
                connection.commit()
                
                if original_message:
                    # Manually disable the button on the original message's view
                    for item in view.children:
                        if item.label == "Claim": # Assuming the claim button has this label
                            item.disabled = True
                            break
                    await original_message.edit(view=view)

                await ctx.send(embed=create_embed("Ticket Claimed", f"Ticket #{ticket_id} has been claimed by {ctx.author.mention}. Reason: {reason}", discord.Color.green()))

            except Error as e:
                print(f"Error claiming ticket via command: {e}")
                await ctx.send(embed=create_embed("Database Error", "An error occurred while claiming the ticket.", discord.Color.red()))
            finally:
                connection.close()
    else:
        await ctx.send(embed=create_embed("Wrong Channel", "This command can only be used in a ticket channel.", discord.Color.red()))


@bot.command(name="close")
async def close_ticket_command(ctx: commands.Context, *, reason: str = "No reason provided."):
    """Closes an active ticket."""
    ticket_category = await get_channel_by_config(ctx.guild, TICKET_CATEGORY_ID, "Tickets")
    if ctx.channel.category and (ctx.channel.category.id == (ticket_category.id if ticket_category else None) or ctx.channel.category.name == "Tickets"):
        connection = create_db_connection()
        if connection:
            try:
                cursor = connection.cursor(dictionary=True)
                cursor.execute(
                    "SELECT ticket_id, requester_id, message_id FROM tickets WHERE channel_id = %s AND status != 'closed'",
                    (ctx.channel.id,)
                )
                ticket_data = cursor.fetchone()

                if not ticket_data:
                    await ctx.send(embed=create_embed("Close Failed", "This is not an active ticket channel.", discord.Color.orange()))
                    return
                
                ticket_id = ticket_data['ticket_id']
                requester_id = ticket_data['requester_id']
                message_id = ticket_data['message_id']

                is_staff = any(get_role_by_name(ctx.guild, role_name) in ctx.author.roles for role_name in [STAFF_ROLE_NAME, MODERATOR_ROLE_NAME, ADMIN_STAFF_ROLE_NAME, MANAGER_ROLE_NAME, PI_ROLE_NAME])
                if ctx.author.id != requester_id and not is_staff:
                    await ctx.send(embed=create_embed("Permission Denied", "You do not have permission to close this ticket.", discord.Color.red()))
                    return
                
                view = TicketView(bot, ticket_id, requester_id, ctx.channel.id)
                if message_id:
                    try:
                        view.message = await ctx.channel.fetch_message(message_id)
                    except discord.NotFound:
                        print(f"Original message for ticket {ticket_id} not found.")

                await view.close_ticket(ctx.channel, reason, ctx.author.id)
                await ctx.send(embed=create_embed("Ticket Closed", "This ticket has been closed.", discord.Color.green()))

            except Error as e:
                print(f"Error closing ticket via command: {e}")
                await ctx.send(embed=create_embed("Database Error", "An error occurred while closing the ticket.", discord.Color.red()))
            finally:
                connection.close()
    else:
        await ctx.send(embed=create_embed("Wrong Channel", "This command can only be used in a ticket channel.", discord.Color.red()))


# Run the bot
bot.run(DISCORD_BOT_TOKEN)
