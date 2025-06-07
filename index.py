import discord
from discord.ext import commands, tasks
import aiomysql
import asyncio
import os
from datetime import datetime, timedelta
import json
import random
import string
from PIL import Image, ImageDraw, ImageFont, ImageOps # For image generation
from io import BytesIO
import requests # For fetching IGN skins from NameMC

# --- Configuration (Use environment variables for production) ---
# IMPORTANT: Replace these with your actual IDs and tokens!
DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN', 'YOUR_DISCORD_BOT_TOKEN_HERE')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'your_db_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'your_db_password')
DB_NAME = os.getenv('DB_NAME', 'your_database_name')

# Discord Channel/Category IDs (replace '0' with your actual IDs)
REGISTER_CHANNEL_ID = int(os.getenv('REGISTER_CHANNEL_ID', '0')) # e.g., 123456789012345678
TICKETS_CHANNEL_ID = int(os.getenv('TICKETS_CHANNEL_ID', '0'))
TICKET_CATEGORY_ID = int(os.getenv('TICKET_CATEGORY_ID', '0'))
CLOSED_TICKETS_CATEGORY_ID = int(os.getenv('CLOSED_TICKETS_CATEGORY_ID', '0'))
STRIKE_REQUESTS_CHANNEL_ID = int(os.getenv('STRIKE_REQUESTS_CHANNEL_ID', '0'))
STRIKE_REQUESTS_CATEGORY_ID = int(os.getenv('STRIKE_REQUESTS_CATEGORY_ID', '0'))
STAFF_UPDATES_CHANNEL_ID = int(os.getenv('STAFF_UPDATES_CHANNEL_ID', '0'))
GAME_CATEGORY_ID = int(os.getenv('GAME_CATEGORY_ID', '0'))
VOICE_CATEGORY_ID = int(os.getenv('VOICE_CATEGORY_ID', '0'))
GAME_LOGS_CHANNEL_ID = int(os.getenv('GAME_LOGS_CHANNEL_ID', '0'))
BAN_LOGS_CHANNEL_ID = int(os.getenv('BAN_LOGS_CHANNEL_ID', '0'))
MUTE_LOGS_CHANNEL_ID = int(os.getenv('MUTE_LOGS_CHANNEL_ID', '0'))
STRIKE_LOGS_CHANNEL_ID = int(os.getenv('STRIKE_LOGS_CHANNEL_ID', '0'))
TICKET_LOGS_CHANNEL_ID = int(os.getenv('TICKET_LOGS_CHANNEL_ID', '0'))
SS_TICKET_LOGS_CHANNEL_ID = int(os.getenv('SS_TICKET_LOGS_CHANNEL_ID', '0'))
POLL_VOTING_CHANNEL_ID = int(os.getenv('POLL_VOTING_CHANNEL_ID', '0'))
AFK_VC_ID = int(os.getenv('AFK_VC_ID', '0')) # The voice channel to move AFK players to
GAMES_CHANNEL_ID = int(os.getenv('GAMES_CHANNEL_ID', '0')) # Channel to send game result images

# Discord Role IDs (replace '0' with your actual IDs)
REGISTERED_ROLE_ID = int(os.getenv('REGISTERED_ROLE_ID', '0'))
BANNED_ROLE_ID = int(os.getenv('BANNED_ROLE_ID', '0'))
MUTED_ROLE_ID = int(os.getenv('MUTED_ROLE_ID', '0'))
FROZEN_ROLE_ID = int(os.getenv('FROZEN_ROLE_ID', '0'))
STAFF_ROLE_ID = int(os.getenv('STAFF_ROLE_ID', '0')) # Base staff role
MOD_ROLE_ID = int(os.getenv('MOD_ROLE_ID', '0'))
ADMIN_ROLE_ID = int(os.getenv('ADMIN_ROLE_ID', '0'))
MANAGER_ROLE_ID = int(os.getenv('MANAGER_ROLE_ID', '0'))
PI_ROLE_ID = int(os.getenv('PI_ROLE_ID', '0')) # Highest admin role
PPP_MANAGER_ROLE_ID = int(os.getenv('PPP_MANAGER_ROLE_ID', '0')) # Role for managing PPP polls
SCREENSHARING_TEAM_ROLE_ID = int(os.getenv('SCREENSHARING_TEAM_ROLE_ID', '0'))

# Elo Role IDs (replace '0' with your actual IDs)
ELO_ROLES = {
    "Iron": int(os.getenv('ELO_IRON_ROLE_ID', '0')),
    "Bronze": int(os.getenv('ELO_BRONZE_ROLE_ID', '0')),
    "Silver": int(os.getenv('ELO_SILVER_ROLE_ID', '0')),
    "Gold": int(os.getenv('ELO_GOLD_ROLE_ID', '0')),
    "Topaz": int(os.getenv('ELO_TOPAZ_ROLE_ID', '0')),
    "Platinum": int(os.getenv('ELO_PLATINUM_ROLE_ID', '0'))
}

ELO_THRESHOLDS = {
    "Iron": (0, 150),
    "Bronze": (150, 400),
    "Silver": (400, 700),
    "Gold": (700, 900),
    "Topaz": (900, 1200),
    "Platinum": (1200, float('inf'))
}

ELO_MODELS = {
    "Iron": {"win": 25, "loss": 10, "mvp": 20},
    "Bronze": {"win": 20, "loss": 10, "mvp": 15},
    "Silver": {"win": 20, "loss": 10, "mvp": 10},
    "Gold": {"win": 15, "loss": 10, "mvp": 10},
    "Topaz": {"win": 10, "loss": 15, "mvp": 10},
    "Platinum": {"win": 5, "loss": 20, "mvp": 10}
}

# --- Bot Setup ---
intents = discord.Intents.all() # Enable all intents. In production, be specific with what your bot needs.
bot = commands.Bot(command_prefix='=', intents=intents, activity=discord.Activity(type=discord.ActivityType.listening, name="asrbw.fun"))

# --- Database Connection Pool ---
async def create_db_pool():
    """Establishes and returns an asynchronous database connection pool."""
    return await aiomysql.create_pool(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        autocommit=True # This means changes are automatically committed after each execute. Set to False for manual transaction management.
    )

@bot.event
async def on_ready():
    """Event that fires when the bot successfully connects to Discord."""
    print(f'Logged in as {bot.user.name} ({bot.user.id})')
    print('Bot is ready!')
    try:
        bot.db_pool = await create_db_pool()
        print('Database connection pool created.')
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        # Optionally, shut down the bot or attempt reconnection logic
        await bot.close()
        return

    # Start background tasks
    elo_decay_task.start()
    temp_mod_check_task.start()
    frozen_role_check_task.start()
    strike_request_poll_monitor.start()
    # queue_timeout_check_task.start() # This one needs more complex logic


@bot.event
async def on_disconnect():
    """Event that fires when the bot disconnects from Discord."""
    if hasattr(bot, 'db_pool') and bot.db_pool:
        bot.db_pool.close()
        await bot.db_pool.wait_closed()
        print('Database connection pool closed.')

# --- Global State Variables (In-memory, cleared on bot restart) ---
# For a production bot, consider persisting these to a database or Redis.
queues = {
    "3v3": [],          # List of discord.Member objects
    "4v4": [],
    "3v3 PUPS+": [],
    "4v4 PUPS+": []
}

active_games = {} # {game_channel_id: GameSessionObject} - Needs a custom class/dataclass to hold game state
# Example GameSessionObject:
# class GameSession:
#     def __init__(self, players, queue_type, game_channel, voice_channel, message_id, captains=None, team1=[], team2=[]):
#         self.players = players # All players in the game
#         self.queue_type = queue_type
#         self.game_channel = game_channel
#         self.voice_channel = voice_channel
#         self.message_id = message_id # Message with current pick/poll
#         self.captains = captains # (captain1_member, captain2_member)
#         self.team1 = team1 # List of discord.Member objects
#         self.team2 = team2 # List of discord.Member objects
#         self.current_picker = None
#         self.pick_stage = 1 # 1: Team1 picks 1, 2: Team2 picks 2, 3: Game starts
#         self.start_time = datetime.now()
#         self.party_season = False # Will be set based on admin config

temp_mod_actions = {} # {user_id: [{'action': 'ban'/'mute', 'expires': datetime, 'role_id': int, 'log_id': int}, ...]}
temp_frozen_roles = {} # {user_id: datetime_expires}
strike_request_polls = {} # {message_id: {'request_id': int, 'expiry_time': datetime, 'requester_id': int, 'target_user_id': int}}

# Admin controlled queue status and party season
QUEUE_OPEN_STATUS = {
    "3v3": True,
    "4v4": True,
    "3v3 PUPS+": True,
    "4v4 PUPS+": True
}
CURRENT_PARTY_SEASON = False # False for captain pick, True for Elo-based auto-teaming
PARTY_SIZE_LIMIT = None # None, 2, 3, 4

# Keep track of game numbers
last_game_number = 0 # Will fetch from DB on startup, or just start at 1 if DB is empty

# --- Utility Functions ---

def create_embed(title, description, color=discord.Color.blue()):
    """Creates a standardized Discord embed with a footer."""
    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text="asrbw.fun")
    return embed

async def get_user_data_from_db(user_id):
    """Fetches user data from the 'users' table."""
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM users WHERE discord_id = %s", (user_id,))
            return await cur.fetchone()

async def get_elo_role_name(elo):
    """Determines the Elo role name based on Elo points."""
    for role_name, (min_elo, max_elo) in ELO_THRESHOLDS.items():
        if min_elo <= elo < max_elo:
            return role_name
    return "Iron" # Default for 0 or negative elo if not explicitly handled

async def update_user_nickname(member: discord.Member, elo: int, ign: str):
    """Updates a user's Discord nickname to [ELO] IGN."""
    try:
        new_nickname = f"[{elo}] {ign}"
        if member.nick != new_nickname:
            await member.edit(nick=new_nickname)
            print(f"Updated nickname for {member.display_name} to {new_nickname}")
    except discord.Forbidden:
        print(f"Bot lacks permissions to change nickname for {member.display_name} ({member.id})")
    except Exception as e:
        print(f"Error updating nickname for {member.display_name} ({member.id}): {e}")

async def assign_elo_role(member: discord.Member, current_elo: int):
    """Assigns the correct Elo role and removes previous ones."""
    guild = member.guild
    # Remove existing Elo roles
    for role_name in ELO_ROLES:
        role_id = ELO_ROLES[role_name]
        elo_role = guild.get_role(role_id)
        if elo_role and elo_role in member.roles:
            try:
                await member.remove_roles(elo_role)
            except discord.Forbidden:
                print(f"Bot lacks permissions to remove role {elo_role.name} from {member.display_name}")

    # Assign new Elo role
    target_role_name = await get_elo_role_name(current_elo)
    target_role_id = ELO_ROLES.get(target_role_name)
    if target_role_id:
        target_role = guild.get_role(target_role_id)
        if target_role:
            try:
                await member.add_roles(target_role)
                print(f"Assigned role {target_role.name} to {member.display_name}")
            except discord.Forbidden:
                print(f"Bot lacks permissions to add role {target_role.name} to {member.display_name}")

async def log_action_to_channel(channel_id, embed_title, embed_description, color=discord.Color.blue()):
    """Sends an embed to a specified log channel."""
    log_channel = bot.get_channel(channel_id)
    if log_channel:
        embed = create_embed(embed_title, embed_description, color)
        await log_channel.send(embed=embed)
    else:
        print(f"Warning: Log channel with ID {channel_id} not found for logging: {embed_title}")

async def generate_html_log_and_send(channel: discord.TextChannel, filename_prefix: str, log_channel_id: int):
    """Generates an HTML log of messages in a channel and sends it to a log channel."""
    try:
        history = [msg async for msg in channel.history(limit=None, oldest_first=True)]
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Chat Log - {channel.name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; background-color: #36393F; color: #DCDDDE; }}
                .message {{ margin-bottom: 10px; }}
                .author {{ font-weight: bold; color: #7289DA; }}
                .timestamp {{ font-size: 0.8em; color: #7B848E; margin-left: 5px; }}
                .content {{ margin-left: 10px; }}
            </style>
        </head>
        <body>
            <h1>Chat Log for #{channel.name}</h1>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <hr>
        """
        for msg in history:
            html_content += f"""
            <div class="message">
                <span class="author">{msg.author.display_name}</span>
                <span class="timestamp">[{msg.created_at.strftime('%Y-%m-%d %H:%M:%S')}]</span>
                <div class="content">{msg.content.replace('<', '&lt;').replace('>', '&gt;')}</div>
            </div>
            """
        html_content += "</body></html>"

        filename = f"{filename_prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
        log_channel = bot.get_channel(log_channel_id)

        # Save to a BytesIO object instead of a file for direct Discord upload
        file_buffer = BytesIO(html_content.encode('utf-8'))
        file_buffer.seek(0)

        if log_channel:
            await log_channel.send(file=discord.File(file_buffer, filename=filename))
        else:
            print(f"Log channel not found: {log_channel_id}")
        return filename # Return filename for logging reference
    except Exception as e:
        print(f"Error generating HTML log for channel {channel.name}: {e}")
        return None

# --- Permissions Check Decorators ---
def has_roles(*role_ids):
    """Decorator to check if a user has any of the specified roles."""
    async def predicate(ctx):
        if not ctx.guild:
            return False
        return any(role.id in role_ids for role in ctx.author.roles)
    return commands.check(predicate)

# Specific role checks
def is_pi():
    return has_roles(PI_ROLE_ID)

def is_manager_or_above():
    return has_roles(MANAGER_ROLE_ID, PI_ROLE_ID)

def is_admin_or_above():
    return has_roles(ADMIN_ROLE_ID, MANAGER_ROLE_ID, PI_ROLE_ID)

def is_moderator_or_above():
    return has_roles(MOD_ROLE_ID, ADMIN_ROLE_ID, MANAGER_ROLE_ID, PI_ROLE_ID)

def is_staff():
    return has_roles(STAFF_ROLE_ID, MOD_ROLE_ID, ADMIN_ROLE_ID, MANAGER_ROLE_ID, PI_ROLE_ID)

def is_ppp_manager():
    return has_roles(PPP_MANAGER_ROLE_ID)

# --- Minecraft Server API Integration (PLACEHOLDER) ---
# You need to implement the actual communication between your Discord bot and your Minecraft plugin.
# This might involve:
# - A custom REST API on your Discord bot that the Minecraft plugin calls.
# - A custom REST API on your Minecraft server that the Discord bot calls.
# - A WebSocket connection for real-time bidirectional communication.
# - RCON for simple command execution.

MINECRAFT_API_BASE_URL = os.getenv('MINECRAFT_API_BASE_URL', 'http://localhost:8080/api') # Example

async def send_to_minecraft_server(endpoint, data):
    """Sends a POST request to the Minecraft server plugin's API."""
    url = f"{MINECRAFT_API_BASE_URL}/{endpoint}"
    try:
        response = await asyncio.to_thread(requests.post, url, json=data, timeout=5)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.Timeout:
        print(f"Minecraft API request to {url} timed out.")
        return {"status": "error", "message": "Minecraft server did not respond in time."}
    except requests.exceptions.RequestException as e:
        print(f"Error communicating with Minecraft API at {url}: {e}")
        return {"status": "error", "message": f"Failed to communicate with Minecraft server: {e}"}
    except json.JSONDecodeError:
        print(f"Minecraft API response from {url} was not valid JSON.")
        return {"status": "error", "message": "Invalid response from Minecraft server."}

# Placeholder for Minecraft-initiated callbacks (e.g., when a game ends)
# These would be handled by a web server framework (Flask, FastAPI) running alongside your bot.
# For example:
# @app.post("/game_ended") # Using Flask/FastAPI here as an example
# async def game_ended():
#     data = request.json
#     await process_game_result(data)
#     return {"status": "success"}

async def process_game_result(game_data):
    """
    Processes game results received from the Minecraft server plugin.
    Expected game_data structure:
    {
        "game_number": int,
        "queue_type": "3v3" | "4v4" | "3v3 PUPS+" | "4v4 PUPS+",
        "map_name": str,
        "team1_ign_players": ["IGN1", "IGN2", ...],
        "team2_ign_players": ["IGN3", "IGN4", ...],
        "winning_team_ign": ["IGN1", "IGN2", ...], # IGNs of winning team
        "mvp_ign": str, # IGN of MVP
        "total_kills": { "IGN1": 10, ... } # Optional, for future use
    }
    """
    print(f"Processing game result for Game #{game_data.get('game_number')}")
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            game_number = game_data['game_number']
            queue_type = game_data['queue_type']
            map_name = game_data.get('map_name', 'Unknown')
            team1_igns = game_data['team1_ign_players']
            team2_igns = game_data['team2_ign_players']
            winning_team_igns = game_data['winning_team_ign']
            mvp_ign = game_data['mvp_ign']

            # Determine winning team label ('team1' or 'team2')
            winning_team_label = None
            if all(ign in team1_igns for ign in winning_team_igns):
                winning_team_label = 'team1'
            elif all(ign in team2_igns for ign in winning_team_igns):
                winning_team_label = 'team2'

            if not winning_team_label:
                print(f"Error: Could not determine winning team label for game {game_number}. Data: {game_data}")
                return

            all_player_igns = team1_igns + team2_igns
            player_discord_ids = {} # {IGN: Discord_ID}

            # Fetch Discord IDs and current Elos for all players
            # Using IN clause with multiple %s for parameterized query
            if all_player_igns: # Avoid empty IN clause
                placeholders = ','.join(['%s'] * len(all_player_igns))
                await cur.execute(
                    f"SELECT discord_id, minecraft_ign, elo, wins, losses, mvps, current_streak FROM users WHERE minecraft_ign IN ({placeholders})",
                    tuple(all_player_igns)
                )
                fetched_players = await cur.fetchall()
                for p in fetched_players:
                    player_discord_ids[p['minecraft_ign']] = p['discord_id']

            updates = []
            mvp_discord_id = player_discord_ids.get(mvp_ign)
            
            for ign in all_player_igns:
                discord_id = player_discord_ids.get(ign)
                if not discord_id:
                    print(f"Warning: IGN {ign} not found in users table. Skipping Elo update for this player.")
                    continue

                user_data = next((p for p in fetched_players if p['discord_id'] == discord_id), None)
                if not user_data: continue # Should not happen if fetched_players is correct

                current_elo_role = await get_elo_role_name(user_data['elo'])
                elo_model = ELO_MODELS.get(current_elo_role, ELO_MODELS['Iron'])

                wins_change = 0
                losses_change = 0
                mvps_change = 0
                elo_change = 0
                streak_change = 0

                if ign in winning_team_igns: # Winning team
                    wins_change = 1
                    elo_change = elo_model['win']
                    streak_change = 1 # Increment streak for winners
                else: # Losing team
                    losses_change = 1
                    elo_change = -elo_model['loss']
                    streak_change = -user_data['current_streak'] - 1 # Reset and decrement streak for losers

                if ign == mvp_ign:
                    mvps_change = 1
                    elo_change += elo_model['mvp']
                
                updates.append({
                    "discord_id": discord_id,
                    "elo_change": elo_change,
                    "wins_change": wins_change,
                    "losses_change": losses_change,
                    "mvps_change": mvps_change,
                    "streak_change": streak_change,
                })

            # Apply updates
            for update in updates:
                discord_id = update['discord_id']
                elo_change = update['elo_change']
                wins_change = update['wins_change']
                losses_change = update['losses_change']
                mvps_change = update['mvps_change']
                streak_change = update['streak_change'] # Can be positive or negative

                await cur.execute(
                    """
                    UPDATE users
                    SET elo = elo + %s,
                        wins = wins + %s,
                        losses = losses + %s,
                        mvps = mvps + %s,
                        current_streak = CASE WHEN %s > 0 THEN current_streak + %s ELSE 0 END, -- If wins_change is positive, increment streak. Else, reset to 0.
                        last_game_date = %s
                    WHERE discord_id = %s
                    """,
                    (elo_change, wins_change, losses_change, mvps_change, wins_change, streak_change, datetime.now(), discord_id)
                )

                # Update Discord roles and nicknames for affected users
                guild = bot.get_guild(bot.guilds[0].id) # Assuming bot is in one guild
                member = guild.get_member(discord_id)
                if member:
                    updated_user_data = await get_user_data_from_db(discord_id)
                    if updated_user_data:
                        await update_user_nickname(member, updated_user_data['elo'], updated_user_data['minecraft_ign'])
                        await assign_elo_role(member, updated_user_data['elo'])

            await conn.commit()
            print(f"Elo and stats updated for Game #{game_number}")

            # Generate and send game result image
            await send_game_result_image(
                game_number, team1_igns, team2_igns,
                winning_team_igns, mvp_ign, map_name
            )

            # Log game details
            await cur.execute(
                """
                INSERT INTO game_history (game_id, queue_type, map_name, team1_players, team2_players, winning_team, mvp_user_id, scored_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (game_number, queue_type, map_name, json.dumps(team1_igns), json.dumps(team2_igns), winning_team_label, mvp_discord_id, 'bot')
            )
            await conn.commit()
            print(f"Game #{game_number} history logged.")

async def send_game_result_image(game_number, team1_igns, team2_igns, winning_team_igns, mvp_ign, map_name):
    """Generates and sends a monochrome image displaying game results."""
    # This is a complex image generation. Placeholder implementation.
    # You'll need to fetch Minecraft skins from NameMC:
    # https://crafatar.com/avatars/{UUID}?size=32 (for heads)
    # https://crafatar.com/renders/body/{UUID}?scale=10 (for full body)
    # You'd need to convert IGNs to UUIDs first (e.g., using Mojang API: https://api.mojang.com/users/profiles/minecraft/IGN)

    img_width, img_height = 800, 600
    img = Image.new('L', (img_width, img_height), color=255) # 'L' for monochrome (grayscale), 255 is white
    draw = ImageDraw.Draw(img)
    try:
        font_large = ImageFont.truetype("arial.ttf", 24)
        font_medium = ImageFont.truetype("arial.ttf", 18)
        font_small = ImageFont.truetype("arial.ttf", 14)
    except IOError:
        print("Could not load Arial font. Using default PIL font.")
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()

    # Convert to monochrome palette
    img = img.convert('1') # Convert to 1-bit pixel (black and white)
    draw = ImageDraw.Draw(img) # Re-initialize draw object after conversion

    # Header
    draw.text((20, 20), f"Game #{game_number} Results - {map_name}", font=font_large, fill=0) # 0 is black

    # Team 1
    draw.text((20, 80), "Team 1", font=font_medium, fill=0)
    y_offset = 110
    for player_ign in team1_igns:
        text = player_ign
        if player_ign == mvp_ign:
            text += " 👑 (MVP)"
        if player_ign in winning_team_igns:
            text += " (Winner)"
        draw.text((30, y_offset), text, font=font_small, fill=0)
        y_offset += 25

    # Team 2
    draw.text((img_width / 2 + 20, 80), "Team 2", font=font_medium, fill=0)
    y_offset = 110
    for player_ign in team2_igns:
        text = player_ign
        if player_ign == mvp_ign:
            text += " 👑 (MVP)"
        if player_ign in winning_team_igns:
            text += " (Winner)"
        draw.text((img_width / 2 + 30, y_offset), text, font=font_small, fill=0)
        y_offset += 25

    # Winning team indicator (text for monochrome)
    winning_team_text = "Winning Team: "
    if all(ign in team1_igns for ign in winning_team_igns):
        winning_team_text += "Team 1"
    else:
        winning_team_text += "Team 2"
    draw.text((20, img_height - 50), winning_team_text, font=font_medium, fill=0)

    # Save image to a BytesIO object
    img_byte_arr = BytesIO()
    img.save(img_byte_arr, format='PNG')
    img_byte_arr.seek(0)

    # Send image to the games channel
    games_channel = bot.get_channel(GAMES_CHANNEL_ID)
    if games_channel:
        try:
            await games_channel.send(file=discord.File(img_byte_arr, filename=f"game_{game_number}_results.png"))
        except discord.Forbidden:
            print(f"Bot lacks permissions to send files in channel {games_channel.name}")
    else:
        print(f"Games channel {GAMES_CHANNEL_ID} not found.")

async def get_minecraft_skin_head(ign):
    """Fetches a player's Minecraft skin head from NameMC and returns it as a PIL Image."""
    try:
        # Step 1: Get UUID from IGN
        uuid_url = f"https://api.mojang.com/users/profiles/minecraft/{ign}"
        uuid_response = requests.get(uuid_url, timeout=5)
        uuid_response.raise_for_status()
        uuid_data = uuid_response.json()
        uuid = uuid_data['id']

        # Step 2: Get skin head image
        skin_url = f"https://crafatar.com/avatars/{uuid}?size=64"
        skin_response = requests.get(skin_url, timeout=5)
        skin_response.raise_for_status()
        skin_image = Image.open(BytesIO(skin_response.content))
        return skin_image.convert('L') # Convert to grayscale for monochrome
    except requests.exceptions.RequestException as e:
        print(f"Error fetching skin for {ign}: {e}")
        return None
    except Exception as e:
        print(f"General error processing skin for {ign}: {e}")
        return None

async def generate_info_card_image(user_data, ctx_author_avatar_url):
    """Generates a monochrome image for user stats/info card."""
    img_width, img_height = 600, 250
    img = Image.new('L', (img_width, img_height), color=255) # White background
    draw = ImageDraw.Draw(img)

    try:
        font_title = ImageFont.truetype("arial.ttf", 30)
        font_stats = ImageFont.truetype("arial.ttf", 20)
        font_ign = ImageFont.truetype("arial.ttf", 25)
    except IOError:
        print("Could not load Arial font. Using default PIL font.")
        font_title = ImageFont.load_default()
        font_stats = ImageFont.load_default()
        font_ign = ImageFont.load_default()

    # Convert to monochrome palette
    img = img.convert('1')
    draw = ImageDraw.Draw(img)

    # Left side: IGN and Skin
    ign = user_data['minecraft_ign']
    skin_head = await get_minecraft_skin_head(ign)
    if skin_head:
        skin_head = skin_head.resize((128, 128), Image.Resampling.LANCZOS)
        img.paste(skin_head, (30, 60))

    draw.text((30, 20), f"IGN: {ign}", font=font_ign, fill=0)
    draw.text((30, 190), f"ELO: {user_data['elo']}", font=font_stats, fill=0)

    # Right side: Stats
    stats_x_start = img_width / 2
    draw.text((stats_x_start, 20), "Stats:", font=font_title, fill=0)

    wins = user_data['wins']
    losses = user_data['losses']
    mvps = user_data['mvps']
    streak = user_data['current_streak']
    total_games = wins + losses + user_data['ties']
    wlr = f"{wins / losses:.2f}" if losses > 0 else "N/A"

    stats_text = [
        f"Wins: {wins}",
        f"Losses: {losses}",
        f"WLR: {wlr}",
        f"MVPs: {mvps}",
        f"Streak: {streak}",
        f"Total Games: {total_games}"
    ]

    y_offset = 70
    for stat_line in stats_text:
        draw.text((stats_x_start, y_offset), stat_line, font=font_stats, fill=0)
        y_offset += 30

    # Save image to a BytesIO object
    img_byte_arr = BytesIO()
    img.save(img_byte_arr, format='PNG')
    img_byte_arr.seek(0)
    return img_byte_arr

# --- Background Tasks ---

@tasks.loop(hours=24) # Run daily
async def elo_decay_task():
    """Decays Elo for Topaz+ players who haven't played in 4 days."""
    print("Running ELO decay task...")
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            decay_threshold = datetime.now() - timedelta(days=4)
            # Find users who qualify for decay
            await cur.execute(
                """
                SELECT discord_id, minecraft_ign, elo FROM users
                WHERE elo >= %s AND last_game_date < %s
                """,
                (ELO_THRESHOLDS['Topaz'][0], decay_threshold)
            )
            decayed_users = await cur.fetchall()

            if decayed_users:
                print(f"Decaying ELO for {len(decayed_users)} users.")
                for user_id, ign, elo in decayed_users:
                    new_elo = max(0, elo - 60) # Ensure Elo doesn't go below 0
                    await cur.execute(
                        "UPDATE users SET elo = %s, last_game_date = %s WHERE discord_id = %s",
                        (new_elo, datetime.now(), user_id) # Update last_game_date to prevent immediate re-decay
                    )
                    guild = bot.get_guild(bot.guilds[0].id) # Assuming bot is in one guild
                    member = guild.get_member(user_id)
                    if member:
                        await update_user_nickname(member, new_elo, ign)
                        await assign_elo_role(member, new_elo)
                await conn.commit()
            else:
                print("No users found for ELO decay.")

@tasks.loop(minutes=1) # Check every minute
async def temp_mod_check_task():
    """Checks for expired temporary bans and mutes and removes roles."""
    current_time = datetime.now()
    guild = bot.get_guild(bot.guilds[0].id) # Assuming bot is in one guild

    if not guild:
        print("Guild not found in temp_mod_check_task. Skipping.")
        return

    # Iterate over a copy of the dictionary to allow modification during iteration
    for user_id, actions in list(temp_mod_actions.items()):
        for action_info in list(actions):
            if action_info['expires'] and action_info['expires'] <= current_time:
                member = guild.get_member(user_id)
                role = guild.get_role(action_info['role_id'])

                if member and role and role in member.roles:
                    try:
                        await member.remove_roles(role)
                        action_type = "unbanned" if action_info['action'] == 'ban' else "unmuted"
                        embed_title = f"User {action_type.capitalize()} (Automatic)"
                        embed_description = f"<@{user_id}> has been automatically {action_type} after their duration expired."
                        await log_action_to_channel(
                            BAN_LOGS_CHANNEL_ID if action_info['action'] == 'ban' else MUTE_LOGS_CHANNEL_ID,
                            embed_title, embed_description
                        )
                        print(f"Automatically {action_type} {member.display_name}")
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to remove role {role.name} from {member.display_name}")
                    except Exception as e:
                        print(f"Error auto-unmodding {member.display_name}: {e}")

                # Remove the completed action from the list
                actions.remove(action_info)
        
        # If no more actions for this user, remove user from temp_mod_actions
        if not actions:
            del temp_mod_actions[user_id]

@tasks.loop(minutes=1)
async def frozen_role_check_task():
    """Checks for expired 'Frozen' roles (after 10 minutes of no screensharer acceptance)."""
    current_time = datetime.now()
    guild = bot.get_guild(bot.guilds[0].id)

    if not guild:
        print("Guild not found in frozen_role_check_task. Skipping.")
        return

    for user_id, expiry_time in list(temp_frozen_roles.items()):
        if expiry_time <= current_time:
            member = guild.get_member(user_id)
            if member:
                frozen_role = guild.get_role(FROZEN_ROLE_ID)
                if frozen_role and frozen_role in member.roles:
                    try:
                        await member.remove_roles(frozen_role)
                        await member.send("Your 'Frozen' role has been automatically removed as no screensharer accepted your request within 10 minutes.")
                        print(f"Automatically removed Frozen role from {member.display_name}")
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to remove Frozen role from {member.display_name}")
            del temp_frozen_roles[user_id]

@tasks.loop(minutes=1) # Check every minute
async def strike_request_poll_monitor():
    """Monitors strike request polls and processes votes after 60 minutes."""
    current_time = datetime.now()
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Select open strike requests where the poll expiry time has passed
            await cur.execute(
                "SELECT * FROM strike_requests WHERE status = 'open' AND poll_expiry_time <= %s",
                (current_time,)
            )
            expired_requests = await cur.fetchall()

            for req in expired_requests:
                request_channel = bot.get_channel(req['channel_id'])
                if not request_channel:
                    print(f"Strike request channel {req['channel_id']} not found for request {req['request_id']}. Marking as closed.")
                    await cur.execute("UPDATE strike_requests SET status = 'closed', closed_at = %s WHERE request_id = %s", (current_time, req['request_id']))
                    continue

                poll_message = None
                try:
                    poll_message = await request_channel.fetch_message(req['poll_message_id'])
                except discord.NotFound:
                    print(f"Poll message {req['poll_message_id']} not found for strike request {req['request_id']}. Marking as closed.")
                    await cur.execute("UPDATE strike_requests SET status = 'closed', closed_at = %s WHERE request_id = %s", (current_time, req['request_id']))
                    await request_channel.delete() # Attempt to delete orphaned channel
                    continue
                except discord.Forbidden:
                    print(f"Bot lacks permissions to fetch poll message {req['poll_message_id']} in channel {request_channel.name}.")
                    await cur.execute("UPDATE strike_requests SET status = 'closed', closed_at = %s WHERE request_id = %s", (current_time, req['request_id']))
                    continue

                upvotes = 0
                downvotes = 0
                for reaction in poll_message.reactions:
                    if str(reaction.emoji) == '👍':
                        upvotes = reaction.count - 1 # Exclude bot's own reaction
                    elif str(reaction.emoji) == '👎':
                        downvotes = reaction.count - 1 # Exclude bot's own reaction

                deserves_strike = False
                # Voting criteria: upvotes should be like 3-1, 4-1, 5-2, and 6-1
                if (upvotes >= 3 and downvotes <= 1) or \
                   (upvotes >= 4 and downvotes <= 1) or \
                   (upvotes >= 5 and downvotes <= 2) or \
                   (upvotes >= 6 and downvotes <= 1):
                    deserves_strike = True

                target_user = bot.get_user(req['target_user_id']) # Get User object, not Member
                if not target_user:
                    print(f"Target user {req['target_user_id']} not found for strike request {req['request_id']}. Closing request.")
                    await cur.execute("UPDATE strike_requests SET status = 'closed', closed_at = %s WHERE request_id = %s", (current_time, req['request_id']))
                    try: await request_channel.delete()
                    except: pass
                    continue
                
                requester_member = bot.get_user(req['requester_id']) # Get User object
                reason = req['reason']

                if deserves_strike:
                    strike_id_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                    await cur.execute(
                        "INSERT INTO moderation_logs (user_id, moderator_id, action, reason, strike_id) VALUES (%s, %s, %s, %s, %s)",
                        (target_user.id, requester_member.id, 'strike', reason, strike_id_str)
                    )
                    await cur.execute(
                        "UPDATE users SET elo = elo - 40 WHERE discord_id = %s",
                        (target_user.id,)
                    )
                    guild = bot.get_guild(bot.guilds[0].id)
                    member = guild.get_member(target_user.id)
                    if member:
                        user_data = await get_user_data_from_db(target_user.id)
                        if user_data:
                            await update_user_nickname(member, user_data['elo'], user_data['minecraft_ign'])
                            await assign_elo_role(member, user_data['elo'])

                    embed = create_embed(
                        "Strike Request Result: Accepted",
                        f"The community has voted to **strike** {target_user.mention}.\n"
                        f"Reason: {reason}\nStrike ID: `{strike_id_str}`"
                    )
                    await request_channel.send(embed=embed)
                    await log_action_to_channel(STRIKE_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
                    await cur.execute("UPDATE strike_requests SET status = 'accepted', closed_at = %s WHERE request_id = %s", (current_time, req['request_id']))
                else:
                    embed = create_embed(
                        "Strike Request Result: Declined",
                        f"The community has voted **against** striking {target_user.mention}."
                    )
                    await request_channel.send(embed=embed)
                    await cur.execute("UPDATE strike_requests SET status = 'declined', closed_at = %s WHERE request_id = %s", (current_time, req['request_id']))

                # Close the channel
                try:
                    await request_channel.delete()
                except discord.Forbidden:
                    print(f"Bot lacks permissions to delete strike request channel {request_channel.name}")
                except Exception as e:
                    print(f"Error deleting strike request channel {request_channel.name}: {e}")

            await conn.commit()

@tasks.loop(minutes=5) # Adjust frequency as needed
async def queue_timeout_check_task():
    """
    Checks game channels for inactivity and moves players to AFK VC if all others leave.
    This requires tracking active game channels and their members.
    More complex as it needs to interact with Discord voice states and game channel states.
    """
    # This task would need to iterate through `active_games`
    # For each active game channel, check if all players (except the bot) have left the associated voice channel
    # If so, delete the game channel and move remaining players to AFK VC.
    # This requires detailed tracking of who is in which game channel/VC.
    print("Running queue timeout check task (placeholder).")
    pass

# --- Registration Commands ---

@bot.command(name='register')
async def register_command(ctx: commands.Context, ign: str):
    """Registers a user with their Minecraft IGN."""
    await ctx.message.add_reaction('✅') # Add reaction to show processing

    if ctx.channel.id != REGISTER_CHANNEL_ID:
        embed = create_embed("Incorrect Channel", "Please use the #register channel to register.")
        await ctx.send(embed=embed, delete_after=3)
        return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Check if user already registered
            await cur.execute("SELECT discord_id, is_registered FROM users WHERE discord_id = %s", (ctx.author.id,))
            user_data = await cur.fetchone()

            if user_data and user_data[1]: # Already registered
                embed = create_embed("Already Registered", "You are already registered!")
                await ctx.send(embed=embed)
                return
            
            # Check if IGN is already taken by another registered user
            await cur.execute("SELECT discord_id FROM users WHERE minecraft_ign = %s AND is_registered = TRUE", (ign,))
            ign_check = await cur.fetchone()
            if ign_check:
                embed = create_embed("IGN Taken", "That Minecraft IGN is already registered to another user.")
                await ctx.send(embed=embed)
                return

            registration_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

            if user_data: # User exists but not registered (e.g., failed previous registration)
                await cur.execute(
                    "UPDATE users SET minecraft_ign = %s, registration_code = %s, is_registered = FALSE WHERE discord_id = %s",
                    (ign, registration_code, ctx.author.id)
                )
            else: # New user
                await cur.execute(
                    "INSERT INTO users (discord_id, minecraft_ign, registration_code) VALUES (%s, %s, %s)",
                    (ctx.author.id, ign, registration_code)
                )
            await conn.commit()

            embed = create_embed(
                "Registration Pending",
                f"Your registration is almost complete! Your IGN is `{ign}`.\n"
                f"To finalize, go to the Minecraft server and type: `/link {registration_code}`\n\n"
                "You cannot move in the Minecraft server until registration is complete."
            )
            embed.add_field(name="Your Code", value=f"`{registration_code}`", inline=False)
            try:
                await ctx.author.send(embed=embed)
                await ctx.send(embed=create_embed("Check your DMs!", "I've sent you a DM with your registration code."), delete_after=5)
            except discord.Forbidden:
                await ctx.send(embed=create_embed("Error Sending DM", f"I couldn't send you a DM. Please enable DMs from server members. Your code is `{registration_code}`. Use `/link {registration_code}` in Minecraft."), delete_after=15)
            except Exception as e:
                print(f"Error sending registration DM: {e}")
                await ctx.send(embed=create_embed("Error", f"An unexpected error occurred while sending your registration code. Please try again or contact staff."), delete_after=10)

# This function would be called by your Minecraft server plugin via an API endpoint.
# E.g., if you run a Flask/FastAPI server with your bot, this would be an endpoint handler.
async def process_link_completion(ign: str, code: str):
    """
    Called when a user completes the /link command in Minecraft.
    Updates database, assigns roles, and updates nickname.
    """
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT discord_id FROM users WHERE minecraft_ign = %s AND registration_code = %s AND is_registered = FALSE",
                (ign, code)
            )
            result = await cur.fetchone()

            if result:
                discord_id = result[0]
                await cur.execute(
                    "UPDATE users SET is_registered = TRUE, registration_code = NULL, last_game_date = %s WHERE discord_id = %s",
                    (datetime.now(), discord_id)
                )
                await conn.commit()

                guild = bot.get_guild(bot.guilds[0].id) # Assuming bot is in one guild
                member = guild.get_member(discord_id)

                if member:
                    # Assign registered role
                    registered_role = guild.get_role(REGISTERED_ROLE_ID)
                    if registered_role:
                        try:
                            await member.add_roles(registered_role)
                        except discord.Forbidden:
                            print(f"Bot lacks permissions to add registered role to {member.display_name}")

                    # Assign initial Elo role (default 0 Elo is Iron)
                    await assign_elo_role(member, 0)

                    # Update nickname
                    await update_user_nickname(member, 0, ign)

                    embed = create_embed(
                        "Registration Complete!",
                        f"Congratulations {member.mention}! You are now fully registered as `{ign}`."
                    )
                    await member.send(embed=embed) # Send DM confirmation
                    return True
            return False

@bot.command(name='forceregister')
@is_moderator_or_above()
async def force_register(ctx: commands.Context, member: discord.Member, ign: str):
    """Force registers a user."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Check if IGN is already taken by a registered user
            await cur.execute("SELECT discord_id FROM users WHERE minecraft_ign = %s AND is_registered = TRUE", (ign,))
            ign_check = await cur.fetchone()
            if ign_check and ign_check[0] != member.id: # If taken by someone else
                embed = create_embed("IGN Taken", f"The Minecraft IGN `{ign}` is already registered to <@{ign_check[0]}>.")
                await ctx.send(embed=embed)
                return

            await cur.execute("SELECT discord_id FROM users WHERE discord_id = %s", (member.id,))
            user_data = await cur.fetchone()

            if user_data:
                await cur.execute(
                    "UPDATE users SET minecraft_ign = %s, is_registered = TRUE, registration_code = NULL, elo = 0, wins = 0, losses = 0, ties = 0, mvps = 0, current_streak = 0, last_game_date = %s WHERE discord_id = %s",
                    (ign, datetime.now(), member.id)
                )
            else:
                await cur.execute(
                    "INSERT INTO users (discord_id, minecraft_ign, elo, is_registered, last_game_date) VALUES (%s, %s, %s, %s, %s)",
                    (member.id, ign, 0, True, datetime.now())
                )
            await conn.commit()

            # Assign roles and update nickname
            guild = ctx.guild
            registered_role = guild.get_role(REGISTERED_ROLE_ID)
            if registered_role:
                try:
                    await member.add_roles(registered_role)
                except discord.Forbidden:
                    await ctx.send("Warning: Could not assign registered role (permissions issue).")
            await assign_elo_role(member, 0) # Assign Iron role
            await update_user_nickname(member, 0, ign)

            embed = create_embed(
                "Force Registered",
                f"{member.mention} has been force registered as `{ign}`."
            )
            await ctx.send(embed=embed)
            print(f"{ctx.author.display_name} force registered {member.display_name} as {ign}.")

# --- Queueing Commands ---

@bot.command(name='q')
async def join_queue(ctx: commands.Context, queue_type: str):
    """Joins a specific game queue."""
    await ctx.message.add_reaction('✅')

    queue_type = queue_type.lower()
    if queue_type not in queues:
        embed = create_embed(
            "Invalid Queue Type",
            f"Available queues: {', '.join(queues.keys())}"
        )
        await ctx.send(embed=embed, delete_after=5)
        return

    if not QUEUE_OPEN_STATUS.get(queue_type, False):
        embed = create_embed("Queue Closed", f"The `{queue_type}` queue is currently closed.")
        await ctx.send(embed=embed)
        return

    user_data = await get_user_data_from_db(ctx.author.id)
    if not user_data or not user_data['is_registered']:
        embed = create_embed("Not Registered", "You must be registered to join a queue. Use `=register <your_ign>`.")
        await ctx.send(embed=embed)
        return

    if ctx.author in queues[queue_type]:
        embed = create_embed("Already in Queue", f"You are already in the `{queue_type}` queue.")
        await ctx.send(embed=embed)
        return

    # Check if banned
    banned_role = ctx.guild.get_role(BANNED_ROLE_ID)
    if banned_role and banned_role in ctx.author.roles:
        embed = create_embed("Banned", "You cannot join queues while banned.")
        await ctx.send(embed=embed)
        return

    # Add to queue
    queues[queue_type].append(ctx.author)
    embed = create_embed("Queue Joined", f"You have joined the `{queue_type}` queue. Current players in queue: {len(queues[queue_type])}")
    await ctx.send(embed=embed)

    # Check for game start conditions
    await check_for_game_start(queue_type)

@bot.command(name='leave')
async def leave_queue(ctx: commands.Context):
    """Leaves any active queue the user is in."""
    await ctx.message.add_reaction('✅')

    found_queue = False
    for q_type, q_list in queues.items():
        if ctx.author in q_list:
            q_list.remove(ctx.author)
            embed = create_embed("Queue Left", f"You have left the `{q_type}` queue. Players remaining: {len(q_list)}")
            await ctx.send(embed=embed)
            found_queue = True
            break
    
    if not found_queue:
        embed = create_embed("Not in Queue", "You are not currently in any queue.")
        await ctx.send(embed=embed)

async def check_for_game_start(queue_type: str):
    """Checks if a queue has enough players to start a game and initiates it."""
    required_players = 6 if '3v3' in queue_type else 8 # 3v3 needs 6, 4v4 needs 8
    
    if len(queues[queue_type]) >= required_players:
        players_for_game = queues[queue_type][:required_players]
        del queues[queue_type][:required_players] # Remove players from queue

        # Fetch Elo for fair matchmaking
        player_elos = {}
        for player in players_for_game:
            user_data = await get_user_data_from_db(player.id)
            player_elos[player.id] = user_data['elo'] if user_data else 0

        game_players_details = []
        for player in players_for_game:
            game_players_details.append({
                "member": player,
                "ign": (await get_user_data_from_db(player.id))['minecraft_ign'],
                "elo": player_elos[player.id]
            })

        game_number = await get_next_game_number()
        game_channel_name = f"game-{game_number:04d}-{queue_type.replace(' ', '-')}" # Format: game-0001-3v3-pups+

        guild = bot.get_guild(bot.guilds[0].id) # Assuming bot is in one guild
        if not guild: return

        game_category = guild.get_channel(GAME_CATEGORY_ID)
        voice_category = guild.get_channel(VOICE_CATEGORY_ID)

        if not game_category or not voice_category:
            print("Game or Voice category not found. Cannot create channels.")
            # Refund players to queue
            queues[queue_type].extend(players_for_game)
            return

        # Create text channel
        game_text_channel = await game_category.create_text_channel(game_channel_name)
        # Create voice channel
        game_voice_channel = await voice_category.create_voice_channel(game_channel_name)

        embed = create_embed(
            f"Game #{game_number:04d} Starting!",
            f"A new `{queue_type}` game is starting!\n"
            f"Text Channel: {game_text_channel.mention}\n"
            f"Voice Channel: {game_voice_channel.mention}"
        )
        for player in players_for_game:
            embed.add_field(name="Player", value=player.mention, inline=True)
        await game_text_channel.send(embed=embed) # Send to the new game channel

        # Move players to voice channel
        for player in players_for_game:
            if player.voice and player.voice.channel: # If already in a voice channel
                try:
                    await player.move_to(game_voice_channel)
                except discord.Forbidden:
                    print(f"Bot lacks permissions to move {player.display_name} to voice channel.")
                except Exception as e:
                    print(f"Error moving {player.display_name} to voice channel: {e}")

        # Game Logic: Captains Pick or Elo-based Auto Teaming
        team1_players_info = []
        team2_players_info = []
        
        if CURRENT_PARTY_SEASON:
            # Elo-based auto teaming (fair matchmaking)
            # Sort players by Elo for better distribution
            sorted_players = sorted(game_players_details, key=lambda p: p['elo'], reverse=True)
            
            # Simple alternating pick for now. A truly fair algorithm is more complex.
            # Could use a min-max algorithm or genetic algorithm for optimal team balancing.
            team1_elo_sum = 0
            team2_elo_sum = 0

            # Alternate picking high/low Elo players to balance teams
            for i, player_info in enumerate(sorted_players):
                if i % 2 == 0:
                    team1_players_info.append(player_info)
                    team1_elo_sum += player_info['elo']
                else:
                    team2_players_info.append(player_info)
                    team2_elo_sum += player_info['elo']

            embed = create_embed(
                "Teams Formed! (Party Season)",
                f"Teams have been automatically formed based on Elo.\n"
                f"**Team 1 (Avg Elo: {team1_elo_sum / len(team1_players_info):.0f}):** " + ", ".join([p['member'].mention for p in team1_players_info]) + "\n"
                f"**Team 2 (Avg Elo: {team2_elo_sum / len(team2_players_info):.0f}):** " + ", ".join([p['member'].mention for p in team2_players_info])
            )
            await game_text_channel.send(embed=embed)
            await start_minecraft_game(game_number, queue_type, team1_players_info, team2_players_info, game_text_channel.id)

        else: # Not party season: Captains pick
            # Select 2 captains randomly
            captains = random.sample(players_for_game, 2)
            team1_captain = captains[0]
            team2_captain = captains[1]
            
            # Players available for picking
            available_players = [p for p in players_for_game if p not in captains]
            
            # Store game state for captain picks
            active_games[game_text_channel.id] = {
                "players_in_game": players_for_game, # All original players
                "queue_type": queue_type,
                "game_text_channel": game_text_channel,
                "game_voice_channel": game_voice_channel,
                "team1_captain": team1_captain,
                "team2_captain": team2_captain,
                "team1_players": [team1_captain],
                "team2_players": [team2_captain],
                "available_players": available_players,
                "current_picker": team1_captain,
                "pick_stage": 1, # Team 1 picks 1
                "game_number": game_number
            }

            # Create an initial prompt with available players and captains
            await send_captain_pick_prompt(game_text_channel.id)

async def send_captain_pick_prompt(game_channel_id):
    """Sends the captain pick prompt to the game channel."""
    game_session = active_games.get(game_channel_id)
    if not game_session: return

    game_text_channel = game_session['game_text_channel']
    available_players = game_session['available_players']
    team1_players = game_session['team1_players']
    team2_players = game_session['team2_players']
    current_picker = game_session['current_picker']

    player_options = []
    for i, player in enumerate(available_players):
        player_options.append(discord.SelectOption(label=player.display_name, value=str(player.id)))

    select_menu = discord.ui.Select(
        placeholder="Pick a player...",
        options=player_options,
        custom_id="player_pick_select"
    )

    class PlayerPickView(discord.ui.View):
        def __init__(self, game_channel_id):
            super().__init__(timeout=300) # 5 minutes to pick
            self.game_channel_id = game_channel_id
            self.add_item(select_menu)

        @discord.ui.select(custom_id="player_pick_select")
        async def select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
            session = active_games.get(self.game_channel_id)
            if not session:
                await interaction.response.send_message("This game session is no longer active.", ephemeral=True)
                return

            if interaction.user != session['current_picker']:
                await interaction.response.send_message("It's not your turn to pick!", ephemeral=True)
                return

            picked_player_id = int(select.values[0])
            picked_player = next((p for p in session['available_players'] if p.id == picked_player_id), None)

            if not picked_player:
                await interaction.response.send_message("That player is no longer available.", ephemeral=True)
                return
            
            # Update teams and available players
            session['available_players'].remove(picked_player)
            
            if session['pick_stage'] == 1: # Team 1 picks 1
                session['team1_players'].append(picked_player)
                session['pick_stage'] = 2
                session['current_picker'] = session['team2_captain']
                
            elif session['pick_stage'] == 2: # Team 2 picks 2 (first pick)
                if len(session['team2_players']) < (len(session['players_in_game']) / 2): # Check if Team 2 needs more players
                    session['team2_players'].append(picked_player)
                    # Stay on stage 2 for second pick by Team 2 if needed for 3v3
                    if len(session['team2_players']) < (len(session['players_in_game']) / 2):
                        pass # Team 2 picks another player
                    else:
                        session['pick_stage'] = 3 # All players picked
                        session['current_picker'] = None

            # Check if all players are picked
            if not session['available_players']:
                session['pick_stage'] = 3 # No more players to pick

            if session['pick_stage'] == 3: # All players picked, game starts
                await interaction.response.edit_message(content="All players picked. Game starting soon!", view=None)
                del active_games[self.game_channel_id] # Game session ends

                # Prepare players for Minecraft
                team1_player_members = session['team1_players']
                team2_player_members = session['team2_players']

                team1_igns = []
                team2_igns = []
                for member in team1_player_members:
                    user_data = await get_user_data_from_db(member.id)
                    if user_data: team1_igns.append(user_data['minecraft_ign'])
                for member in team2_player_members:
                    user_data = await get_user_data_from_db(member.id)
                    if user_data: team2_igns.append(user_data['minecraft_ign'])

                await start_minecraft_game(
                    session['game_number'],
                    session['queue_type'],
                    team1_player_members, # Pass members for moving to VC later if needed
                    team2_player_members,
                    game_text_channel.id
                )
                
            else: # Continue picking
                embed = create_embed(
                    "Player Picked!",
                    f"{interaction.user.mention} picked {picked_player.mention}."
                )
                embed.add_field(name="Team 1", value=", ".join([p.mention for p in session['team1_players']]), inline=False)
                embed.add_field(name="Team 2", value=", ".join([p.mention for p in session['team2_players']]), inline=False)
                embed.add_field(name="Next Picker", value=session['current_picker'].mention, inline=False)
                
                # Update select menu options for next pick
                new_player_options = []
                for p in session['available_players']:
                    new_player_options.append(discord.SelectOption(label=p.display_name, value=str(p.id)))
                select_menu.options = new_player_options
                select_menu.disabled = (len(new_player_options) == 0)

                await interaction.response.edit_message(embed=embed, view=PlayerPickView(self.game_channel_id))

    current_picks_embed = create_embed(
        "Captain's Pick Phase",
        f"It's {current_picker.mention}'s turn to pick!\n\n"
        "**Available Players:** " + ", ".join([p.mention for p in available_players]) + "\n\n"
        f"**Team 1:** {', '.join([p.mention for p in team1_players])}\n"
        f"**Team 2:** {', '.join([p.mention for p in team2_players])}\n"
    )
    
    # Store the message ID for interactions later if needed
    message = await game_text_channel.send(embed=current_picks_embed, view=PlayerPickView(game_channel_id))
    active_games[game_channel_id]['message_id'] = message.id


async def get_next_game_number():
    """Fetches the next available game number from the database."""
    global last_game_number
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT MAX(game_id) FROM game_history")
            result = await cur.fetchone()
            if result and result[0] is not None:
                last_game_number = result[0] + 1
            else:
                last_game_number = 1
            return last_game_number

async def start_minecraft_game(game_number, queue_type, team1_members, team2_members, game_channel_id):
    """
    Sends a command to the Minecraft server plugin to start a game.
    """
    # Convert Discord members to IGNs
    team1_igns = []
    team2_igns = []
    for member in team1_members:
        user_data = await get_user_data_from_db(member.id)
        if user_data and user_data['is_registered']:
            team1_igns.append(user_data['minecraft_ign'])
    for member in team2_members:
        user_data = await get_user_data_from_db(member.id)
        if user_data and user_data['is_registered']:
            team2_igns.append(user_data['minecraft_ign'])

    # Get a random map from a predefined list (replace with your actual map pool)
    map_pool = ["Forest", "SkyWars", "Desert", "Snowy_Peak", "Volcano"]
    selected_map = random.choice(map_pool)

    game_info = {
        "game_number": game_number,
        "queue_type": queue_type,
        "map_name": selected_map,
        "team1_igns": team1_igns,
        "team2_igns": team2_igns,
        "game_channel_id": game_channel_id # So Minecraft can report back to this channel
    }

    response = await send_to_minecraft_server("start_bedwars_game", game_info)

    game_text_channel = bot.get_channel(game_channel_id)
    if game_text_channel:
        if response.get("status") == "success":
            embed = create_embed(
                "Minecraft Game Initiated!",
                f"Game #{game_number:04d} is starting on map `{selected_map}`.\n"
                f"Players will be warped to the server and auto-teamed.\n"
                f"Team 1: {', '.join(team1_igns)}\n"
                f"Team 2: {', '.join(team2_igns)}"
            )
            await game_text_channel.send(embed=embed)
        else:
            embed = create_embed(
                "Minecraft Game Start Failed",
                f"Could not initiate game #{game_number:04d} on the Minecraft server.\n"
                f"Reason: {response.get('message', 'Unknown error')}\n"
                f"Please contact staff."
            )
            await game_text_channel.send(embed=embed)
            # Re-add players to queue or handle gracefully if game couldn't start
            for member in team1_members + team2_members:
                queues[queue_type].append(member) # Put them back in queue
            await game_text_channel.send("Players have been returned to queue due to game start failure.")


# --- Moderation Commands ---

def parse_duration(duration_str: str):
    """Parses a duration string (e.g., '1h', '30m', '1y') into a timedelta object."""
    if not duration_str:
        return None, "Duration not provided."
    
    unit = duration_str[-1].lower()
    try:
        value = int(duration_str[:-1])
    except ValueError:
        return None, "Invalid duration format. Use, e.g., '1s', '1m', '1h', '1d', '1y'."

    if unit == 's':
        return timedelta(seconds=value), None
    elif unit == 'm':
        return timedelta(minutes=value), None
    elif unit == 'h':
        return timedelta(hours=value), None
    elif unit == 'd':
        return timedelta(days=value), None
    elif unit == 'y':
        return timedelta(days=value * 365), None # Approximate year
    else:
        return None, "Invalid duration unit. Use 's', 'm', 'h', 'd', 'y'."

@bot.command(name='ban')
@is_admin_or_above()
async def ban_command(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str):
    """Bans a user by assigning a banned role."""
    await ctx.message.add_reaction('✅')

    timedelta_duration, error_msg = parse_duration(duration)
    if error_msg:
        await ctx.send(embed=create_embed("Error", error_msg))
        return

    banned_role = ctx.guild.get_role(BANNED_ROLE_ID)
    if not banned_role:
        await ctx.send(embed=create_embed("Error", "Banned role not found. Please configure `BANNED_ROLE_ID`."))
        return

    if banned_role in member.roles:
        await ctx.send(embed=create_embed("Already Banned", f"{member.mention} is already banned."))
        return

    try:
        await member.add_roles(banned_role)
        expires_at = datetime.now() + timedelta_duration

        # Store in database
        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO moderation_logs (user_id, moderator_id, action, reason, duration) VALUES (%s, %s, %s, %s, %s)",
                    (member.id, ctx.author.id, 'ban', reason, timedelta_duration.total_seconds())
                )
                log_id = cur.lastrowid # Get the ID of the newly inserted log
                await conn.commit()
        
        if member.id not in temp_mod_actions:
            temp_mod_actions[member.id] = []
        temp_mod_actions[member.id].append({
            'action': 'ban',
            'expires': expires_at,
            'role_id': BANNED_ROLE_ID,
            'log_id': log_id
        })

        embed = create_embed(
            "User Banned",
            f"{member.mention} has been **banned**."
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.add_field(name="Duration", value=duration, inline=False)
        embed.add_field(name="Expires", value=expires_at.strftime('%Y-%m-%d %H:%M:%S'), inline=False)
        await ctx.send(embed=embed)
        await log_action_to_channel(BAN_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
        print(f"{ctx.author.display_name} banned {member.display_name} for {duration}: {reason}")

    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to assign the banned role."))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred: {e}"))

@bot.command(name='unban')
@is_admin_or_above()
async def unban_command(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason provided."):
    """Unbans a user by removing the banned role."""
    await ctx.message.add_reaction('✅')

    banned_role = ctx.guild.get_role(BANNED_ROLE_ID)
    if not banned_role:
        await ctx.send(embed=create_embed("Error", "Banned role not found. Please configure `BANNED_ROLE_ID`."))
        return

    if banned_role not in member.roles:
        await ctx.send(embed=create_embed("Not Banned", f"{member.mention} is not currently banned."))
        return

    try:
        await member.remove_roles(banned_role)
        
        # Remove from temp_mod_actions if present
        if member.id in temp_mod_actions:
            temp_mod_actions[member.id] = [action for action in temp_mod_actions[member.id] if action['action'] != 'ban']
            if not temp_mod_actions[member.id]:
                del temp_mod_actions[member.id]

        # Log to database
        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO moderation_logs (user_id, moderator_id, action, reason) VALUES (%s, %s, %s, %s)",
                    (member.id, ctx.author.id, 'unban', reason)
                )
                await conn.commit()

        embed = create_embed(
            "User Unbanned",
            f"{member.mention} has been **unbanned**."
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        await ctx.send(embed=embed)
        await log_action_to_channel(BAN_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
        print(f"{ctx.author.display_name} unbanned {member.display_name}: {reason}")

    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to remove the banned role."))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred: {e}"))

@bot.command(name='mute')
@is_admin_or_above()
async def mute_command(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str):
    """Mutes a user by assigning a muted role."""
    await ctx.message.add_reaction('✅')

    timedelta_duration, error_msg = parse_duration(duration)
    if error_msg:
        await ctx.send(embed=create_embed("Error", error_msg))
        return

    muted_role = ctx.guild.get_role(MUTED_ROLE_ID)
    if not muted_role:
        await ctx.send(embed=create_embed("Error", "Muted role not found. Please configure `MUTED_ROLE_ID`."))
        return

    if muted_role in member.roles:
        await ctx.send(embed=create_embed("Already Muted", f"{member.mention} is already muted."))
        return

    try:
        await member.add_roles(muted_role)
        expires_at = datetime.now() + timedelta_duration

        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO moderation_logs (user_id, moderator_id, action, reason, duration) VALUES (%s, %s, %s, %s, %s)",
                    (member.id, ctx.author.id, 'mute', reason, timedelta_duration.total_seconds())
                )
                log_id = cur.lastrowid
                await conn.commit()

        if member.id not in temp_mod_actions:
            temp_mod_actions[member.id] = []
        temp_mod_actions[member.id].append({
            'action': 'mute',
            'expires': expires_at,
            'role_id': MUTED_ROLE_ID,
            'log_id': log_id
        })

        embed = create_embed(
            "User Muted",
            f"{member.mention} has been **muted**."
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.add_field(name="Duration", value=duration, inline=False)
        embed.add_field(name="Expires", value=expires_at.strftime('%Y-%m-%d %H:%M:%S'), inline=False)
        await ctx.send(embed=embed)
        await log_action_to_channel(MUTE_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
        print(f"{ctx.author.display_name} muted {member.display_name} for {duration}: {reason}")

    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to assign the muted role."))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred: {e}"))

@bot.command(name='unmute')
@is_admin_or_above()
async def unmute_command(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason provided."):
    """Unmutes a user by removing the muted role."""
    await ctx.message.add_reaction('✅')

    muted_role = ctx.guild.get_role(MUTED_ROLE_ID)
    if not muted_role:
        await ctx.send(embed=create_embed("Error", "Muted role not found. Please configure `MUTED_ROLE_ID`."))
        return

    if muted_role not in member.roles:
        await ctx.send(embed=create_embed("Not Muted", f"{member.mention} is not currently muted."))
        return

    try:
        await member.remove_roles(muted_role)

        if member.id in temp_mod_actions:
            temp_mod_actions[member.id] = [action for action in temp_mod_actions[member.id] if action['action'] != 'mute']
            if not temp_mod_actions[member.id]:
                del temp_mod_actions[member.id]

        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO moderation_logs (user_id, moderator_id, action, reason) VALUES (%s, %s, %s, %s)",
                    (member.id, ctx.author.id, 'unmute', reason)
                )
                await conn.commit()

        embed = create_embed(
            "User Unmuted",
            f"{member.mention} has been **unmuted**."
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        await ctx.send(embed=embed)
        await log_action_to_channel(MUTE_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
        print(f"{ctx.author.display_name} unmuted {member.display_name}: {reason}")

    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to remove the muted role."))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred: {e}"))

async def apply_strike(target_user: discord.User, moderator_id: int, reason: str):
    """Internal function to apply a strike."""
    strike_id_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO moderation_logs (user_id, moderator_id, action, reason, strike_id) VALUES (%s, %s, %s, %s, %s)",
                (target_user.id, moderator_id, 'strike', reason, strike_id_str)
            )
            await cur.execute(
                "UPDATE users SET elo = elo - 40 WHERE discord_id = %s",
                (target_user.id,)
            )
            await conn.commit()

            guild = bot.get_guild(bot.guilds[0].id)
            member = guild.get_member(target_user.id)
            if member:
                user_data = await get_user_data_from_db(target_user.id)
                if user_data:
                    await update_user_nickname(member, user_data['elo'], user_data['minecraft_ign'])
                    await assign_elo_role(member, user_data['elo'])

    embed = create_embed(
        "User Striked",
        f"{target_user.mention} has received a **strike**."
    )
    embed.add_field(name="Reason", value=reason, inline=False)
    embed.add_field(name="Strike ID", value=f"`{strike_id_str}`", inline=False)
    await log_action_to_channel(STRIKE_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
    print(f"User {target_user.id} striked by {moderator_id} for {reason}. Strike ID: {strike_id_str}")
    return strike_id_str

@bot.command(name='strike')
@is_admin_or_above() # Modified from Manager+ to Admin+ as per new instruction
async def strike_command(ctx: commands.Context, member: discord.Member, *, reason: str):
    """Issues a strike to a user, deducting 40 Elo."""
    await ctx.message.add_reaction('✅')

    strike_id_str = await apply_strike(member, ctx.author.id, reason)
    embed = create_embed(
        "User Striked",
        f"{member.mention} has received a **strike**."
    )
    embed.add_field(name="Reason", value=reason, inline=False)
    embed.add_field(name="Strike ID", value=f"`{strike_id_str}`", inline=False)
    await ctx.send(embed=embed)
    # Log to channel is handled by apply_strike

@bot.command(name='strikeremove', aliases=['srem'])
@is_admin_or_above() # Modified from Manager+ to Admin+ as per new instruction
async def strike_remove_command(ctx: commands.Context, strike_id: str, *, reason: str = "No reason provided."):
    """Removes a strike from a user."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT user_id, reason FROM moderation_logs WHERE strike_id = %s AND action = 'strike'", (strike_id,))
            strike_data = await cur.fetchone()

            if not strike_data:
                embed = create_embed("Strike Not Found", f"No strike found with ID `{strike_id}`.")
                await ctx.send(embed=embed)
                return
            
            user_id = strike_data['user_id']
            original_reason = strike_data['reason']

            # Restore Elo (assume 40 Elo was deducted, if logic changes, this needs to change)
            await cur.execute(
                "UPDATE users SET elo = elo + 40 WHERE discord_id = %s",
                (user_id,)
            )
            # Mark strike as unstriking in logs (or delete, but logging is better)
            await cur.execute(
                "UPDATE moderation_logs SET action = 'unstriked', reason = CONCAT('UNSTRIKED: ', %s, ' (Original: ', reason, ')') WHERE strike_id = %s",
                (reason, strike_id)
            )
            await conn.commit()

            member = ctx.guild.get_member(user_id)
            if member:
                user_data = await get_user_data_from_db(user_id)
                if user_data:
                    await update_user_nickname(member, user_data['elo'], user_data['minecraft_ign'])
                    await assign_elo_role(member, user_data['elo'])

            embed = create_embed(
                "Strike Removed",
                f"Strike ID `{strike_id}` has been **removed**."
            )
            embed.add_field(name="User", value=f"<@{user_id}>", inline=False)
            embed.add_field(name="Reason for Removal", value=reason, inline=False)
            embed.add_field(name="Original Reason", value=original_reason, inline=False)
            await ctx.send(embed=embed)
            await log_action_to_channel(STRIKE_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
            print(f"{ctx.author.display_name} removed strike {strike_id} from {user_id}: {reason}")

    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to remove the banned role."))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred: {e}"))

@bot.command(name='strikerequest', aliases=['sr'])
async def strike_request_command(ctx: commands.Context, member: discord.Member, reason: str, proof: str):
    """Allows players to request a strike on another player with proof."""
    await ctx.message.add_reaction('✅')

    if ctx.channel.id != STRIKE_REQUESTS_CHANNEL_ID:
        embed = create_embed("Incorrect Channel", f"Please use the <#{STRIKE_REQUESTS_CHANNEL_ID}> channel for strike requests.")
        await ctx.send(embed=embed, delete_after=5)
        return

    # Basic validation for proof (expecting a direct image URL)
    if not (proof.startswith('http://') or proof.startswith('https://') and (proof.endswith('.png') or proof.endswith('.jpg') or proof.endswith('.jpeg') or proof.endswith('.gif'))):
        embed = create_embed("Invalid Proof", "Proof must be a direct image URL (png, jpg, jpeg, gif).")
        await ctx.send(embed=embed)
        return

    guild = ctx.guild
    category = guild.get_channel(STRIKE_REQUESTS_CATEGORY_ID)
    if not category:
        await ctx.send(embed=create_embed("Error", "Strike Requests category not found. Please configure `STRIKE_REQUESTS_CATEGORY_ID`."))
        return

    # Create a new channel for the request
    channel_name = f"strike-request-{member.name.lower().replace(' ', '-')}-{random.randint(100,999)}"
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        member: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        # Staff roles should also be able to read and send messages
        guild.get_role(STAFF_ROLE_ID): discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    request_channel = await category.create_text_channel(channel_name, overwrites=overwrites)

    # Store request in DB
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO strike_requests (requester_id, target_user_id, reason, proof_url, channel_id, poll_expiry_time) VALUES (%s, %s, %s, %s, %s, %s)",
                (ctx.author.id, member.id, reason, proof, request_channel.id, datetime.now() + timedelta(minutes=60))
            )
            request_id = cur.lastrowid
            await conn.commit()

    embed = create_embed(
        f"Strike Request for {member.display_name} (ID: {request_id})",
        f"Requested by: {ctx.author.mention}\n"
        f"Target: {member.mention}\n\n"
        f"**Reason:** {reason}"
    )
    embed.set_image(url=proof)
    embed.add_field(name="Vote (60 minutes to vote):", value="👍 = Deserves Strike\n👎 = Does Not Deserve Strike", inline=False)
    
    poll_message = await request_channel.send(embed=embed)
    await poll_message.add_reaction('👍')
    await poll_message.add_reaction('👎')

    async with bot.db_pool.acquire() as conn: # Update message ID after sending
        async with conn.cursor() as cur:
            await cur.execute("UPDATE strike_requests SET poll_message_id = %s WHERE request_id = %s", (poll_message.id, request_id))
            await conn.commit()

    embed_log = create_embed(
        "New Strike Request",
        f"Requested by {ctx.author.mention} on {member.mention}.\n"
        f"Channel: {request_channel.mention}\nReason: {reason}\nProof: {proof}"
    )
    await log_action_to_channel(STRIKE_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color) # Send as embed, not HTML for this log

    await ctx.send(embed=create_embed(
        "Strike Request Created",
        f"Your strike request for {member.mention} has been created in {request_channel.mention}. "
        "A poll will begin shortly."
    ))

@bot.command(name='ss')
async def screenshare_command(ctx: commands.Context, member: discord.Member, reason: str, proof: str):
    """Initiates a screenshare request, assigning a 'Frozen' role."""
    await ctx.message.add_reaction('✅')

    # Basic validation for proof
    if not (proof.startswith('http://') or proof.startswith('https://') and (proof.endswith('.png') or proof.endswith('.jpg') or proof.endswith('.jpeg') or proof.endswith('.gif'))):
        embed = create_embed("Invalid Proof", "Proof must be a direct image URL (png, jpg, jpeg, gif).")
        await ctx.send(embed=embed)
        return

    guild = ctx.guild
    frozen_role = guild.get_role(FROZEN_ROLE_ID)
    if not frozen_role:
        await ctx.send(embed=create_embed("Error", "Frozen role not found. Please configure `FROZEN_ROLE_ID`."))
        return

    if frozen_role in member.roles:
        await ctx.send(embed=create_embed("Already Frozen", f"{member.mention} is already frozen."))
        return

    try:
        await member.add_roles(frozen_role)
        temp_frozen_roles[member.id] = datetime.now() + timedelta(minutes=10) # 10 minute timeout

        # Create a private ticket channel for screenshare team and involved users
        category = guild.get_channel(TICKET_CATEGORY_ID) # Use the general ticket category for SS tickets
        if not category:
            await ctx.send(embed=create_embed("Error", "Ticket category not found. Cannot create screenshare ticket."))
            return

        channel_name = f"ss-ticket-{member.name.lower().replace(' ', '-')}-{random.randint(1000,9999)}"
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            member: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            guild.get_role(SCREENSHARING_TEAM_ROLE_ID): discord.PermissionOverwrite(read_messages=True, send_messages=True)
        }
        ss_ticket_channel = await category.create_text_channel(channel_name, overwrites=overwrites)

        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO ss_tickets (requester_id, target_user_id, reason, proof_url, channel_id) VALUES (%s, %s, %s, %s, %s)",
                    (ctx.author.id, member.id, reason, proof, ss_ticket_channel.id)
                )
                ss_ticket_id = cur.lastrowid
                await conn.commit()

        embed = create_embed(
            f"Screenshare Request for {member.display_name} (ID: {ss_ticket_id})",
            f"Requested by: {ctx.author.mention}\n"
            f"Target: {member.mention}\n\n"
            f"**Reason:** {reason}"
        )
        embed.set_image(url=proof)

        # Buttons for Screensharing staff
        accept_button = discord.ui.Button(label="Accept Screenshare", style=discord.ButtonStyle.success, custom_id="ss_accept")
        decline_button = discord.ui.Button(label="Decline Screenshare", style=discord.ButtonStyle.danger, custom_id="ss_decline")

        view = discord.ui.View(timeout=None) # No timeout for the view, managed by frozen_role_check_task
        view.add_item(accept_button)
        view.add_item(decline_button)

        async def accept_callback(interaction: discord.Interaction):
            # This check needs to be an instance check for the interaction, not a global check
            # For simplicity, we are passing ctx.author and ctx.guild roles via function, or use interaction.user.roles directly.
            # `is_staff()` is a `commands.check` and cannot directly check interaction.user.roles outside a command context.
            # Best to replicate the logic or pass the roles correctly.
            member_roles = [role.id for role in interaction.user.roles]
            if not any(role_id in member_roles for role_id in [SCREENSHARING_TEAM_ROLE_ID]): 
                await interaction.response.send_message("You are not authorized to accept screenshares.", ephemeral=True)
                return
            
            # Check if already accepted/declined
            async with bot.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT status FROM ss_tickets WHERE ss_ticket_id = %s", (ss_ticket_id,))
                    status_row = await cur.fetchone()
                    if status_row['status'] != 'open':
                        await interaction.response.send_message("This screenshare request has already been processed.", ephemeral=True)
                        return

                    await cur.execute("UPDATE ss_tickets SET status = 'accepted', accepted_by_id = %s WHERE ss_ticket_id = %s", (interaction.user.id, ss_ticket_id))
                    await conn.commit()

            # Remove from temp_frozen_roles so it's not automatically removed
            if member.id in temp_frozen_roles:
                del temp_frozen_roles[member.id]

            await interaction.response.edit_message(content=f"{interaction.user.mention} has **accepted** the screenshare.", view=None, embed=None)
            embed_log = create_embed(
                "Screenshare Accepted",
                f"{member.mention}'s screenshare request ({ss_ticket_id}) has been accepted by {interaction.user.mention}."
            )
            await log_action_to_channel(SS_TICKET_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color)
            await ss_ticket_channel.send(embed=embed_log)

        async def decline_callback(interaction: discord.Interaction):
            member_roles = [role.id for role in interaction.user.roles]
            if not any(role_id in member_roles for role_id in [SCREENSHARING_TEAM_ROLE_ID]):
                await interaction.response.send_message("You are not authorized to decline screenshares.", ephemeral=True)
                return

            async with bot.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT status FROM ss_tickets WHERE ss_ticket_id = %s", (ss_ticket_id,))
                    status_row = await cur.fetchone()
                    if status_row['status'] != 'open':
                        await interaction.response.send_message("This screenshare request has already been processed.", ephemeral=True)
                        return

                    await cur.execute("UPDATE ss_tickets SET status = 'declined', accepted_by_id = %s, closed_at = %s, close_reason = 'Declined by screensharer' WHERE ss_ticket_id = %s", (interaction.user.id, datetime.now(), ss_ticket_id))
                    await conn.commit()

            # Remove frozen role
            if frozen_role in member.roles:
                try:
                    await member.remove_roles(frozen_role)
                except discord.Forbidden:
                    print(f"Bot lacks permissions to remove Frozen role from {member.display_name}")
            if member.id in temp_frozen_roles:
                del temp_frozen_roles[member.id] # Remove from auto-cleanup

            await interaction.response.edit_message(content=f"{interaction.user.mention} has **declined** the screenshare. Ticket closing.", view=None, embed=None)
            embed_log = create_embed(
                "Screenshare Declined",
                f"{member.mention}'s screenshare request ({ss_ticket_id}) has been declined by {interaction.user.mention}."
            )
            await log_action_to_channel(SS_TICKET_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color)
            await ss_ticket_channel.send(embed=embed_log)
            # Move to closed tickets category
            await ss_ticket_channel.edit(category=guild.get_channel(CLOSED_TICKETS_CATEGORY_ID))

        accept_button.callback = accept_callback
        decline_button.callback = decline_callback

        await ss_ticket_channel.send(embed=embed, view=view)
        await ctx.send(embed=create_embed(
            "Screenshare Request Sent",
            f"{member.mention} has been assigned the 'Frozen' role and a screenshare ticket has been opened in {ss_ticket_channel.mention}. "
            f"Screenshare staff have 10 minutes to accept."
        ))
        embed_log = create_embed(
            "New Screenshare Request",
            f"Requested by {ctx.author.mention} on {member.mention}.\n"
            f"Channel: {ss_ticket_channel.mention}\nReason: {reason}\nProof: {proof}"
        )
        await log_action_to_channel(SS_TICKET_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color)

    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to assign the frozen role or create channels."))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred: {e}"))

@bot.command(name='ssclose')
@is_staff() # Only staff can close SS tickets
async def ss_close_command(ctx: commands.Context, *, reason: str = "No reason provided."):
    """Closes an active screenshare ticket."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM ss_tickets WHERE channel_id = %s AND status = 'accepted'", (ctx.channel.id,))
            ss_ticket = await cur.fetchone()

            if not ss_ticket:
                await ctx.send(embed=create_embed("Not an Active Screenshare Ticket", "This channel is not an active, accepted screenshare ticket."))
                return

            target_member = ctx.guild.get_member(ss_ticket['target_user_id'])
            if target_member:
                frozen_role = ctx.guild.get_role(FROZEN_ROLE_ID)
                if frozen_role and frozen_role in target_member.roles:
                    try:
                        await target_member.remove_roles(frozen_role)
                    except discord.Forbidden:
                        print(f"Bot lacks permissions to remove Frozen role from {target_member.display_name}")
                if target_member.id in temp_frozen_roles:
                    del temp_frozen_roles[target_member.id]

            await cur.execute(
                "UPDATE ss_tickets SET status = 'closed', closed_at = %s, close_reason = %s WHERE ss_ticket_id = %s",
                (datetime.now(), reason, ss_ticket['ss_ticket_id'])
            )
            await conn.commit()

            embed = create_embed(
                "Screenshare Ticket Closed",
                f"This screenshare ticket has been closed by {ctx.author.mention}.\n"
                f"Reason: {reason}"
            )
            await ctx.send(embed=embed)
            await log_action_to_channel(SS_TICKET_LOGS_CHANNEL_ID, embed.title, embed.description, embed.color)
            
            # Move to closed tickets category
            try:
                await ctx.channel.edit(category=ctx.guild.get_channel(CLOSED_TICKETS_CATEGORY_ID))
            except discord.Forbidden:
                print(f"Bot lacks permissions to move channel {ctx.channel.name} to closed category.")
            except Exception as e:
                print(f"Error moving channel {ctx.channel.name}: {e}")

# --- Poll Commands (PPP Manager Only) ---

@bot.group(name='poll', invoke_without_command=True) # Changed from @bot.command to @bot.group
@is_ppp_manager()
async def create_poll_command(ctx: commands.Context):
    """Starts a poll in #ppp-voting or displays poll commands."""
    await ctx.message.add_reaction('✅')
    if ctx.invoked_subcommand is None:
        embed = create_embed(
            "Poll Commands",
            "Use `=poll create <kind> [user_id]` to create a new poll.\n"
            "Use `=poll close <kind> [user_id]` to close a poll.\n"
            "Use `=mypoll <kind>` to see your poll status."
        )
        await ctx.send(embed=embed)

@create_poll_command.command(name='create') # Now a subcommand of 'poll'
@is_ppp_manager()
async def poll_create_subcommand(ctx: commands.Context, kind: str, member: discord.Member = None):
    """Starts a poll in #ppp-voting."""
    # Logic from the original create_poll_command goes here
    await ctx.message.add_reaction('✅')

    poll_channel = bot.get_channel(POLL_VOTING_CHANNEL_ID)
    if not poll_channel:
        await ctx.send(embed=create_embed("Error", "Poll voting channel not found. Configure `POLL_VOTING_CHANNEL_ID`."))
        return
    
    if poll_channel.id != ctx.channel.id:
        await ctx.send(embed=create_embed("Incorrect Channel", f"Please use {poll_channel.mention} to create polls."), delete_after=5)
        return

    target_user_info = f" for {member.mention}" if member else ""
    poll_title = f"New Poll: {kind.capitalize()}{target_user_info}"
    poll_description = f"Vote on this poll! Use the reactions below."
    
    embed = create_embed(poll_title, poll_description)
    embed.add_field(name="Status", value="Open", inline=True)
    embed.add_field(name="Kind", value=kind, inline=True)
    if member:
        embed.add_field(name="Target User", value=member.mention, inline=True)

    message = await poll_channel.send(embed=embed)
    await message.add_reaction('👍')
    await message.add_reaction('👎')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO polls (kind, target_user_id, message_id, channel_id, status) VALUES (%s, %s, %s, %s, %s)",
                (kind, member.id if member else None, message.id, poll_channel.id, 'open')
            )
            await conn.commit()
    
    await ctx.send(embed=create_embed("Poll Created", f"Poll '{kind}' created in {poll_channel.mention}."), delete_after=5)

@create_poll_command.command(name='close') # Now a subcommand of 'poll'
@is_ppp_manager()
async def poll_close_subcommand(ctx: commands.Context, kind: str, member: discord.Member = None):
    """Closes an active poll."""
    # Logic from the original close_poll_command goes here
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            target_user_id = member.id if member else None
            await cur.execute(
                "SELECT poll_id, message_id, channel_id FROM polls WHERE kind = %s AND target_user_id <=> %s AND status = 'open'",
                (kind, target_user_id)
            )
            poll_data = await cur.fetchone()

            if not poll_data:
                await ctx.send(embed=create_embed("Poll Not Found", "Could not find an open poll with that kind and target user."))
                return
            
            poll_id = poll_data['poll_id']
            message_id = poll_data['message_id']
            channel_id = poll_data['channel_id']
            
            poll_channel = bot.get_channel(channel_id)
            if not poll_channel:
                await ctx.send(embed=create_embed("Error", "Poll channel not found."))
                return

            try:
                poll_message = await poll_channel.fetch_message(message_id)
                # Update embed
                updated_embed = poll_message.embeds[0]
                updated_embed.set_field_at(0, name="Status", value="Closed", inline=True)
                await poll_message.edit(embed=updated_embed, view=None) # Remove view/buttons if any
            except discord.NotFound:
                await ctx.send(embed=create_embed("Error", "Poll message not found. Database updated, but message could not be edited."))
            except discord.Forbidden:
                await ctx.send(embed=create_embed("Permissions Error", "I lack permissions to edit the poll message."))

            await cur.execute(
                "UPDATE polls SET status = 'closed', closed_at = %s WHERE poll_id = %s",
                (datetime.now(), poll_id)
            )
            await conn.commit()
            await ctx.send(embed=create_embed("Poll Closed", f"Poll '{kind}' for {member.mention if member else 'general'} has been closed."))

@bot.command(name='mypoll')
async def my_poll_status_command(ctx: commands.Context, kind: str):
    """Shows the status of a specific poll."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT * FROM polls WHERE kind = %s AND target_user_id = %s ORDER BY created_at DESC LIMIT 1",
                (kind, ctx.author.id)
            )
            poll_data = await cur.fetchone()

            if not poll_data:
                await ctx.send(embed=create_embed("Poll Not Found", f"You don't have a poll of kind '{kind}'."))
                return
            
            embed = create_embed(
                f"Your Poll Status: {poll_data['kind'].capitalize()}",
                f"Status: **{poll_data['status'].capitalize()}**\n"
                f"Created: {poll_data['created_at'].strftime('%Y-%m-%d %H:%M:%S')}"
            )
            if poll_data['closed_at']:
                embed.add_field(name="Closed", value=poll_data['closed_at'].strftime('%Y-%m-%d %H:%M:%S'), inline=False)
            embed.add_field(name="Message Link", value=f"[Go to Poll](https://discord.com/channels/{ctx.guild.id}/{poll_data['channel_id']}/{poll_data['message_id']})", inline=False)
            await ctx.send(embed=embed)

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """Tracks votes on polls."""
    if payload.user_id == bot.user.id: return # Ignore bot's own reactions

    channel = bot.get_channel(payload.channel_id)
    if not channel or channel.id != POLL_VOTING_CHANNEL_ID: return # Only care about voting channel

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT poll_id, status FROM polls WHERE message_id = %s", (payload.message_id,))
            poll_data = await cur.fetchone()

            if not poll_data or poll_data['status'] == 'closed': return # Not a poll or poll is closed

            vote_type = None
            if str(payload.emoji) == '👍':
                vote_type = 'upvote'
            elif str(payload.emoji) == '👎':
                vote_type = 'downvote'
            
            if not vote_type: return # Not a relevant reaction

            # Check if user already voted on this poll
            await cur.execute(
                "SELECT vote_id FROM poll_votes WHERE poll_id = %s AND user_id = %s",
                (poll_data['poll_id'], payload.user_id)
            )
            existing_vote = await cur.fetchone()

            if existing_vote:
                # If already voted, remove their previous reaction
                message = await channel.fetch_message(payload.message_id)
                user = bot.get_user(payload.user_id)
                if user and message:
                    for reaction in message.reactions:
                        if reaction.emoji in ['👍', '👎']:
                            try:
                                await message.remove_reaction(reaction.emoji, user)
                            except discord.HTTPException:
                                pass # Ignore if reaction already removed
                await cur.execute("DELETE FROM poll_votes WHERE poll_id = %s AND user_id = %s", (poll_data['poll_id'], payload.user_id))
                await cur.execute(
                    "INSERT INTO poll_votes (poll_id, user_id, vote_type) VALUES (%s, %s, %s)",
                    (poll_data['poll_id'], payload.user_id, vote_type)
                )
            else:
                await cur.execute(
                    "INSERT INTO poll_votes (poll_id, user_id, vote_type) VALUES (%s, %s, %s)",
                    (poll_data['poll_id'], payload.user_id, vote_type)
                )
            await conn.commit()

@bot.command(name='myvote')
async def my_vote_command(ctx: commands.Context, kind: str):
    """Shows how the user voted on a specific poll."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT p.poll_id, p.target_user_id, p.status, pv.vote_type FROM polls p LEFT JOIN poll_votes pv ON p.poll_id = pv.poll_id "
                "WHERE p.kind = %s AND p.target_user_id = %s ORDER BY p.created_at DESC LIMIT 1",
                (kind, ctx.author.id)
            )
            result = await cur.fetchone()

            if not result:
                await ctx.send(embed=create_embed("Vote Not Found", f"You haven't voted on a poll of kind '{kind}' that targeted you."))
                return

            poll_status = result['status']
            vote_type = result['vote_type']

            embed = create_embed(
                f"Your Vote for Poll: {kind.capitalize()}",
                f"Status: **{poll_status.capitalize()}**\n"
                f"Your Vote: **{vote_type.capitalize() if vote_type else 'Not Voted'}**"
            )
            await ctx.send(embed=embed)

# This command should only be seen by the person whose poll it is, and PPP manager.
# To achieve this, it's best to handle it in a private channel or DM, or filter reactions
# directly from the database query when a specific user is requesting their own poll's votes.
# For simplicity, this example will show it to the user.
@bot.event
async def on_reaction_add(reaction: discord.Reaction, user: discord.Member):
    # This event is for showing who upvoted/downvoted
    # It requires checking against PPP manager role and the poll's target user
    if user.bot: return

    if reaction.message.channel.id != POLL_VOTING_CHANNEL_ID: return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT poll_id, target_user_id FROM polls WHERE message_id = %s",
                (reaction.message.id,)
            )
            poll_data = await cur.fetchone()
            if not poll_data: return

            # Check if the user is the target of the poll or a PPP manager
            if user.id == poll_data['target_user_id'] or is_ppp_manager_role_check(user):
                if str(reaction.emoji) in ['👍', '👎']:
                    voters = []
                    async for reactor in reaction.users():
                        if not reactor.bot:
                            voters.append(reactor.mention)
                    
                    if voters:
                        vote_type_str = "Upvotes" if str(reaction.emoji) == '👍' else "Downvotes"
                        embed = create_embed(
                            f"Poll Voters: {vote_type_str}",
                            f"Users who reacted with {reaction.emoji} on this poll:\n" + ", ".join(voters)
                        )
                        await reaction.message.channel.send(embed=embed, delete_after=15)
            
def is_ppp_manager_role_check(member: discord.Member):
    """Helper to check if a member has the PPP Manager role."""
    return PPP_MANAGER_ROLE_ID in [role.id for role in member.roles]

# --- History Command ---

@bot.command(name='h', aliases=['history'])
async def history_command(ctx: commands.Context, member: discord.Member = None):
    """Shows past strikes, bans, and mutes of a person."""
    await ctx.message.add_reaction('✅')

    target_user = member or ctx.author
    user_id = target_user.id

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT * FROM moderation_logs WHERE user_id = %s ORDER BY timestamp DESC",
                (user_id,)
            )
            logs = await cur.fetchall()

            if not logs:
                await ctx.send(embed=create_embed("History", f"{target_user.display_name} has no recorded moderation history."))
                return

            page_size = 5
            total_pages = (len(logs) + page_size - 1) // page_size

            def get_page_embed(page_num):
                start_index = (page_num - 1) * page_size
                end_index = start_index + page_size
                current_page_logs = logs[start_index:end_index]

                embed = create_embed(
                    f"{target_user.display_name}'s Moderation History (Page {page_num}/{total_pages})",
                    "Recent actions:"
                )
                for log in current_page_logs:
                    action_time = log['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                    duration_info = f" ({timedelta(seconds=log['duration'])})" if log['duration'] else ""
                    strike_id_info = f" (ID: `{log['strike_id']}`)" if log['strike_id'] else ""
                    
                    action_desc = f"**{log['action'].capitalize()}** by <@{log['moderator_id']}> on {action_time}{duration_info}{strike_id_info}\nReason: {log['reason']}"
                    embed.add_field(name="\u200b", value=action_desc, inline=False) # Zero-width space for blank field name

                return embed

            current_page = 1
            message = await ctx.send(embed=get_page_embed(current_page))

            if total_pages > 1:
                await message.add_reaction('⬅️')
                await message.add_reaction('➡️')

                def check(reaction, user):
                    return user == ctx.author and str(reaction.emoji) in ['⬅️', '➡️'] and reaction.message.id == message.id

                while True:
                    try:
                        reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)

                        if str(reaction.emoji) == '➡️' and current_page < total_pages:
                            current_page += 1
                            await message.edit(embed=get_page_embed(current_page))
                        elif str(reaction.emoji) == '⬅️' and current_page > 1:
                            current_page -= 1
                            await message.edit(embed=get_page_embed(current_page))
                        
                        await message.remove_reaction(reaction, user)
                    except asyncio.TimeoutError:
                        await message.clear_reactions()
                        break
                    except discord.Forbidden:
                        print("Bot lacks permissions to remove reactions.")
                        break

# --- Info Card Command ---

@bot.command(name='i')
async def info_card_command(ctx: commands.Context, member: discord.Member = None):
    """Generates an info card image for a user's stats."""
    await ctx.message.add_reaction('✅')

    target_user = member or ctx.author
    user_data = await get_user_data_from_db(target_user.id)

    if not user_data or not user_data['is_registered']:
        embed = create_embed("Not Registered", f"{target_user.display_name} is not registered.")
        await ctx.send(embed=embed)
        return

    # Pass the actual user's avatar URL from Discord to get a default if skin fails
    avatar_url = target_user.avatar.url if target_user.avatar else target_user.default_avatar.url
    
    # Generate the image
    image_buffer = await generate_info_card_image(user_data, avatar_url)
    
    if image_buffer:
        try:
            await ctx.send(file=discord.File(image_buffer, filename="info_card.png"))
        except discord.Forbidden:
            await ctx.send(embed=create_embed("Permissions Error", "I lack permissions to send files."))
        except Exception as e:
            await ctx.send(embed=create_embed("Error", f"An error occurred while sending the info card: {e}"))
    else:
        await ctx.send(embed=create_embed("Image Generation Failed", "Could not generate info card image."))


# --- Leaderboard Commands ---

@bot.command(name='lb', aliases=['leaderboard'])
async def leaderboard_command(ctx: commands.Context, stat_type: str = 'elo'):
    """Displays various leaderboards."""
    await ctx.message.add_reaction('✅')

    valid_stats = {
        'wins': 'wins',
        'elo': 'elo',
        'losses': 'losses',
        'games': 'wins + losses + ties', # Calculated column
        'mvps': 'mvps',
        'streaks': 'current_streak'
    }

    if stat_type.lower() not in valid_stats:
        embed = create_embed(
            "Invalid Leaderboard Type",
            f"Available leaderboards: {', '.join(valid_stats.keys())}"
        )
        await ctx.send(embed=embed)
        return

    order_by_column = valid_stats[stat_type.lower()]
    
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Note: For 'games', we calculate on the fly.
            # To handle 'wins + losses + ties' in ORDER BY, it's safer to fetch all and sort in Python
            # or add a computed column/view in SQL if performance is critical for very large tables.
            query = f"SELECT discord_id, minecraft_ign, elo, wins, losses, ties, mvps, current_streak FROM users ORDER BY {order_by_column} DESC LIMIT 10"
            await cur.execute(query)
            top_players = await cur.fetchall()

            if not top_players:
                await ctx.send(embed=create_embed("Leaderboard Empty", "No players found to display on the leaderboard."))
                return

            leaderboard_text = ""
            for i, player in enumerate(top_players):
                value = player[stat_type.lower()] if stat_type.lower() != 'games' else (player['wins'] + player['losses'] + player['ties'])
                leaderboard_text += f"{i+1}. **[{player['elo']}] {player['minecraft_ign']}** - {stat_type.capitalize()}: {value}\n"
            
            embed = create_embed(f"Top 10 {stat_type.capitalize()} Leaderboard", leaderboard_text)
            await ctx.send(embed=embed)

# --- Admin Commands ---

@bot.group(name='admin', invoke_without_command=True)
@is_pi()
async def admin_group(ctx: commands.Context):
    """Admin commands for bot configuration. Only accessible by PI role."""
    await ctx.message.add_reaction('✅')
    if ctx.invoked_subcommand is None:
        embed = create_embed("Admin Commands", "Use `=admin <command>`.\n"
                             "Available commands: `set_partysize`, `queue`, `queues`, `purgeall`.")
        await ctx.send(embed=embed)

@admin_group.command(name='set_partysize')
@is_pi()
async def admin_set_partysize(ctx: commands.Context, size: str):
    """Sets the maximum party size (none, 2, 3, 4). 'none' means no party season."""
    await ctx.message.add_reaction('✅')
    global CURRENT_PARTY_SEASON, PARTY_SIZE_LIMIT

    if size.lower() == 'none':
        CURRENT_PARTY_SEASON = False
        PARTY_SIZE_LIMIT = None
        embed = create_embed("Party Season Status", "Party Season is now **OFF**. Captains will pick teams.")
    elif size.isdigit() and int(size) in [2, 3, 4]:
        CURRENT_PARTY_SEASON = True
        PARTY_SIZE_LIMIT = int(size)
        embed = create_embed("Party Season Status", f"Party Season is now **ON**. Max party size: **{PARTY_SIZE_LIMIT}**. Teams will be Elo-balanced.")
    else:
        embed = create_embed("Invalid Party Size", "Please use 'none', '2', '3', or '4'.")
    
    await ctx.send(embed=embed)

@admin_group.command(name='queue')
@is_pi()
async def admin_set_queue_status(ctx: commands.Context, queue_type: str, status: int):
    """Sets a specific queue to open (1) or closed (0)."""
    await ctx.message.add_reaction('✅')
    global QUEUE_OPEN_STATUS

    queue_type = queue_type.lower()
    if queue_type not in queues:
        embed = create_embed("Invalid Queue Type", f"Available queues: {', '.join(queues.keys())}")
        await ctx.send(embed=embed)
        return
    
    if status not in [0, 1]:
        embed = create_embed("Invalid Status", "Status must be 0 (closed) or 1 (open).")
        await ctx.send(embed=embed)
        return
    
    QUEUE_OPEN_STATUS[queue_type] = bool(status)
    status_text = "open" if bool(status) else "closed"
    embed = create_embed("Queue Status Updated", f"The `{queue_type}` queue is now **{status_text}**.")
    await ctx.send(embed=embed)

@admin_group.command(name='queues')
@is_pi()
async def admin_set_active_queues(ctx: commands.Context, *queue_types: str):
    """Sets which queues are active for the season (e.g., `3v3 4v4` or `3v3_pups+`)."""
    await ctx.message.add_reaction('✅')
    global QUEUE_OPEN_STATUS

    all_queues = list(queues.keys())
    # Close all queues first
    for q_type in all_queues:
        QUEUE_OPEN_STATUS[q_type] = False

    activated_queues = []
    for q_type_arg in queue_types:
        q_type_normalized = q_type_arg.lower().replace('_', ' ') # Handle 3v3_pups+
        if q_type_normalized in all_queues:
            QUEUE_OPEN_STATUS[q_type_normalized] = True
            activated_queues.append(q_type_normalized)
        else:
            await ctx.send(embed=create_embed("Warning", f"Queue type `{q_type_arg}` not recognized. Skipping."))
    
    if activated_queues:
        embed = create_embed(
            "Active Queues Updated",
            f"The following queues are now active: **{', '.join(activated_queues)}**.\n"
            f"All other queues are closed."
        )
    else:
        embed = create_embed("Active Queues Updated", "All queues are now **closed**.")
    
    await ctx.send(embed=embed)

@admin_group.command(name='purgeall')
@is_pi()
async def admin_purge_all(ctx: commands.Context):
    """Wipes all player stats and Elo from the database. **DANGEROUS COMMAND.**"""
    await ctx.message.add_reaction('✅')

    confirm_message = await ctx.send(embed=create_embed(
        "CONFIRM PURGE",
        "**WARNING: This will wipe ALL player data (Elo, stats, history) from the database.**\n"
        "Are you absolutely sure? Type `YES` to confirm."
    ))
    
    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content == 'YES'
    
    try:
        await bot.wait_for('message', check=check, timeout=30.0)
    except asyncio.TimeoutError:
        await ctx.send(embed=create_embed("Purge Cancelled", "Confirmation timed out. Purge cancelled."))
        return
    
    try:
        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Ensure tables exist before truncating to avoid errors on fresh DB
                await cur.execute("SHOW TABLES LIKE 'game_history'")
                if await cur.fetchone(): await cur.execute("TRUNCATE TABLE game_history")
                
                await cur.execute("SHOW TABLES LIKE 'moderation_logs'")
                if await cur.fetchone(): await cur.execute("TRUNCATE TABLE moderation_logs")
                
                await cur.execute("SHOW TABLES LIKE 'strike_requests'")
                if await cur.fetchone(): await cur.execute("TRUNCATE TABLE strike_requests")
                
                await cur.execute("SHOW TABLES LIKE 'ss_tickets'")
                if await cur.fetchone(): await cur.execute("TRUNCATE TABLE ss_tickets")
                
                await cur.execute("SHOW TABLES LIKE 'polls'")
                if await cur.fetchone(): await cur.execute("TRUNCATE TABLE polls")
                
                await cur.execute("SHOW TABLES LIKE 'poll_votes'")
                if await cur.fetchone(): await cur.execute("TRUNCATE TABLE poll_votes")
                
                # Reset users table (assuming 'users' table always exists after initial setup)
                await cur.execute("UPDATE users SET elo = 0, wins = 0, losses = 0, ties = 0, mvps = 0, current_streak = 0, last_game_date = NULL, is_registered = FALSE, registration_code = NULL")
                await conn.commit()

        # Reset nicknames and roles for all members in the guild (can be very slow for large guilds)
        guild = ctx.guild
        registered_role = guild.get_role(REGISTERED_ROLE_ID)
        elo_roles_to_remove = [guild.get_role(r_id) for r_id in ELO_ROLES.values() if guild.get_role(r_id)]
        
        for member in guild.members:
            if not member.bot:
                try:
                    if member.nick and member.nick.startswith('['): # Assuming [ELO] IGN format
                        await member.edit(nick=None)
                    
                    roles_to_remove = [r for r in member.roles if (registered_role and r == registered_role) or (r in elo_roles_to_remove)]
                    if roles_to_remove:
                        await member.remove_roles(*roles_to_remove)
                    # Add Iron role if needed after purge (re-registration handles this)
                except discord.Forbidden:
                    print(f"Bot lacks permissions to modify roles/nickname for {member.display_name} during purge.")
                except Exception as e:
                    print(f"Error purging roles/nickname for {member.display_name}: {e}")

        embed = create_embed("Purge Complete", "**All player stats and Elo have been wiped.**")
        await ctx.send(embed=embed)
        print(f"Purge All executed by {ctx.author.display_name}.")

    except Exception as e:
        await ctx.send(embed=create_embed("Error During Purge", f"An error occurred during purge: {e}"))


# --- Stat Modification Commands (Admin+ Only) ---
# These are moved out of the `admin` group but still require `is_admin_or_above`

@bot.command(name='wins')
@is_admin_or_above()
async def modify_wins(ctx: commands.Context, member: discord.Member, amount: int, mvp_status: int = 0):
    """Modifies a user's wins and updates Elo."""
    await ctx.message.add_reaction('✅')
    
    if amount < 0:
        await ctx.send(embed=create_embed("Invalid Amount", "Amount must be a positive integer."))
        return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            user_data = await get_user_data_from_db(member.id)
            if not user_data:
                await ctx.send(embed=create_embed("User Not Found", f"{member.display_name} is not registered."))
                return

            current_elo = user_data['elo']
            current_elo_role = await get_elo_role_name(current_elo)
            elo_model = ELO_MODELS.get(current_elo_role, ELO_MODELS['Iron'])

            elo_change = amount * elo_model['win']
            if mvp_status == 1:
                elo_change += amount * elo_model['mvp'] # Assuming MVP bonus per win

            await cur.execute(
                """
                UPDATE users
                SET wins = wins + %s,
                    elo = elo + %s,
                    mvps = mvps + %s,
                    current_streak = current_streak + %s,
                    last_game_date = %s
                WHERE discord_id = %s
                """,
                (amount, elo_change, amount if mvp_status == 1 else 0, amount, datetime.now(), member.id) # Increment streak by amount of wins
            )
            await conn.commit()

            updated_user_data = await get_user_data_from_db(member.id)
            await update_user_nickname(member, updated_user_data['elo'], updated_user_data['minecraft_ign'])
            await assign_elo_role(member, updated_user_data['elo'])

            embed = create_embed(
                "Wins Modified",
                f"Added {amount} wins, {elo_change} Elo, and {amount if mvp_status==1 else 0} MVPs to {member.mention}."
            )
            await ctx.send(embed=embed)

@bot.command(name='losses')
@is_admin_or_above()
async def modify_losses(ctx: commands.Context, member: discord.Member, amount: int):
    """Modifies a user's losses and updates Elo."""
    await ctx.message.add_reaction('✅')

    if amount < 0:
        await ctx.send(embed=create_embed("Invalid Amount", "Amount must be a positive integer."))
        return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            user_data = await get_user_data_from_db(member.id)
            if not user_data:
                await ctx.send(embed=create_embed("User Not Found", f"{member.display_name} is not registered."))
                return

            current_elo = user_data['elo']
            current_elo_role = await get_elo_role_name(current_elo)
            elo_model = ELO_MODELS.get(current_elo_role, ELO_MODELS['Iron'])

            elo_change = - (amount * elo_model['loss']) # Deduct Elo for losses

            await cur.execute(
                """
                UPDATE users
                SET losses = losses + %s,
                    elo = elo + %s,
                    current_streak = 0, -- Reset streak on loss
                    last_game_date = %s
                WHERE discord_id = %s
                """,
                (amount, elo_change, datetime.now(), member.id)
            )
            await conn.commit()

            updated_user_data = await get_user_data_from_db(member.id)
            await update_user_nickname(member, updated_user_data['elo'], updated_user_data['minecraft_ign'])
            await assign_elo_role(member, updated_user_data['elo'])

            embed = create_embed(
                "Losses Modified",
                f"Added {amount} losses and {elo_change} Elo to {member.mention}."
            )
            await ctx.send(embed=embed)

@bot.command(name='elochange')
@is_admin_or_above()
async def elo_change(ctx: commands.Context, member: discord.Member, amount: int):
    """Increments or decrements a user's Elo by a specific amount."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            user_data = await get_user_data_from_db(member.id)
            if not user_data:
                await ctx.send(embed=create_embed("User Not Found", f"{member.display_name} is not registered."))
                return

            await cur.execute(
                "UPDATE users SET elo = elo + %s, last_game_date = %s WHERE discord_id = %s",
                (amount, datetime.now(), member.id)
            )
            await conn.commit()

            updated_user_data = await get_user_data_from_db(member.id)
            await update_user_nickname(member, updated_user_data['elo'], updated_user_data['minecraft_ign'])
            await assign_elo_role(member, updated_user_data['elo'])

            action = "incremented" if amount >= 0 else "decremented"
            embed = create_embed(
                "Elo Changed",
                f"{member.mention}'s Elo has been {action} by {abs(amount)}. New Elo: {updated_user_data['elo']}."
            )
            await ctx.send(embed=embed)

@bot.command(name='elo')
@is_admin_or_above()
async def set_elo(ctx: commands.Context, member: discord.Member, new_elo: int):
    """Sets a user's Elo to a specific value."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            user_data = await get_user_data_from_db(member.id)
            if not user_data:
                await ctx.send(embed=create_embed("User Not Found", f"{member.display_name} is not registered."))
                return

            await cur.execute(
                "UPDATE users SET elo = %s, last_game_date = %s WHERE discord_id = %s",
                (new_elo, datetime.now(), member.id)
            )
            await conn.commit()

            updated_user_data = await get_user_data_from_db(member.id)
            await update_user_nickname(member, updated_user_data['elo'], updated_user_data['minecraft_ign'])
            await assign_elo_role(member, updated_user_data['elo'])

            embed = create_embed(
                "Elo Set",
                f"{member.mention}'s Elo has been set to **{new_elo}**."
            )
            await ctx.send(embed=embed)

# --- Game Result Modification Commands (Admin+ Only) ---

@bot.command(name='vg')
@is_admin_or_above()
async def view_game_details(ctx: commands.Context, game_no: int):
    """Displays the players and teams of a specific game."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM game_history WHERE game_id = %s", (game_no,))
            game_data = await cur.fetchone()

            if not game_data:
                await ctx.send(embed=create_embed("Game Not Found", f"Game #{game_no} not found in history."))
                return
            
            team1_players_ign = json.loads(game_data['team1_players'])
            team2_players_ign = json.loads(game_data['team2_players'])
            mvp_user_id = game_data['mvp_user_id']
            
            mvp_mention = f"<@{mvp_user_id}>" if mvp_user_id else "N/A"

            embed = create_embed(
                f"Game #{game_no} Details ({game_data['queue_type']})",
                f"Map: **{game_data['map_name']}**\n"
                f"Scored by: **{game_data['scored_by'].capitalize()}**\n"
                f"Winning Team: **{game_data['winning_team'].capitalize()}**\n"
                f"MVP: {mvp_mention}"
            )
            embed.add_field(name="Team 1", value=", ".join(team1_players_ign), inline=True)
            embed.add_field(name="Team 2", value=", ".join(team2_players_ign), inline=True)
            embed.add_field(name="Undone", value="Yes" if game_data['is_undone'] else "No", inline=True)
            embed.set_footer(text="asrbw.fun")
            await ctx.send(embed=embed)

@bot.command(name='undo')
@is_admin_or_above()
async def undo_game_score(ctx: commands.Context, game_no: int):
    """Undoes the scoring of a game, reverting Elo and stats."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM game_history WHERE game_id = %s AND is_undone = FALSE", (game_no,))
            game_data = await cur.fetchone()

            if not game_data:
                await ctx.send(embed=create_embed("Game Not Found or Already Undone", f"Game #{game_no} not found or has already been undone."))
                return
            
            team1_igns = json.loads(game_data['team1_players'])
            team2_igns = json.loads(game_data['team2_players'])
            winning_team_label = game_data['winning_team']
            mvp_user_id = game_data['mvp_user_id']
            
            winning_igns = team1_igns if winning_team_label == 'team1' else team2_igns
            losing_igns = team2_igns if winning_team_label == 'team1' else team1_igns

            # Revert Elo and stats
            all_player_igns = winning_igns + losing_igns
            
            for ign in all_player_igns:
                await cur.execute(f"SELECT discord_id, elo, wins, losses, mvps, current_streak FROM users WHERE minecraft_ign = %s", (ign,))
                user_data = await cur.fetchone()
                if not user_data: continue
                
                discord_id = user_data['discord_id']
                current_elo_role = await get_elo_role_name(user_data['elo'])
                elo_model = ELO_MODELS.get(current_elo_role, ELO_MODELS['Iron'])

                wins_change = 0
                losses_change = 0
                mvps_change = 0
                elo_change = 0
                streak_change = 0 # This will be set based on revert logic

                if ign in winning_igns: # Revert winner's stats
                    wins_change = -1
                    elo_change = -elo_model['win']
                    if discord_id == mvp_user_id:
                        mvps_change = -1
                        elo_change -= elo_model['mvp']
                    # Revert streak: Decrement streak by 1. If it was 1, set to 0.
                    streak_change = -1 if user_data['current_streak'] > 0 else 0 
                else: # Revert loser's stats
                    losses_change = -1
                    elo_change = elo_model['loss'] # Add back what was lost
                    # Streak: If a loser had their streak reset, undoing means their streak might need to be restored.
                    # This is complex without full historical streak tracking. For now, assume it was reset to 0 and cannot be "un-reset" simply.
                    streak_change = 0 # No change to streak on undoing a loss that resulted in a reset

                await cur.execute(
                    """
                    UPDATE users
                    SET wins = wins + %s,
                        losses = losses + %s,
                        elo = elo + %s,
                        mvps = mvps + %s,
                        current_streak = CASE WHEN (current_streak + %s) >= 0 THEN current_streak + %s ELSE 0 END, -- Ensure streak doesn't go negative
                        last_game_date = %s
                    WHERE discord_id = %s
                    """,
                    (wins_change, losses_change, elo_change, mvps_change, streak_change, streak_change, datetime.now(), discord_id)
                )
                
                member = ctx.guild.get_member(discord_id)
                if member:
                    updated_user_data = await get_user_data_from_db(discord_id)
                    if updated_user_data:
                        await update_user_nickname(member, updated_user_data['elo'], updated_user_data['minecraft_ign'])
                        await assign_elo_role(member, updated_user_data['elo'])

            await cur.execute(
                "UPDATE game_history SET is_undone = TRUE, scored_by = CONCAT('UNDONE BY ', %s, ' (Original: ', scored_by, ')') WHERE game_id = %s",
                (ctx.author.display_name, game_no)
            )
            await conn.commit()

            embed = create_embed(
                "Game Score Undone",
                f"Game #{game_no}'s scoring has been **undone**. Player stats and Elo have been reverted."
            )
            await ctx.send(embed=embed)
            print(f"{ctx.author.display_name} undone game {game_no}.")

@bot.command(name='rescore')
@is_admin_or_above()
async def rescore_game(ctx: commands.Context, game_no: int, winning_team_label: str, mvp_member: discord.Member):
    """Rescores a game, applying new Elo and stats based on new winner/MVP."""
    await ctx.message.add_reaction('✅')

    if winning_team_label.lower() not in ['team1', 'team2']:
        await ctx.send(embed=create_embed("Invalid Team", "Winning team must be 'team1' or 'team2'."))
        return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM game_history WHERE game_id = %s", (game_no,))
            game_data = await cur.fetchone()

            if not game_data:
                await ctx.send(embed=create_embed("Game Not Found", f"Game #{game_no} not found."))
                return
            
            if not game_data['is_undone']:
                await ctx.send(embed=create_embed("Game Not Undone", f"Game #{game_no} must be undone first before it can be rescoreed. Use `=undo {game_no}`."))
                return

            team1_igns = json.loads(game_data['team1_players'])
            team2_igns = json.loads(game_data['team2_players'])
            
            winning_igns = team1_igns if winning_team_label.lower() == 'team1' else team2_igns
            losing_igns = team2_igns if winning_team_label.lower() == 'team1' else team1_igns

            mvp_ign = (await get_user_data_from_db(mvp_member.id))['minecraft_ign'] if (await get_user_data_from_db(mvp_member.id)) else None
            if not mvp_ign:
                await ctx.send(embed=create_embed("MVP Not Registered", "The selected MVP is not registered."))
                return

            # Apply new scores and Elo
            all_player_igns = winning_igns + losing_igns
            player_discord_ids = {} # {IGN: Discord_ID}

            # Fetch Discord IDs and current Elos for all players
            if all_player_igns:
                placeholders = ','.join(['%s'] * len(all_player_igns))
                await cur.execute(
                    f"SELECT discord_id, minecraft_ign, elo, wins, losses, mvps, current_streak FROM users WHERE minecraft_ign IN ({placeholders})",
                    tuple(all_player_igns)
                )
                fetched_players = await cur.fetchall()
                for p in fetched_players:
                    player_discord_ids[p['minecraft_ign']] = p['discord_id']

            updates = []
            
            for ign in all_player_igns:
                discord_id = player_discord_ids.get(ign)
                if not discord_id: continue

                user_data = next((p for p in fetched_players if p['discord_id'] == discord_id), None)
                if not user_data: continue

                current_elo_role = await get_elo_role_name(user_data['elo'])
                elo_model = ELO_MODELS.get(current_elo_role, ELO_MODELS['Iron'])

                win_change = 0
                loss_change = 0
                mvp_change = 0
                elo_gain_or_loss = 0
                streak_change = 0

                if ign in winning_igns: # Winning team
                    win_change = 1
                    elo_gain_or_loss = elo_model['win']
                    streak_change = 1
                else: # Losing team
                    loss_change = 1
                    elo_gain_or_loss = -elo_model['loss']
                    streak_change = -user_data['current_streak'] - 1 # Reset and decrement streak

                if ign == mvp_ign:
                    mvp_change = 1
                    elo_gain_or_loss += elo_model['mvp']
                
                updates.append({
                    "discord_id": discord_id,
                    "wins_change": win_change,
                    "losses_change": loss_change,
                    "elo_change": elo_gain_or_loss,
                    "mvps_change": mvp_change,
                    "streak_change": streak_change
                })

            for update in updates:
                discord_id = update['discord_id']
                await cur.execute(
                    """
                    UPDATE users
                    SET wins = wins + %s,
                        losses = losses + %s,
                        elo = elo + %s,
                        mvps = mvps + %s,
                        current_streak = CASE WHEN %s > 0 THEN current_streak + %s ELSE 0 END,
                        last_game_date = %s
                    WHERE discord_id = %s
                    """,
                    (update['wins_change'], update['losses_change'], update['elo_change'], update['mvps_change'],
                     update['wins_change'], update['streak_change'], datetime.now(), discord_id)
                )

                member = ctx.guild.get_member(discord_id)
                if member:
                    updated_user_data = await get_user_data_from_db(discord_id)
                    if updated_user_data:
                        await update_user_nickname(member, updated_user_data['elo'], updated_user_data['minecraft_ign'])
                        await assign_elo_role(member, updated_user_data['elo'])

            await cur.execute(
                "UPDATE game_history SET winning_team = %s, mvp_user_id = %s, is_undone = FALSE, scored_by = %s WHERE game_id = %s",
                (winning_team_label.lower(), mvp_member.id, f"RESCORED BY {ctx.author.display_name}", game_no)
            )
            await conn.commit()

            embed = create_embed(
                "Game Rescored",
                f"Game #{game_no} has been **rescored**.\n"
                f"New Winning Team: **{winning_team_label.capitalize()}**\n"
                f"New MVP: {mvp_member.mention}"
            )
            await ctx.send(embed=embed)
            # Re-send game result image if needed, or update the old one
            await send_game_result_image(
                game_no, team1_igns, team2_igns,
                winning_igns, mvp_ign, game_data['map_name']
            )

@bot.command(name='score')
@is_admin_or_above()
async def score_game(ctx: commands.Context, game_no: int, winning_team_label: str, mvp_member: discord.Member):
    """Manually scores a game, applying Elo and stats."""
    await ctx.message.add_reaction('✅')

    if winning_team_label.lower() not in ['team1', 'team2']:
        await ctx.send(embed=create_embed("Invalid Team", "Winning team must be 'team1' or 'team2'."))
        return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Check if game exists and is not already scored
            await cur.execute("SELECT * FROM game_history WHERE game_id = %s", (game_no,))
            existing_game = await cur.fetchone()
            
            if existing_game and not existing_game['is_undone']:
                await ctx.send(embed=create_embed("Game Already Scored", f"Game #{game_no} is already scored. If you need to change it, use `=undo {game_no}` then `=rescore {game_no}`."))
                return
            elif not existing_game:
                # If game doesn't exist, we need player info. This is where it gets tricky without MC plugin info.
                await ctx.send(embed=create_embed("Game Not Found", "Cannot score a game that doesn't exist in history. This command assumes game details (players, map) are already logged from MC. If you are manually adding a new game, you need to provide player IGNs for each team."))
                return

            # If it exists but is undone, we proceed to rescore it.
            # Logic similar to `rescore_game`
            await ctx.invoke(bot.get_command('rescore'), game_no=game_no, winning_team_label=winning_team_label, mvp_member=mvp_member)


# --- Ticket System ---

@bot.group(name='ticket', invoke_without_command=True) # Changed from @bot.command to @bot.group
async def ticket_group(ctx: commands.Context):
    """Handles ticket system commands."""
    await ctx.message.add_reaction('✅')
    if ctx.invoked_subcommand is None:
        embed = create_embed(
            "Ticket System",
            "Use `=ticket create <type>` to open a new ticket.\n"
            "Available types: `general`, `appeal`, `store`, `screenshareappeal` / `ssappeal`."
        )
        await ctx.send(embed=embed, delete_after=10) # Auto-delete instructions if no subcommand

@ticket_group.command(name='create')
async def create_ticket(ctx: commands.Context, ticket_type: str):
    """Creates a new ticket."""
    await ctx.message.add_reaction('✅')

    if ctx.channel.id != TICKETS_CHANNEL_ID:
        embed = create_embed("Incorrect Channel", f"Please use the <#{TICKETS_CHANNEL_ID}> channel to create tickets.")
        await ctx.send(embed=embed, delete_after=3)
        return
    
    valid_types = ['general', 'appeal', 'store', 'screenshareappeal', 'ssappeal']
    ticket_type = ticket_type.lower()
    if ticket_type not in valid_types:
        embed = create_embed("Invalid Ticket Type", f"Available types: {', '.join(valid_types)}. This message will be deleted in 3 seconds.")
        await ctx.send(embed=embed, delete_after=3)
        return

    # Normalize ssappeal
    if ticket_type == 'ssappeal':
        ticket_type = 'screenshareappeal'

    guild = ctx.guild
    category = guild.get_channel(TICKET_CATEGORY_ID)
    if not category:
        await ctx.send(embed=create_embed("Error", "Ticket category not found. Please configure `TICKET_CATEGORY_ID`."))
        return

    channel_name = f"{ticket_type}-{ctx.author.name.lower().replace(' ', '-')}-{random.randint(1000,9999)}"
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        guild.get_role(STAFF_ROLE_ID): discord.PermissionOverwrite(read_messages=True, send_messages=True),
        guild.get_role(MOD_ROLE_ID): discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    try:
        ticket_channel = await category.create_text_channel(channel_name, overwrites=overwrites)
    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to create channels."))
        return
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred creating the channel: {e}"))
        return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO tickets (user_id, ticket_type, channel_id, status) VALUES (%s, %s, %s, %s)",
                (ctx.author.id, ticket_type, ticket_channel.id, 'open')
            )
            ticket_id = cur.lastrowid
            await conn.commit()

    embed = create_embed(
        f"Ticket #{ticket_id} - {ticket_type.capitalize()}",
        f"Thank you for opening a ticket! A staff member will be with you shortly.\n\n"
        f"User: {ctx.author.mention}"
    )

    claim_button = discord.ui.Button(label="Claim", style=discord.ButtonStyle.primary, custom_id="ticket_claim")
    close_button = discord.ui.Button(label="Close", style=discord.ButtonStyle.danger, custom_id="ticket_close")

    view = discord.ui.View(timeout=None)
    view.add_item(claim_button)
    view.add_item(close_button)

    async def claim_callback(interaction: discord.Interaction):
        member_roles = [role.id for role in interaction.user.roles]
        if not any(role_id in member_roles for role_id in [STAFF_ROLE_ID, MOD_ROLE_ID, ADMIN_ROLE_ID, MANAGER_ROLE_ID, PI_ROLE_ID]): # Simplified staff check
            await interaction.response.send_message("You are not authorized to claim tickets.", ephemeral=True)
            return
        
        async with bot.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT status FROM tickets WHERE ticket_id = %s", (ticket_id,))
                ticket_status = await cur.fetchone()
                if ticket_status and ticket_status['status'] == 'claimed':
                    await interaction.response.send_message("This ticket is already claimed.", ephemeral=True)
                    return
                await cur.execute("UPDATE tickets SET status = 'claimed', claimed_by_id = %s WHERE ticket_id = %s", (interaction.user.id, ticket_id))
                await conn.commit()

        await interaction.response.edit_message(content=f"Ticket claimed by {interaction.user.mention}.", view=None, embed=None) # Remove buttons
        await ticket_channel.send(embed=create_embed("Ticket Claimed", f"This ticket has been claimed by {interaction.user.mention}."))
        embed_log = create_embed("Ticket Claimed", f"Ticket #{ticket_id} ({ticket_type}) claimed by {interaction.user.mention}.")
        await log_action_to_channel(TICKET_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color)

    async def close_callback(interaction: discord.Interaction):
        # Anyone can close, but staff will get log, user will get moved to closed category
        await interaction.response.send_message("Please provide a reason to close this ticket in the chat.", ephemeral=True)
        # This part of the interaction is intentionally left simple here, as the =close command is intended to handle the full closure logic.

    claim_button.callback = claim_callback
    close_button.callback = close_button # The =close command will handle the actual closure.

    await ticket_channel.send(embed=embed, view=view)
    await ctx.send(embed=create_embed("Ticket Created", f"Your ticket has been created in {ticket_channel.mention}."))
    embed_log = create_embed("New Ticket Created", f"Ticket #{ticket_id} ({ticket_type}) created by {ctx.author.mention} in {ticket_channel.mention}.")
    await log_action_to_channel(TICKET_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color)


@bot.command(name='close')
async def close_ticket_command(ctx: commands.Context, *, reason: str = "No reason provided."):
    """Closes the current ticket channel."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM tickets WHERE channel_id = %s AND status IN ('open', 'claimed')", (ctx.channel.id,))
            ticket_data = await cur.fetchone()

            if not ticket_data:
                await ctx.send(embed=create_embed("Not an Active Ticket", "This channel is not an open or claimed ticket."))
                return

            ticket_id = ticket_data['ticket_id']
            user_id = ticket_data['user_id']
            ticket_type = ticket_data['ticket_type']

            # Generate HTML log
            log_html_filename = await generate_html_log_and_send(
                ctx.channel, f"ticket-log-{ticket_id}", TICKET_LOGS_CHANNEL_ID
            )

            await cur.execute(
                "UPDATE tickets SET status = 'closed', closed_at = %s, close_reason = %s, log_html_url = %s WHERE ticket_id = %s",
                (datetime.now(), reason, log_html_filename, ticket_id)
            )
            await conn.commit()

            embed = create_embed(
                "Ticket Closed",
                f"Ticket #{ticket_id} ({ticket_type}) has been closed by {ctx.author.mention}.\n"
                f"Reason: {reason}"
            )
            await ctx.send(embed=embed)
            embed_log = create_embed("Ticket Closed", f"Ticket #{ticket_id} ({ticket_type}) closed by {ctx.author.mention}. Reason: {reason}.")
            await log_action_to_channel(TICKET_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color)

            # Move to closed tickets category
            closed_category = ctx.guild.get_channel(CLOSED_TICKETS_CATEGORY_ID)
            if closed_category:
                try:
                    # Update channel permissions for the user to only read
                    original_opener = ctx.guild.get_member(user_id)
                    if original_opener:
                        await ctx.channel.set_permissions(original_opener, read_messages=True, send_messages=False)
                    await ctx.channel.edit(category=closed_category, sync_permissions=False)
                except discord.Forbidden:
                    print(f"Bot lacks permissions to move or modify permissions for channel {ctx.channel.name}.")
                except Exception as e:
                    print(f"Error moving channel {ctx.channel.name}: {e}")
            else:
                print("Closed tickets category not found. Cannot move channel.")

@bot.command(name='delete')
@is_staff() # Only staff can delete
async def delete_ticket_command(ctx: commands.Context, *, reason: str = "No reason provided."):
    """Deletes a ticket channel."""
    await ctx.message.add_reaction('✅')

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Check if this is a ticket channel
            await cur.execute("SELECT * FROM tickets WHERE channel_id = %s", (ctx.channel.id,))
            ticket_data = await cur.fetchone()

            if not ticket_data:
                await ctx.send(embed=create_embed("Not a Ticket Channel", "This command can only be used in a ticket channel."))
                return
            
            ticket_id = ticket_data['ticket_id']
            ticket_type = ticket_data['ticket_type']

            embed = create_embed(
                "Deleting Ticket",
                f"Ticket #{ticket_id} ({ticket_type}) is being deleted by {ctx.author.mention}.\n"
                f"Reason: {reason}"
            )
            await ctx.send(embed=embed) # Send message before deleting channel

            # Generate HTML log before deleting channel
            log_html_filename = await generate_html_log_and_send(
                ctx.channel, f"ticket-log-deleted-{ticket_id}", TICKET_LOGS_CHANNEL_ID
            )

            # Delete from DB
            await cur.execute("DELETE FROM tickets WHERE ticket_id = %s", (ticket_id,))
            await conn.commit()

            embed_log = create_embed("Ticket Deleted", f"Ticket #{ticket_id} ({ticket_type}) deleted by {ctx.author.mention}. Reason: {reason}.")
            await log_action_to_channel(TICKET_LOGS_CHANNEL_ID, embed_log.title, embed_log.description, embed_log.color)

            try:
                await ctx.channel.delete()
            except discord.Forbidden:
                print(f"Bot lacks permissions to delete channel {ctx.channel.name}.")
            except Exception as e:
                print(f"Error deleting channel {ctx.channel.name}: {e}")


# --- Staff Updates (on_member_update) ---
@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """Monitors role changes for staff updates (promotion/demotion)."""
    # Define your staff roles hierarchy (lower to higher)
    staff_roles_hierarchy = {
        STAFF_ROLE_ID: "Staff",
        MOD_ROLE_ID: "Moderator",
        ADMIN_ROLE_ID: "Admin",
        MANAGER_ROLE_ID: "Manager",
        PI_ROLE_ID: "PI"
    }

    # Get roles before and after the update
    before_roles = {role.id for role in before.roles}
    after_roles = {role.id for role in after.roles}

    # Filter for only roles relevant to the hierarchy
    before_staff_roles_filtered = {
        role_id: staff_roles_hierarchy[role_id]
        for role_id in before_roles if role_id in staff_roles_hierarchy
    }
    after_staff_roles_filtered = {
        role_id: staff_roles_hierarchy[role_id]
        for role_id in after_roles if role_id in staff_roles_hierarchy
    }

    # Get the "highest" role before and after
    def get_highest_role(role_dict):
        if not role_dict:
            return None, -1
        highest_id = max(role_dict, key=lambda r_id: list(staff_roles_hierarchy.keys()).index(r_id))
        return role_dict[highest_id], list(staff_roles_hierarchy.keys()).index(highest_id)

    before_highest_name, before_highest_index = get_highest_role(before_staff_roles_filtered)
    after_highest_name, after_highest_index = get_highest_role(after_staff_roles_filtered)

    staff_updates_channel = bot.get_channel(STAFF_UPDATES_CHANNEL_ID)
    if not staff_updates_channel:
        print("Staff updates channel not found.")
        return

    # Promotion/Demotion Logic
    if after_highest_index > before_highest_index: # Promoted
        title = "Staff Promotion!"
        description = f"🎉 {after.mention} has been **promoted** to **{after_highest_name}**!"
        if before_highest_name:
            description += f" (from {before_highest_name})"
        embed = create_embed(title, description, color=discord.Color.green())
        await staff_updates_channel.send(embed=embed)
    elif after_highest_index < before_highest_index: # Demoted
        title = "Staff Demotion!"
        description = f"⬇️ {after.mention} has been **demoted** from **{before_highest_name}**!"
        if after_highest_name:
            description += f" (to {after_highest_name})"
        else:
            description += " (and no longer has staff roles)"
        embed = create_embed(title, description, color=discord.Color.red())
        await staff_updates_channel.send(embed=embed)


# --- Chat Purge ---
@bot.command(name='purgechat')
@is_admin_or_above()
async def purge_chat(ctx: commands.Context, message_id_or_none: int = None):
    """
    Purges chat messages.
    If message_id is provided, deletes messages until that ID.
    If no message_id, deletes all messages from the channel.
    """
    await ctx.message.add_reaction('✅')

    try:
        if message_id_or_none is None:
            # Purge all messages in the channel
            await ctx.channel.purge(limit=None)
            embed = create_embed("Chat Purged", f"All messages in {ctx.channel.mention} have been purged by {ctx.author.mention}.")
            await ctx.send(embed=embed)
        else:
            # Purge messages until a specific message ID
            target_message = await ctx.channel.fetch_message(message_id_or_none)
            # Fetch history before the command message, up to but not including the target message
            deleted_count = 0
            async for message in ctx.channel.history(limit=None, before=ctx.message, after=target_message):
                await message.delete()
                deleted_count += 1
            
            # Delete the target message and the command message
            await target_message.delete()
            await ctx.message.delete()

            await ctx.send(embed=create_embed("Chat Purged", f"Purged {deleted_count} messages in {ctx.channel.mention} until message ID `{message_id_or_none}` by {ctx.author.mention}."))
            

    except discord.Forbidden:
        await ctx.send(embed=create_embed("Permissions Error", "I do not have permission to delete messages."))
    except discord.NotFound:
        await ctx.send(embed=create_embed("Message Not Found", "The specified message ID was not found in this channel."))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"An error occurred during chat purge: {e}"))


# --- Main Bot Run ---
if __name__ == '__main__':
    # It's good practice to load environment variables from a .env file for local development
    # from dotenv import load_dotenv
    # load_dotenv()

    # Basic check for token existence
    if not DISCORD_BOT_TOKEN or DISCORD_BOT_TOKEN == 'YOUR_DISCORD_BOT_TOKEN_HERE':
        print("Error: DISCORD_BOT_TOKEN environment variable not set or is placeholder.")
        print("Please set the DISCORD_BOT_TOKEN in your environment or .env file.")
        exit(1)

    # You might want to fetch the last game number from DB here before bot.run if it's crucial for initial state
    # async def fetch_last_game_num():
    #     global last_game_number
    #     async with bot.db_pool.acquire() as conn:
    #         async with conn.cursor() as cur:
    #             await cur.execute("SELECT MAX(game_id) FROM game_history")
    #             result = await cur.fetchone()
    #             if result and result[0] is not None:
    #                 last_game_number = result[0]
    # await fetch_last_game_num() # Requires running async outside of bot.run

    bot.run(DISCORD_BOT_TOKEN)

