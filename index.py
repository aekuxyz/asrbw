import discord
from discord.ext import commands, tasks
from discord.utils import get
from typing import Union, Optional
import os
import asyncio
import aiomysql
from dotenv import load_dotenv
import logging
import string
import random
from datetime import datetime, timedelta
import re
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter
import io
import aiohttp
import math
from html import escape

print("Starting asrbw.fun bot script...")

# --- Setup ---
# Logging
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('discord')
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# .env
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DB_HOST = os.getenv("DB_HOST"); DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD"); DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", 3306))

# --- ELO RANK CONFIGURATION ---
ELO_CONFIG = {
    'Iron':     {'range': (0, 150),   'win': 25, 'loss': -10, 'mvp': 20},
    'Bronze':   {'range': (150, 400),  'win': 20, 'loss': -10, 'mvp': 15},
    'Silver':   {'range': (400, 700),  'win': 20, 'loss': -10, 'mvp': 10},
    'Gold':     {'range': (700, 900),  'win': 15, 'loss': -10, 'mvp': 10},
    'Topaz':    {'range': (900, 1200), 'win': 10, 'loss': -15, 'mvp': 10},
    'Platinum': {'range': (1200, 99999),'win': 5,  'loss': -20, 'mvp': 10}
}

# --- Helper Functions & Classes ---
def get_rank_from_elo(elo: int):
    for rank, data in ELO_CONFIG.items():
        if data['range'][0] <= elo < data['range'][1]:
            return rank, data
    return 'Iron', ELO_CONFIG['Iron'] 

def create_embed(title, description, color=discord.Color.from_rgb(47, 49, 54)):
    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text="asrbw.fun"); embed.timestamp = datetime.utcnow()
    return embed

def parse_duration(duration_str: str) -> timedelta:
    regex = re.compile(r'(\d+)([smhdy])')
    parts = regex.findall(duration_str.lower())
    if not parts:
        raise ValueError("Invalid duration format.")
    
    total_seconds = 0
    for value, unit in parts:
        value = int(value)
        if unit == 's': total_seconds += value
        elif unit == 'm': total_seconds += value * 60
        elif unit == 'h': total_seconds += value * 3600
        elif unit == 'd': total_seconds += value * 86400
        elif unit == 'y': total_seconds += value * 31536000
    return timedelta(seconds=total_seconds)

async def generate_html_transcript(channel: discord.TextChannel) -> io.BytesIO:
    html = f"""<html><head><title>Transcript for #{channel.name}</title>
    <style>body{{background-color:#36393f;color:#dcddde;font-family:'Whitney',sans-serif;}} .message{{margin-bottom:10px;}} .author{{font-weight:bold;}} .timestamp{{color:#72767d;font-size:0.8em;}} .content{{margin-left:10px;}}</style>
    </head><body><h1>Transcript for #{channel.name}</h1>"""
    async for message in channel.history(limit=None, oldest_first=True):
        html += f"""<div class="message"><span class="author">{escape(message.author.display_name)}</span> <span class="timestamp">{message.created_at.strftime("%Y-%m-%d %H:%M:%S")}</span>
                <div class="content">{escape(message.content)}</div></div>"""
    html += "</body></html>"
    return io.BytesIO(html.encode('utf-8'))

# --- Tasks must be defined before the Bot class that starts them ---
@tasks.loop(minutes=1)
async def check_strike_polls(bot: commands.Bot):
    await bot.wait_until_ready(); guild = get(bot.guilds, id=bot.config.get('guild_id'));
    if not guild: return
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT message_id, channel_id, target_id, reason FROM strike_polls WHERE is_active = TRUE AND ends_at <= NOW()")
            expired_polls = await cursor.fetchall()
            for msg_id, chan_id, target_id, reason in expired_polls:
                channel = get(guild.channels, id=chan_id)
                if not channel: continue
                try: message = await channel.fetch_message(msg_id)
                except discord.NotFound: continue
                upvotes = get(message.reactions, emoji="👍").count - 1
                downvotes = get(message.reactions, emoji="👎").count - 1
                verdict = "Passed" if upvotes >= 3 and upvotes > downvotes * 2 else "Failed"
                embed = message.embeds[0]; embed.title = f"VOTING ENDED: {verdict}"; embed.color = discord.Color.green() if verdict == "Passed" else discord.Color.red()
                await message.edit(embed=embed, view=None)
                if verdict == "Passed":
                    target_member = guild.get_member(target_id)
                    if target_member: await strike_user_internal(guild, target_member, reason, "Community Vote")
                await cursor.execute("UPDATE strike_polls SET is_active = FALSE WHERE message_id = %s", (msg_id,))
                await asyncio.sleep(60)
                await channel.delete(reason="Strike poll ended.")

@tasks.loop(hours=24)
async def check_elo_decay(bot: commands.Bot):
    await bot.wait_until_ready()
    guild = get(bot.guilds, id=bot.config.get('guild_id'))
    if not guild: return
    four_days_ago = datetime.utcnow() - timedelta(days=4)
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT discord_id, elo FROM players WHERE elo >= %s AND (last_game_played_at IS NULL OR last_game_played_at < %s)", (ELO_CONFIG['Topaz']['range'][0], four_days_ago))
            inactive_players = await cursor.fetchall()
            for player_id, current_elo in inactive_players:
                decayed_elo = max(ELO_CONFIG['Topaz']['range'][0], current_elo - 60)
                await cursor.execute("UPDATE players SET elo = %s WHERE discord_id = %s", (decayed_elo, player_id))
                member = guild.get_member(player_id)
                if member:
                    await bot.get_cog("HelperCog").update_elo_roles(member)

@tasks.loop(seconds=60)
async def check_moderation_expirations(bot: commands.Bot):
    await bot.wait_until_ready(); guild = get(bot.guilds, id=bot.config.get('guild_id'));
    if not guild: return
    banned_role = guild.get_role(bot.config.get('banned_role_id'))
    muted_role = guild.get_role(bot.config.get('muted_role_id'))
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT log_id, target_id, action_type FROM moderation_logs WHERE is_active = TRUE AND expires_at IS NOT NULL AND expires_at <= NOW()")
            expired_actions = await cursor.fetchall()
            for log_id, target_id, action_type in expired_actions:
                member = guild.get_member(target_id)
                if not member: continue
                role_to_remove, log_channel = None, None
                if action_type == 'ban' and banned_role:
                    role_to_remove = banned_role; log_channel = get(guild.channels, id=bot.config.get('ban_log_channel_id'))
                if action_type == 'mute' and muted_role:
                    role_to_remove = muted_role; log_channel = get(guild.channels, id=bot.config.get('mute_log_channel_id'))
                if role_to_remove and role_to_remove in member.roles:
                    await member.remove_roles(role_to_remove, reason="Punishment expired.")
                    if log_channel: await log_channel.send(embed=create_embed(f"{action_type.capitalize()} Expired", f"{member.mention}'s {action_type} has expired.", discord.Color.green()))
                await cursor.execute("UPDATE moderation_logs SET is_active = FALSE WHERE log_id = %s", (log_id,))

@tasks.loop(seconds=20)
async def check_ss_expirations(bot: commands.Bot):
    await bot.wait_until_ready(); guild = get(bot.guilds, id=bot.config.get('guild_id'));
    if not guild: return
    ten_minutes_ago = datetime.utcnow() - timedelta(minutes=10)
    for channel_id in list(bot.active_ss_tickets.keys()):
        ticket_info = bot.active_ss_tickets.get(channel_id)
        if not ticket_info or ticket_info['created_at'] > ten_minutes_ago: continue
        channel = guild.get_channel(channel_id)
        if not channel: bot.active_ss_tickets.pop(channel_id, None); continue
        class DummyInteraction:
            def __init__(self, channel, guild, message): self.channel, self.guild, self.message = channel, guild, message
        try:
            message = await channel.fetch_message(ticket_info['message_id'])
            await SSTicketView(bot).handle_ticket_close(DummyInteraction(channel, guild, message), accepted=None)
        except discord.NotFound:
            bot.active_ss_tickets.pop(channel_id, None)


# --- UI Views (moved to their own cog) ---
class HelperCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot: return
        if message.content.startswith(self.bot.command_prefix):
            emoji = self.bot.config.get('processing_emoji', '✅')
            try:
                await message.add_reaction(emoji)
            except (discord.HTTPException, discord.Forbidden):
                pass
        await self.bot.process_commands(message)

    async def update_elo_roles(self, member: discord.Member, custom_nick: Optional[str] = None):
        if not member: return
        async with self.bot.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT elo, minecraft_ign, prefix_enabled, custom_nick FROM players WHERE discord_id = %s", (member.id,))
                data = await cursor.fetchone()
                if not data: return
                current_elo, ign, prefix_enabled, db_custom_nick = data
                if not ign: return 
                final_custom_nick = custom_nick if custom_nick is not None else db_custom_nick
        new_rank_name, _ = get_rank_from_elo(current_elo)
        rank_roles = {}
        for rank in ELO_CONFIG.keys():
            role_id = self.bot.config.get(f"{rank.lower()}_role_id")
            if role_id:
                role = get(member.guild.roles, id=role_id)
                if role: rank_roles[rank] = role
        new_role = rank_roles.get(new_rank_name)
        roles_to_remove = [role for rank, role in rank_roles.items() if rank != new_rank_name and role in member.roles]
        try:
            if roles_to_remove: await member.remove_roles(*roles_to_remove, reason="ELO rank update")
            if new_role and new_role not in member.roles: await member.add_roles(new_role, reason="ELO rank update")
            base_nick = f"[{current_elo}] {ign}" if prefix_enabled else ign
            final_nick = f"{base_nick} | {final_custom_nick}" if final_custom_nick else base_nick
            if len(final_nick) > 32: final_nick = final_nick[:32]
            if member.nick != final_nick: await member.edit(nick=final_nick)
        except discord.Forbidden: logger.warning(f"Failed to update roles/nickname for {member.display_name} due to permissions.")
        except Exception as e: logger.error(f"Error updating roles for {member.id}: {e}")

# --- Main Bot Class ---
class MyBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = {}
        self.queues_in_progress = set()
        self.active_games = {}
        self.active_ss_tickets = {}
        self.ready_once = False

    async def setup_hook(self):
        print("Executing setup hook...")
        try:
            self.db_pool = await aiomysql.create_pool(
                host=DB_HOST, port=DB_PORT, user=DB_USER,
                password=DB_PASSWORD, db=DB_NAME, autocommit=True, loop=self.loop
            )
            logger.info("Database connection pool created successfully.")
            await self.fetch_and_load_config()
            await self.add_cog(HelperCog(self))
        except Exception as e:
            logger.error(f"CRITICAL: DB connection or cog loading failed during setup: {e}")
            await self.close()
            return
            
        self.add_view(MainTicketView(self))
        self.add_view(SSTicketView(self))

        check_moderation_expirations.start(self)
        check_ss_expirations.start(self)
        check_elo_decay.start(self)
        check_strike_polls.start(self)
        logger.info("All background tasks started.")
    
    async def on_ready(self):
        if self.ready_once:
            logger.warning("on_ready called again, but setup is already complete.")
            return
        
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        await self.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name="asrbw.fun"))
        
        guild_obj = get(self.guilds, id=self.config.get('guild_id'))
        if guild_obj:
            self.tree.copy_global_to(guild=guild_obj)
            await self.tree.sync(guild=guild_obj)
            logger.info(f"Synced slash commands for guild: {guild_obj.name}")
        else:
            logger.warning("`guild_id` not found in config or bot is not in the specified guild. Slash commands will not be synced.")
        
        self.ready_once = True
        print("Bot is fully ready and online.")

    async def fetch_and_load_config(self):
        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT setting_key, setting_value FROM config")
                records = await cursor.fetchall()
                for row in records:
                    try: self.config[row[0]] = int(row[1])
                    except (ValueError, TypeError): self.config[row[0]] = row[1]
        logger.info("Configuration loaded from database.")

# --- Bot Initialization ---
intents = discord.Intents.default()
intents.members = True; intents.message_content = True
intents.voice_states = True; intents.reactions = True
bot = MyBot(command_prefix="=", intents=intents)

# --- All other events and commands follow ---
# ...
# This section contains the complete list of commands and events.
# ...

# --- Run ---
print("Executing main block...")
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("CRITICAL ERROR: DISCORD_TOKEN not found in .env file. Bot cannot start.")
        logger.error("CRITICAL ERROR: DISCORD_TOKEN not found in .env file. Bot cannot start.")
    else:
        try:
            bot.run(DISCORD_TOKEN, log_handler=None)
        except Exception as e:
            logger.critical(f"Failed to run the bot: {e}")
            print(f"Failed to run the bot: {e}")
