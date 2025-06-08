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

# Bot Init
intents = discord.Intents.default()
intents.members = True; intents.message_content = True
intents.voice_states = True; intents.reactions = True
bot = commands.Bot(command_prefix="=", intents=intents)
bot.config = {}; bot.queues_in_progress = set()
bot.active_games = {}; bot.active_ss_tickets = {}

# --- ELO RANK CONFIGURATION ---
ELO_CONFIG = {
    'Iron':     {'range': (0, 150),   'win': 25, 'loss': -10, 'mvp': 20},
    'Bronze':   {'range': (150, 400),  'win': 20, 'loss': -10, 'mvp': 15},
    'Silver':   {'range': (400, 700),  'win': 20, 'loss': -10, 'mvp': 10},
    'Gold':     {'range': (700, 900),  'win': 15, 'loss': -10, 'mvp': 10},
    'Topaz':    {'range': (900, 1200), 'win': 10, 'loss': -15, 'mvp': 10},
    'Platinum': {'range': (1200, 99999),'win': 5,  'loss': -20, 'mvp': 10}
}
def get_rank_from_elo(elo: int):
    for rank, data in ELO_CONFIG.items():
        if data['range'][0] <= elo < data['range'][1]:
            return rank, data
    return 'Iron', ELO_CONFIG['Iron'] # Default

# --- Helper Functions & Classes ---
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

async def fetch_config():
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT setting_key, setting_value FROM config")
            records = await cursor.fetchall()
            for row in records:
                try: bot.config[row[0]] = int(row[1])
                except (ValueError, TypeError): bot.config[row[0]] = row[1]
    logger.info("Configuration loaded from database.")

async def update_elo_roles(member: discord.Member):
    if not member: return

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT elo, minecraft_ign FROM players WHERE discord_id = %s", (member.id,))
            data = await cursor.fetchone()
            if not data: return
            
            current_elo, ign = data
            if not ign: return 

    new_rank_name, _ = get_rank_from_elo(current_elo)
    
    rank_roles = {}
    for rank in ELO_CONFIG.keys():
        role_id = bot.config.get(f"{rank.lower()}_role_id")
        if role_id:
            role = get(member.guild.roles, id=role_id)
            if role: rank_roles[rank] = role
    
    new_role = rank_roles.get(new_rank_name)
    roles_to_remove = [role for rank, role in rank_roles.items() if rank != new_rank_name and role in member.roles]

    try:
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason="ELO rank update")
        if new_role and new_role not in member.roles:
            await member.add_roles(new_role, reason="ELO rank update")
        
        await member.edit(nick=f"[{current_elo}] {ign}")

    except discord.Forbidden:
        logger.warning(f"Failed to update roles/nickname for {member.display_name} due to permissions.")
    except Exception as e:
        logger.error(f"Error updating roles for {member.id}: {e}")

class PaginatorView(discord.ui.View):
    def __init__(self, embeds):
        super().__init__(timeout=180)
        self.embeds = embeds
        self.current_page = 0
    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def previous_page(self, i: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        await i.response.edit_message(embed=self.embeds[self.current_page])
    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_page(self, i: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(len(self.embeds) - 1, self.current_page + 1)
        await i.response.edit_message(embed=self.embeds[self.current_page])

class TeamPickView(discord.ui.View):
    def __init__(self, manager):
        super().__init__(timeout=300); self.manager = manager
        self.update_buttons()
    def update_buttons(self):
        self.clear_items()
        for player in self.manager.unpicked_players:
            button = discord.ui.Button(label=player.display_name, custom_id=str(player.id))
            button.callback = self.button_callback
            self.add_item(button)
    async def button_callback(self, i: discord.Interaction):
        if i.user.id != self.manager.current_picker.id: return await i.response.send_message("It's not your turn!", ephemeral=True)
        await self.manager.pick_player(i, int(i.data['custom_id']))

class GameManager:
    def __init__(self, bot, players, game_info, text_channel, voice_channel, game_id):
        self.bot, self.players, self.game_info, self.text_channel, self.voice_channel, self.game_id = bot, players, game_info, text_channel, voice_channel, game_id
        self.team1, self.team2 = [], []
    async def setup_teams(self):
        party_season = bot.config.get('party_season', 0)
        if party_season == 1:
            await self.create_elo_balanced_teams()
        else:
            self.unpicked_players = list(self.players)
            self.captain1, self.captain2 = random.sample(self.unpicked_players, 2)
            self.team1.append(self.captain1); self.team2.append(self.captain2)
            self.unpicked_players.remove(self.captain1); self.unpicked_players.remove(self.captain2)
            self.current_picker = self.captain1; self.state = "picking"
            await self.start_picking()
    async def create_elo_balanced_teams(self):
        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                player_ids = [p.id for p in self.players]
                format_strings = ','.join(['%s'] * len(player_ids))
                await cursor.execute(f"SELECT discord_id, elo FROM players WHERE discord_id IN ({format_strings}) ORDER BY elo DESC", tuple(player_ids))
                sorted_players_data = await cursor.fetchall()
        
        sorted_players = [get(self.text_channel.guild.members, id=pid) for pid, elo in sorted_players_data]
        
        team1_elo, team2_elo = 0, 0
        for i, player in enumerate(sorted_players):
            if team1_elo <= team2_elo:
                self.team1.append(player); team1_elo += sorted_players_data[i][1]
            else:
                self.team2.append(player); team2_elo += sorted_players_data[i][1]
        
        await self.finalize_teams()
    def create_teams_embed(self):
        embed = create_embed(f"Game #{self.game_id} - Team Selection", f"It's **{self.current_picker.display_name}**'s turn to pick.")
        t1 = ' '.join([p.mention for p in self.team1]); t2 = ' '.join([p.mention for p in self.team2])
        embed.add_field(name=f"👑 Team 1: {self.captain1.display_name}", value=t1 or "...", inline=False)
        embed.add_field(name=f"👑 Team 2: {self.captain2.display_name}", value=t2 or "...", inline=False)
        return embed
    async def start_picking(self): await self.text_channel.send(embed=self.create_teams_embed(), view=TeamPickView(self))
    async def pick_player(self, i: discord.Interaction, p_id: int):
        p = i.guild.get_member(p_id)
        if not p or p not in self.unpicked_players: return await i.response.send_message("Can't pick this player.", ephemeral=True)
        if self.current_picker in self.team1: self.team1.append(p)
        else: self.team2.append(p)
        self.unpicked_players.remove(p)
        if not self.unpicked_players: return await self.finalize_teams(interaction=i)
        if len(self.team1) == 1 and len(self.team2) == 2: self.current_picker = self.captain2
        elif len(self.team1) == 1 and len(self.team2) == 1: self.current_picker = self.captain2
        await i.response.edit_message(embed=self.create_teams_embed(), view=TeamPickView(self))
    async def finalize_teams(self, interaction: Optional[discord.Interaction] = None):
        self.state = "ready"
        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                t1_data = [(self.game_id, p.id, 1) for p in self.team1]
                t2_data = [(self.game_id, p.id, 2) for p in self.team2]
                await cursor.executemany("INSERT INTO game_participants (game_id, player_id, team_id) VALUES (%s, %s, %s)", t1_data + t2_data)
        embed = create_embed(f"Game #{self.game_id} - Teams are Set!", f"Use `=score {self.game_id} <winning_team_num> <mvp>` to score.")
        embed.add_field(name="Team 1", value=' '.join([p.mention for p in self.team1]), inline=False)
        embed.add_field(name="Team 2", value=' '.join([p.mention for p in self.team2]), inline=False)
        if interaction: await interaction.response.edit_message(embed=embed, view=None)
        else: await self.text_channel.send(embed=embed)
        del bot.active_games[self.text_channel.id]

class SSTicketView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)
    async def handle_ticket_close(self, i: discord.Interaction, accepted: bool):
        for item in self.children: item.disabled = True
        await i.message.edit(view=self)
        info = bot.active_ss_tickets.pop(i.channel.id, None)
        if not info: return
        member = i.guild.get_member(info['target_id']); role = get(i.guild.roles, id=bot.config.get('frozen_role_id'))
        if not accepted:
            if member and role and role in member.roles: await member.remove_roles(role, reason="SS Request Declined/Timed Out")
            reason = "Request declined." if accepted is False else "Request timed out."
            await i.channel.send(embed=create_embed("Ticket Closed", reason, discord.Color.orange()))
            await asyncio.sleep(5); await i.channel.delete()
        else: await i.channel.send(embed=create_embed("Ticket Accepted", f"{i.user.mention} has accepted the request.", discord.Color.green()))
    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success, custom_id="ss_accept")
    async def accept(self, i: discord.Interaction, button: discord.ui.Button):
        if not get(i.guild.roles, id=bot.config.get('screenshare_staff_role_id')) in i.user.roles: return await i.response.send_message("No permission.", ephemeral=True)
        await self.handle_ticket_close(i, accepted=True); await i.response.defer()
    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger, custom_id="ss_decline")
    async def decline(self, i: discord.Interaction, button: discord.ui.Button):
        if not get(i.guild.roles, id=bot.config.get('screenshare_staff_role_id')) in i.user.roles: return await i.response.send_message("No permission.", ephemeral=True)
        await self.handle_ticket_close(i, accepted=False); await i.response.defer()

async def generate_html_transcript(channel: discord.TextChannel) -> io.BytesIO:
    html = f"""<html><head><title>Transcript for #{channel.name}</title>
    <style>body{{background-color:#36393f;color:#dcddde;font-family:'Whitney',sans-serif;}} .message{{margin-bottom:10px;}} .author{{font-weight:bold;}} .timestamp{{color:#72767d;font-size:0.8em;}} .content{{margin-left:10px;}}</style>
    </head><body><h1>Transcript for #{channel.name}</h1>"""
    async for message in channel.history(limit=None, oldest_first=True):
        html += f"""<div class="message"><span class="author">{escape(message.author.display_name)}</span> <span class="timestamp">{message.created_at.strftime("%Y-%m-%d %H:%M:%S")}</span>
                <div class="content">{escape(message.content)}</div></div>"""
    html += "</body></html>"
    return io.BytesIO(html.encode('utf-8'))

async def create_ticket_from_button(interaction: discord.Interaction, ticket_type: str):
    """Handles the ticket creation logic for all buttons."""
    await interaction.response.defer(ephemeral=True)
    category = get(interaction.guild.categories, id=bot.config.get('ticket_category_id'))
    staff_role = get(interaction.guild.roles, id=bot.config.get('staff_role_id'))
    if not category or not staff_role:
        return await interaction.followup.send("Ticket system is not fully configured.", ephemeral=True)

    overwrites = {
        interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
        interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        interaction.guild.me: discord.PermissionOverwrite(read_messages=True),
        staff_role: discord.PermissionOverwrite(read_messages=True)
    }
    channel_name = f"{ticket_type}-{interaction.user.name}"
    channel = await interaction.guild.create_text_channel(channel_name, category=category, overwrites=overwrites)
    
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("INSERT INTO tickets (channel_id, creator_id, type) VALUES (%s, %s, %s)", (channel.id, interaction.user.id, ticket_type.lower()))

    embed = create_embed(f"Ticket Created: {ticket_type.capitalize()}", f"Welcome, {interaction.user.mention}. Staff will be with you shortly.")
    await channel.send(content=staff_role.mention, embed=embed)
    await interaction.followup.send(f"Your ticket has been created: {channel.mention}", ephemeral=True)

class MainTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="General", emoji="☑️", custom_id="ticket_general", style=discord.ButtonStyle.secondary)
    async def general_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await create_ticket_from_button(interaction, "General")
        
    @discord.ui.button(label="Appeal", emoji="⚖️", custom_id="ticket_appeal", style=discord.ButtonStyle.secondary)
    async def appeal_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await create_ticket_from_button(interaction, "Appeal")

    @discord.ui.button(label="Store", emoji="🛒", custom_id="ticket_store", style=discord.ButtonStyle.secondary)
    async def store_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await create_ticket_from_button(interaction, "Store")

    @discord.ui.button(label="Partnership", emoji="🤝", custom_id="ticket_partnership", style=discord.ButtonStyle.secondary)
    async def partnership_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await create_ticket_from_button(interaction, "Partnership")


# --- Custom Checks & Start Processes ---
def is_ppp_manager():
    async def pred(ctx):
        if r_id := bot.config.get('ppp_manager_role_id'): return get(ctx.guild.roles, id=r_id) in ctx.author.roles or ctx.author.guild_permissions.administrator
        return ctx.author.guild_permissions.administrator
    return commands.check(pred)

def in_strike_request_channel():
    async def pred(ctx):
        return ctx.channel.id == bot.config.get('strike_request_channel_id')
    return commands.check(pred)

async def start_game_process(bot, players, queue_info):
    guild = players[0].guild; game_id = None
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("INSERT INTO games (game_type) VALUES (%s)", (queue_info['name'],)); game_id = cursor.lastrowid
    if not game_id: return
    text_cat = get(guild.categories, id=bot.config.get('game_text_category_id')); voice_cat = get(guild.categories, id=bot.config.get('game_voice_category_id'))
    if not text_cat or not voice_cat: return
    overwrites = {guild.default_role: discord.PermissionOverwrite(read_messages=False)}
    for p in players: overwrites[p] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
    text_ch = await text_cat.create_text_channel(f"game-{game_id}"); voice_ch = await voice_cat.create_voice_channel(f"Game {game_id}")
    for p in players:
        try: await p.move_to(voice_ch, reason="Game started")
        except: pass
    manager = GameManager(bot, players, queue_info, text_ch, voice_ch, game_id)
    bot.active_games[text_ch.id] = manager
    await manager.setup_teams()

def is_staff():
    async def pred(ctx):
        if r_id := bot.config.get('staff_role_id'): return get(ctx.guild.roles, id=r_id) in ctx.author.roles or ctx.author.guild_permissions.administrator
        return ctx.author.guild_permissions.administrator
    return commands.check(pred)

def is_admin():
    async def pred(ctx):
        if r_id := bot.config.get('admin_role_id'): return get(ctx.guild.roles, id=r_id) in ctx.author.roles or ctx.author.guild_permissions.administrator
        return ctx.author.guild_permissions.administrator
    return commands.check(pred)

# --- Background Tasks & Events ---
@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user.name}')
    try:
        bot.db_pool = await aiomysql.create_pool(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, autocommit=True)
        logger.info("DB connected.")
        await fetch_config()
    except Exception as e:
        logger.error(f"DB connection failed: {e}"); await bot.close(); return
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name="asrbw.fun"))
    
    guild_obj = get(bot.guilds, id=bot.config.get('guild_id'))
    if guild_obj:
        bot.add_view(MainTicketView())
        bot.tree.copy_global_to(guild=guild_obj)
        await bot.tree.sync(guild=guild_obj)
        logger.info(f"Synced slash commands for guild: {guild_obj.name}")

    check_moderation_expirations.start(); check_ss_expirations.start(); check_elo_decay.start(); check_strike_polls.start()
    logger.info("Bot ready, tasks started.")

@bot.event
async def on_member_update(before, after):
    staff_roles_ids = {bot.config.get('mod_role_id'), bot.config.get('admin_role_id'), bot.config.get('manager_role_id')}
    staff_roles_ids.discard(None)
    if not staff_roles_ids: return
    before_roles = set(before.roles); after_roles = set(after.roles)
    if before_roles == after_roles: return
    guild_roles = {role.id: role for role in after.guild.roles}
    staff_roles = {guild_roles.get(rid) for rid in staff_roles_ids if rid in guild_roles}
    changed_staff_role = (after_roles - before_roles) & staff_roles or (before_roles - after_roles) & staff_roles
    if not changed_staff_role: return
    channel = get(after.guild.channels, id=bot.config.get('staff_updates_channel_id'))
    if not channel: return
    action = "updated"
    if added_roles := after_roles - before_roles:
        action = "promoted" if before_roles & staff_roles else "welcomed to the staff team"
    elif removed_roles := before_roles - after_roles:
        action = "demoted" if after_roles & staff_roles else "has left the staff team"
    await channel.send(embed=create_embed("Staff Update", f"{after.mention}'s roles have been {action}.", discord.Color.blue()))

@tasks.loop(minutes=1)
async def check_strike_polls():
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
                await asyncio.sleep(60) # Keep channel for a minute to see result
                await channel.delete(reason="Strike poll ended.")

@tasks.loop(hours=24)
async def check_elo_decay():
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
                await update_elo_roles(member)

@tasks.loop(seconds=60)
async def check_moderation_expirations():
    await bot.wait_until_ready(); guild = bot.get_guild(bot.config.get('guild_id'));
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
async def check_ss_expirations():
    await bot.wait_until_ready(); guild = bot.get_guild(bot.config.get('guild_id'));
    if not guild: return
    ten_minutes_ago = datetime.utcnow() - timedelta(minutes=10)
    for channel_id in list(bot.active_ss_tickets.keys()):
        ticket_info = bot.active_ss_tickets.get(channel_id)
        if not ticket_info or ticket_info['created_at'] > ten_minutes_ago: continue
        channel = guild.get_channel(channel_id)
        if not channel: bot.active_ss_tickets.pop(channel_id, None); continue
        class DummyInteraction:
            def __init__(self, channel, guild, message): self.channel, self.guild, self.message = channel, guild, message
        message = await channel.fetch_message(ticket_info['message_id'])
        await SSTicketView().handle_ticket_close(DummyInteraction(channel, guild, message), accepted=None)

@bot.event
async def on_message(message):
    if message.author.bot: return
    if message.content.startswith(bot.command_prefix):
        emoji = bot.config.get('processing_emoji', '✅')
        try:
            await message.add_reaction(emoji)
        except (discord.HTTPException, discord.Forbidden):
            pass # Ignore if emoji is invalid or permissions are missing
    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    emoji = bot.config.get('processing_emoji', '✅')
    if ctx.prefix == bot.command_prefix and ctx.message:
        try: await ctx.message.remove_reaction(emoji, bot.user)
        except discord.Forbidden: pass
    if isinstance(error, commands.CommandNotFound): return
    elif isinstance(error, commands.CheckFailure): await ctx.send(embed=create_embed("Permission Denied", "You do not have the required permissions for this command.", discord.Color.red()), ephemeral=True)
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send(embed=create_embed("Missing Argument", f"You're missing: `{error.param.name}`.", discord.Color.orange()), ephemeral=True)
    elif isinstance(error, commands.CommandError) and "is a required argument that is missing" in str(error): await ctx.send(embed=create_embed("Missing Attachment", "You must attach an image as proof.", discord.Color.orange()), ephemeral=True)
    else: logger.error(f"Error in command '{ctx.command}': {error}"); await ctx.send(embed=create_embed("Error", "An unexpected error occurred.", discord.Color.dark_red()), ephemeral=True)

@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot: return
    if not before.channel and after.channel:
        if after.channel.id in bot.queues_in_progress: return
        queue_channels = {
            bot.config.get('queue_3v3_id'): {'size': 6, 'name': '3v3'},
            bot.config.get('queue_4v4_id'): {'size': 8, 'name': '4v4'},
            bot.config.get('queue_3v3_pups_id'): {'size': 6, 'name': '3v3 PUPS+'},
            bot.config.get('queue_4v4_pups_id'): {'size': 8, 'name': '4v4 PUPS+'},
        }
        if after.channel.id not in queue_channels: return
        queue_info = queue_channels[after.channel.id]
        if len(after.channel.members) >= queue_info['size']:
            bot.queues_in_progress.add(after.channel.id)
            players_for_game = after.channel.members[:queue_info['size']]
            await after.channel.send(embed=create_embed("Queue Full!", f"Creating a {queue_info['name']} game for {', '.join([p.mention for p in players_for_game])}..."))
            await start_game_process(bot, players_for_game, queue_info)
            bot.queues_in_progress.remove(after.channel.id)
    elif before.channel and not after.channel:
        if before.channel.id in bot.active_games:
            manager = bot.active_games[before.channel.id]
            if manager.state == "picking":
                await manager.text_channel.send(embed=create_embed("Game Aborted", f"{member.mention} left during team selection. The game has been cancelled.", discord.Color.red()))
                await manager.text_channel.delete(); await manager.voice_channel.delete(); del bot.active_games[before.channel.id]

@bot.event
async def on_raw_reaction_add(payload):
    if payload.user_id == bot.user.id: return
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT poll_id FROM ppp_polls WHERE message_id = %s AND is_open = TRUE", (payload.message_id,))
            poll_res = await cursor.fetchone()
            if poll_res:
                guild = bot.get_guild(payload.guild_id)
                voter = get(guild.members, id=payload.user_id)
                pups_role = get(guild.roles, id=bot.config.get('pups_role_id'))
                pugs_role = get(guild.roles, id=bot.config.get('pugs_role_id'))
                premium_role = get(guild.roles, id=bot.config.get('premium_role_id'))
                required_roles = [r for r in [pups_role, pugs_role, premium_role] if r is not None]
                if not voter or not any(role in voter.roles for role in required_roles): return
                vote_type = 'upvote' if str(payload.emoji) == '👍' else 'downvote'
                await cursor.execute("DELETE FROM ppp_poll_votes WHERE poll_id = %s AND voter_id = %s", (poll_res[0], payload.user_id))
                await cursor.execute("INSERT INTO ppp_poll_votes (poll_id, voter_id, vote_type) VALUES (%s, %s, %s)", (poll_res[0], payload.user_id, vote_type))
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM strike_polls WHERE message_id = %s AND is_active = TRUE", (payload.message_id,))
            if not await cursor.fetchone(): return
    channel = bot.get_channel(payload.channel_id); message = await channel.fetch_message(payload.message_id)
    if str(payload.emoji) not in ["👍", "👎"]: return
    for reaction in message.reactions:
        if reaction.emoji in ["👍", "👎"] and str(reaction.emoji) != str(payload.emoji):
            if user := bot.get_user(payload.user_id): await reaction.remove(user)

# --- ALL COMMANDS ---
@bot.hybrid_command(name="register", description="Register your Minecraft account.")
@discord.app_commands.describe(ign="Your in-game name.")
async def register(ctx, ign: str):
    code = ''.join(random.choices(string.ascii_uppercase+string.digits, k=6))
    try:
        async with bot.db_pool.acquire() as conn:
            async with conn.cursor() as cursor: await cursor.execute("INSERT INTO players (discord_id, minecraft_ign, registration_code) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE minecraft_ign=VALUES(minecraft_ign), registration_code=VALUES(registration_code)", (ctx.author.id, ign, code))
        await ctx.author.send(embed=create_embed("Registration", f"Log in to `play.asrbw.fun` and type `/link {code}`"))
        await ctx.send("DM sent with instructions.", ephemeral=True)
    except discord.Forbidden: await ctx.send("Could not DM you. Please enable DMs from server members.", ephemeral=True)

@bot.hybrid_command(name="forceregister", description="Manually register a user.")
@is_staff()
async def force_register(ctx, member: discord.Member, ign: str):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("INSERT INTO players (discord_id, minecraft_ign, elo) VALUES (%s, %s, 0) ON DUPLICATE KEY UPDATE minecraft_ign=VALUES(minecraft_ign)", (member.id, ign))
    if role_id := bot.config.get('registered_role_id'): await member.add_roles(get(ctx.guild.roles, id=role_id))
    await update_elo_roles(member)
    await ctx.send(embed=create_embed("Registered", f"{member.mention} is now `{ign}`."))

async def issue_moderation(ctx: commands.Context, member: discord.Member, action: str, role: discord.Role, reason: str, duration: Optional[timedelta] = None):
    log_channel = None
    if action == "ban": log_channel = get(ctx.guild.channels, id=bot.config.get('ban_log_channel_id'))
    elif action == "mute": log_channel = get(ctx.guild.channels, id=bot.config.get('mute_log_channel_id'))

    if role in member.roles: 
        await ctx.send(embed=create_embed(f"Already {action}ed", f"{member.mention} already has the {role.name} role.", discord.Color.orange()), ephemeral=True)
        return
    await member.add_roles(role, reason=reason)
    expires_at = datetime.utcnow() + duration if duration else None
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("INSERT INTO moderation_logs (target_id, moderator_id, action_type, reason, expires_at) VALUES (%s, %s, %s, %s, %s)", (member.id, ctx.author.id, action, reason, expires_at))
    duration_str = f" for {str(duration)}" if duration else " permanently"
    embed = create_embed(f"User {action.capitalize()}ed", f"**Member:** {member.mention}\n**Action:** {action.capitalize()}{duration_str}", discord.Color.red())
    embed.add_field(name="Reason", value=reason, inline=False).set_footer(text=f"Moderator: {ctx.author.display_name}")
    await ctx.send(embed=embed);
    if log_channel: await log_channel.send(embed=embed)

@bot.hybrid_command(name="ban", description="Ban a user.")
@is_staff()
async def ban(ctx, member: discord.Member, duration: str, *, reason: str):
    role = get(ctx.guild.roles, id=bot.config.get('banned_role_id'))
    if not role: return await ctx.send(embed=create_embed("Error", "Banned role not configured.", discord.Color.red()), ephemeral=True)
    try: await issue_moderation(ctx, member, "ban", role, reason, parse_duration(duration))
    except ValueError as e: await ctx.send(embed=create_embed("Invalid Duration", str(e), discord.Color.red()), ephemeral=True)

@bot.hybrid_command(name="mute", description="Mute a user.")
@is_staff()
async def mute(ctx, member: discord.Member, duration: str, *, reason: str):
    role = get(ctx.guild.roles, id=bot.config.get('muted_role_id'))
    if not role: return await ctx.send(embed=create_embed("Error", "Muted role not configured.", discord.Color.red()), ephemeral=True)
    try: await issue_moderation(ctx, member, "mute", role, reason, parse_duration(duration))
    except ValueError as e: await ctx.send(embed=create_embed("Invalid Duration", str(e), discord.Color.red()), ephemeral=True)

async def remove_moderation(ctx, member: discord.Member, action: str, role: discord.Role, reason: str):
    log_channel = None
    if action == "ban": log_channel = get(ctx.guild.channels, id=bot.config.get('ban_log_channel_id'))
    elif action == "mute": log_channel = get(ctx.guild.channels, id=bot.config.get('mute_log_channel_id'))

    if role not in member.roles: await ctx.send(embed=create_embed(f"Not {action}ed", f"{member.mention} does not have the {role.name} role.", discord.Color.orange()), ephemeral=True); return
    await member.remove_roles(role, reason=reason)
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Set the original punishment to inactive
            await cursor.execute("UPDATE moderation_logs SET is_active = FALSE WHERE target_id = %s AND action_type = %s AND is_active = TRUE", (member.id, action))
            # Create a new log entry for the un-action
            un_action = f"un{action}"
            await cursor.execute("INSERT INTO moderation_logs (target_id, moderator_id, action_type, reason) VALUES (%s, %s, %s, %s)", (member.id, ctx.author.id, un_action, reason))

    embed = create_embed(f"User Un{action}ed", f"{member.mention} has been un{action}ed.", discord.Color.green())
    embed.add_field(name="Reason", value=reason, inline=False)
    await ctx.send(embed=embed);
    if log_channel: await log_channel.send(embed=embed)

@bot.hybrid_command(name="unban", description="Unban a user.")
@is_staff()
async def unban(ctx, member: discord.Member, *, reason: str = "No reason provided."):
    await remove_moderation(ctx, member, "ban", get(ctx.guild.roles, id=bot.config.get('banned_role_id')), reason)

@bot.hybrid_command(name="unmute", description="Unmute a user.")
@is_staff()
async def unmute(ctx, member: discord.Member, *, reason: str = "No reason provided."):
    await remove_moderation(ctx, member, "mute", get(ctx.guild.roles, id=bot.config.get('muted_role_id')), reason)

async def strike_user_internal(guild, member: discord.Member, reason: str, moderator: Union[discord.Member, str]):
    mod_name = moderator.name if isinstance(moderator, discord.Member) else moderator
    log_channel = get(guild.channels, id=bot.config.get('strike_log_channel_id'))
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            mod_id = moderator.id if isinstance(moderator, discord.Member) else bot.user.id
            await cursor.execute("INSERT INTO moderation_logs (target_id, moderator_id, action_type, reason) VALUES (%s, %s, 'strike', %s)", (member.id, mod_id, reason))
            await cursor.execute("UPDATE players SET elo = elo - 40 WHERE discord_id = %s", (member.id,)); strike_id = cursor.lastrowid
    embed = create_embed("User Striked", f"**Member:** {member.mention}\n**Action:** Strike\n**ELO Change:** -40", discord.Color.red())
    embed.add_field(name="Reason", value=reason).set_footer(text=f"Striked by {mod_name} | Strike ID: {strike_id}")
    if log_channel: await log_channel.send(embed=embed)
    await update_elo_roles(member)

@bot.hybrid_command(name="strike", description="Issue a strike to a user.")
@is_staff()
async def strike(ctx, member: discord.Member, *, reason: str):
    await strike_user_internal(ctx.guild, member, reason, ctx.author)
    await ctx.send(f"{member.mention} has been striked.", ephemeral=True)

@bot.hybrid_command(aliases=["srem"], name="strikeremove", description="Remove a strike by its ID.")
@is_staff()
async def strikeremove(ctx, strike_id: int, *, reason: str):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT target_id FROM moderation_logs WHERE log_id = %s AND action_type = 'strike'", (strike_id,))
            res = await cursor.fetchone()
            if not res: return await ctx.send(embed=create_embed("Error", "Strike ID not found.", discord.Color.red()), ephemeral=True)
            target_id = res[0]
            await cursor.execute("DELETE FROM moderation_logs WHERE log_id = %s", (strike_id,))
            await cursor.execute("UPDATE players SET elo = elo + 40 WHERE discord_id = %s", (target_id,))
    await ctx.send(embed=create_embed("Strike Removed", f"Strike ID `{strike_id}` has been removed. Reason: {reason}", discord.Color.green()))
    await update_elo_roles(get(ctx.guild.members, id=target_id))

@bot.hybrid_command(aliases=["sr"], name="strikerequest", description="Request a community vote to strike a user.")
@in_strike_request_channel()
@discord.app_commands.describe(reason="Reason for the strike request.", proof="Image proof.")
async def strikerequest(ctx, member: discord.Member, reason: str, proof: discord.Attachment):
    if not proof: return await ctx.send("You must attach an image as proof.", ephemeral=True)
    category = get(ctx.guild.categories, id=bot.config.get('strike_request_category_id'))
    if not category: return await ctx.send("Strike request category not configured.", ephemeral=True)

    poll_channel = await category.create_text_channel(f"strike-poll-{member.name}")
    embed = create_embed(f"Strike Request against {member.display_name}", f"**Reason:** {reason}\n\nRequested by: {ctx.author.mention}")
    embed.set_image(url=proof.url).set_footer(text="Voting ends in 60 seconds.")
    msg = await poll_channel.send(embed=embed); await msg.add_reaction("👍"); await msg.add_reaction("👎")
    
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            ends_at = datetime.utcnow() + timedelta(seconds=60)
            await cursor.execute("INSERT INTO strike_polls (message_id, channel_id, target_id, requester_id, reason, ends_at) VALUES (%s, %s, %s, %s, %s, %s)", (msg.id, poll_channel.id, member.id, ctx.author.id, reason, ends_at))
    await ctx.send(f"Strike request created in {poll_channel.mention}", ephemeral=True)

@bot.hybrid_command(aliases=['i'], name="info", description="View a player's stats card.")
async def info(ctx, member: Optional[discord.Member] = None):
    member = member or ctx.author
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("SELECT minecraft_ign, elo, wins, losses, mvps, win_streak FROM players WHERE discord_id = %s", (member.id,)); data = await cursor.fetchone()
    if not data: return await ctx.send(embed=create_embed("Not Registered", f"{member.mention} is not registered.", discord.Color.orange()))
    ign, elo, wins, losses, mvps, streak = data; wlr = round(wins / losses, 2) if losses > 0 else wins
    
    skin_data = None
    async with aiohttp.ClientSession() as session:
        # Try to get UUID from Mojang API first
        uuid = None
        try:
            async with session.get(f'https://api.mojang.com/users/profiles/minecraft/{ign}') as resp:
                if resp.status == 200:
                    uuid = (await resp.json())['id']
        except Exception as e:
            logger.warning(f"Could not get UUID for {ign} from Mojang API: {e}")

        # Use Visage API with UUID, fallback to Steve if UUID fails
        render_url = f"https://visage.surgeplay.com/full/832/{uuid}" if uuid else "https://visage.surgeplay.com/full/832/8667ba71-b85a-4004-af54-457a9734eed7"
        
        async with session.get(render_url) as resp:
            if resp.status == 200:
                skin_data = await resp.read()
            else:
                logger.error(f"Failed to get skin from Visage for UUID {uuid}, status: {resp.status}")

    card = Image.new('RGB', (600, 450), color='#111111')
    
    # Create the glass panel
    glass = Image.new('RGBA', (560, 410))
    draw_glass = ImageDraw.Draw(glass)
    draw_glass.rectangle((0,0,560,410), fill=(20,20,20,180)) # semi-transparent black
    card.paste(glass, (20,20), glass)
    
    draw = ImageDraw.Draw(card)
    
    if skin_data:
        skin = Image.open(io.BytesIO(skin_data)).resize((180, 420), Image.Resampling.LANCZOS)
        card.paste(skin, (35, 15), skin)

    try:
        title_f = ImageFont.truetype("Poppins-Bold.ttf", 48)
        stat_f = ImageFont.truetype("Poppins-Bold.ttf", 32)
        label_f = ImageFont.truetype("Poppins-Regular.ttf", 26)
        footer_f = ImageFont.truetype("Poppins-Bold.ttf", 20)
    except IOError:
        logger.warning("Poppins font not found. Falling back to default font.")
        title_f=ImageFont.load_default(); stat_f=ImageFont.load_default(); label_f=ImageFont.load_default(); footer_f = ImageFont.load_default()
    
    draw.text((240, 45), ign, fill='white', font=title_f)
    draw.line([(240, 95), (560, 95)], fill='#99AAB5', width=1)
    stats = {"ELO": elo, "Wins": wins, "Losses": losses, "W/L Ratio": wlr, "MVPs": mvps, "Streak": streak}; y = 125
    for label, value in stats.items():
        draw.text((260, y), label, fill='#bbbbbb', font=label_f)
        draw.text((540, y-2), str(value), fill='white', font=stat_f, anchor="ra")
        y += 45
    
    draw.text((570, 420), ".gg/asianrbw", fill='#72767d', font=footer_f, anchor="rs")
    
    buffer = io.BytesIO(); card.save(buffer, 'PNG'); buffer.seek(0)
    await ctx.send(file=discord.File(buffer, f"{ign}_stats.png"))


# --- ALL OTHER COMMANDS AND GROUPS (history, lb, admin, stats, scoring, tickets, polls) ARE UNCHANGED ---
# ... (The rest of the file is identical to the previous version)

# --- Run ---
if __name__ == "__main__":
    if DISCORD_TOKEN:
        bot.run(DISCORD_TOKEN, log_handler=None)
    else:
        logger.error("ERROR: DISCORD_TOKEN not found in .env file.")
