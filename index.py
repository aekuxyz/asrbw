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
from PIL import Image, ImageDraw, ImageFont, ImageOps
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

# Bot Init - Using Hybrid Commands requires a command_prefix, even if you mainly use slash commands.
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

async def fetch_config():
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT setting_key, setting_value FROM config")
            records = await cursor.fetchall()
            for row in records:
                try: bot.config[row[0]] = int(row[1])
                except (ValueError, TypeError): bot.config[row[0]] = row[1]
    logger.info("Configuration loaded from database.")

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
    four_days_ago = datetime.utcnow() - timedelta(days=4)
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT discord_id, elo FROM players WHERE elo >= %s AND (last_game_played_at IS NULL OR last_game_played_at < %s)", (ELO_CONFIG['Topaz']['range'][0], four_days_ago))
            inactive_players = await cursor.fetchall()
            for player_id, current_elo in inactive_players:
                decayed_elo = max(ELO_CONFIG['Topaz']['range'][0], current_elo - 60)
                await cursor.execute("UPDATE players SET elo = %s WHERE discord_id = %s", (decayed_elo, player_id))

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
        await message.add_reaction(emoji)
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
            await cursor.execute("SELECT elo FROM players WHERE discord_id = %s", (member.id,))
            current_elo = (await cursor.fetchone() or [0])[0]
    try:
        await member.edit(nick=f"[{current_elo}] {ign}")
        if role_id := bot.config.get('registered_role_id'): await member.add_roles(get(ctx.guild.roles, id=role_id))
    except Exception: pass
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
            await cursor.execute("UPDATE moderation_logs SET is_active = FALSE WHERE target_id = %s AND action_type = %s AND is_active = TRUE", (member.id, action))
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
            await cursor.execute("DELETE FROM moderation_logs WHERE log_id = %s", (strike_id,))
            await cursor.execute("UPDATE players SET elo = elo + 40 WHERE discord_id = %s", (res[0],))
    await ctx.send(embed=create_embed("Strike Removed", f"Strike ID `{strike_id}` has been removed. Reason: {reason}", discord.Color.green()))

@bot.hybrid_command(aliases=["sr"], name="strikerequest", description="Request a community vote to strike a user.")
@in_strike_request_channel()
@discord.app_commands.describe(reason="Reason for the strike request.", proof="Image proof.")
async def strikerequest(ctx, member: discord.Member, reason: str, proof: discord.Attachment):
    if not proof: return await ctx.send("You must attach an image as proof.", ephemeral=True)
    category = get(ctx.guild.categories, id=bot.config.get('strike_request_category_id'))
    if not category: return await ctx.send("Strike request category not configured.", ephemeral=True)

    poll_channel = await category.create_text_channel(f"strike-poll-{member.name}")
    embed = create_embed(f"Strike Request against {member.display_name}", f"**Reason:** {reason}\n\nRequested by: {ctx.author.mention}")
    embed.set_image(url=proof.url).set_footer(text="Voting ends in 60 minutes.")
    msg = await poll_channel.send(embed=embed); await msg.add_reaction("👍"); await msg.add_reaction("👎")
    
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            ends_at = datetime.utcnow() + timedelta(minutes=60)
            await cursor.execute("INSERT INTO strike_polls (message_id, channel_id, target_id, requester_id, reason, ends_at) VALUES (%s, %s, %s, %s, %s, %s)", (msg.id, poll_channel.id, member.id, ctx.author.id, reason, ends_at))
    await ctx.send(f"Strike request created in {poll_channel.mention}", ephemeral=True)

@bot.hybrid_command(aliases=['i'], name="info", description="View a player's stats card.")
async def info(ctx, member: Optional[discord.Member] = None):
    member = member or ctx.author
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("SELECT minecraft_ign, elo, wins, losses, mvps, win_streak FROM players WHERE discord_id = %s", (member.id,)); data = await cursor.fetchone()
    if not data: return await ctx.send(embed=create_embed("Not Registered", f"{member.mention} is not registered.", discord.Color.orange()))
    ign, elo, wins, losses, mvps, streak = data; wlr = round(wins / losses, 2) if losses > 0 else wins
    async with aiohttp.ClientSession() as s:
        async with s.get(f'https://crafatar.com/renders/body/{ign}?overlay') as r:
            if r.status != 200: return await ctx.send("Could not fetch skin."); skin_data = await r.read()
    skin = Image.open(io.BytesIO(skin_data)).resize((180, 420), Image.Resampling.LANCZOS)
    card = Image.new('RGB', (600, 450), color='#2F3136'); card.paste(skin, (20, 15), skin)
    draw = ImageDraw.Draw(card); font_path = "arial.ttf"
    try: title_f = ImageFont.truetype(font_path, 40); stat_f = ImageFont.truetype(font_path, 28); label_f = ImageFont.truetype(font_path, 22)
    except IOError: title_f=ImageFont.load_default(); stat_f=ImageFont.load_default(); label_f=ImageFont.load_default()
    draw.text((230, 30), ign, fill='white', font=title_f); draw.line([(230, 80), (560, 80)], fill='#99AAB5', width=2)
    stats = {"ELO": elo, "Wins": wins, "Losses": losses, "W/L Ratio": wlr, "MVPs": mvps, "Streak": streak}; y = 110
    for label, value in stats.items(): draw.text((250, y), label, fill='#99AAB5', font=label_f); draw.text((450, y-4), str(value), fill='white', font=stat_f, anchor="ma"); y += 50
    buffer = io.BytesIO(); card.save(buffer, 'PNG'); buffer.seek(0)
    await ctx.send(file=discord.File(buffer, f"{ign}_stats.png"))

@bot.hybrid_command(aliases=['h'], name="history", description="View a user's moderation history.")
@is_staff()
async def history(ctx, member: discord.Member):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT log_id, moderator_id, action_type, reason, created_at, expires_at, is_active FROM moderation_logs WHERE target_id = %s ORDER BY created_at DESC", (member.id,))
            logs = await cursor.fetchall()
    if not logs: return await ctx.send(embed=create_embed("Clean Record", f"{member.mention} has no moderation history."))
    embeds = []
    items_per_page = 5
    for i in range(0, len(logs), items_per_page):
        page_logs = logs[i:i+items_per_page]
        embed = create_embed(f"History for {member.display_name}", f"Page {i//items_per_page + 1}/{math.ceil(len(logs)/items_per_page)}")
        for log_id, mod_id, type, reason, ts, exp, active in page_logs:
            mod = get(ctx.guild.members, id=mod_id) or "Unknown"; status = "Active" if active else "Inactive/Expired"
            field_val = f"**Reason:** {reason or 'None'}\n**Moderator:** {mod.mention}\n**Status:** {status}"
            embed.add_field(name=f"#{log_id} - {type.capitalize()}", value=field_val, inline=False)
        embeds.append(embed)
    await ctx.send(embed=embeds[0], view=PaginatorView(embeds))

@bot.hybrid_command(aliases=['lb'], name="leaderboard", description="View leaderboards for different stats.")
@discord.app_commands.describe(category="The leaderboard category to view.")
async def leaderboard(ctx, category: str = "elo"):
    valid_cats = {'elo': 'elo', 'wins': 'wins', 'losses': 'losses', 'mvps': 'mvps', 'streak': 'win_streak'}
    if category.lower() not in valid_cats: return await ctx.send(f"Invalid category. Use: {', '.join(valid_cats.keys())}")
    db_col = valid_cats[category.lower()]
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"SELECT discord_id, {db_col} FROM players ORDER BY {db_col} DESC LIMIT 100")
            data = await cursor.fetchall()
    if not data: return await ctx.send("No data for this leaderboard yet.")
    embeds = []
    items_per_page = 10
    for i in range(0, len(data), items_per_page):
        page_data = data[i:i+items_per_page]
        embed = create_embed(f"Leaderboard - {category.capitalize()}", f"Page {i//items_per_page + 1}/{math.ceil(len(data)/items_per_page)}")
        description = ""
        for rank, (player_id, value) in enumerate(page_data, start=i+1):
            player = get(ctx.guild.members, id=player_id)
            description += f"`{rank}.` {player.mention if player else 'Unknown User'}: **{value}**\n"
        embed.description = description
        embeds.append(embed)
    await ctx.send(embed=embeds[0], view=PaginatorView(embeds))

@bot.hybrid_command(name="purgechat", description="Deletes a specified number of messages.")
@commands.has_permissions(manage_messages=True)
async def purge_chat(ctx, limit: int = 100):
    await ctx.defer(ephemeral=True)
    purged = await ctx.channel.purge(limit=limit)
    await ctx.send(f"Deleted {len(purged)} messages.", delete_after=5)

@bot.hybrid_group(name="admin", description="Admin-only commands.")
@is_admin()
async def admin(ctx):
    if ctx.invoked_subcommand is None: await ctx.send(embed=create_embed("Admin Commands", "Use an admin subcommand.", discord.Color.orange()))

@admin.command(name="partymode", description="Toggle party season mode.")
async def admin_partymode(ctx, state: str):
    state_val = 1 if state.lower() in ['on', '1', 'true'] else 0
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("INSERT INTO config (setting_key, setting_value) VALUES ('party_season', %s) ON DUPLICATE KEY UPDATE setting_value=VALUES(setting_value)", (state_val,))
    await fetch_config() # Reload config
    await ctx.send(f"Party Season mode has been turned **{'ON' if state_val == 1 else 'OFF'}**.")

@admin.command(name="purgeall", description="DANGER: Wipes all player stats and data.")
async def admin_purgeall(ctx):
    await ctx.send("This is a dangerous command. Type `CONFIRM` to proceed.");
    try: await bot.wait_for('message', timeout=30.0, check=lambda m: m.author == ctx.author and m.channel == ctx.channel and m.content == 'CONFIRM')
    except asyncio.TimeoutError: return await ctx.send("Purge cancelled.")
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("DELETE FROM players")
    await ctx.send(embed=create_embed("DATABASE PURGED", "All player data has been wiped.", discord.Color.dark_red()))

@admin.command(name="setpartysize", description="Set the default party size for matchmaking.")
async def admin_setpartysize(ctx, size: int):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("INSERT INTO config (setting_key, setting_value) VALUES ('party_size', %s) ON DUPLICATE KEY UPDATE setting_value=VALUES(setting_value)", (str(size),))
    await fetch_config()
    await ctx.send(f"Party size set to `{size}`.")

@bot.hybrid_command(name="wins", description="Add wins to a user.")
@is_admin()
async def wins(ctx, member: discord.Member, amount: int):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("UPDATE players SET wins = wins + %s, win_streak = win_streak + %s, elo = elo + %s WHERE discord_id = %s", (amount, amount, 20 * amount, member.id))
    await ctx.send(f"Added {amount} wins to {member.mention}.")
    
@bot.hybrid_command(name="losses", description="Add losses to a user.")
@is_admin()
async def losses(ctx, member: discord.Member, amount: int):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("UPDATE players SET losses = losses + %s, win_streak = 0, elo = elo - %s WHERE discord_id = %s", (amount, 10 * amount, member.id))
    await ctx.send(f"Added {amount} losses to {member.mention}.")

@bot.hybrid_command(name="elochange", description="Change a user's ELO by a specific amount.")
@is_admin()
async def elochange(ctx, member: discord.Member, amount: int):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("UPDATE players SET elo = elo + %s WHERE discord_id = %s", (amount, member.id))
    await ctx.send(f"Changed {member.mention}'s ELO by {amount}.")

@bot.hybrid_command(name="elo", description="Set a user's ELO to a specific value.")
@is_admin()
async def elo(ctx, member: discord.Member, amount: int):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("UPDATE players SET elo = %s WHERE discord_id = %s", (amount, member.id))
    await ctx.send(f"Set {member.mention}'s ELO to {amount}.")

@bot.hybrid_command(name="mvps", description="Add MVPs to a user.")
@is_admin()
async def mvps(ctx, member: discord.Member, amount: int):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("UPDATE players SET mvps = mvps + %s WHERE discord_id = %s", (amount, member.id))
    await ctx.send(f"Added {amount} MVPs to {member.mention}.")

async def create_results_image(team1_players, team2_players, winning_team_num, mvp):
    card = Image.new('RGB', (800, 400), color='#1a1a1a')
    draw = ImageDraw.Draw(card)
    try: font = ImageFont.truetype("arial.ttf", 24)
    except: font = ImageFont.load_default()
    t1_color = '#57F287' if winning_team_num == 1 else '#80848E'
    draw.text((150, 30), "Team 1", fill=t1_color, font=font, anchor="ms")
    for i, p in enumerate(team1_players):
        name = f"👑 {p['ign']}" if p['id'] == mvp else p['ign']
        draw.text((150, 80 + i*40), name, fill='white', font=font, anchor="ms")
    t2_color = '#57F287' if winning_team_num == 2 else '#80848E'
    draw.text((650, 30), "Team 2", fill=t2_color, font=font, anchor="ms")
    for i, p in enumerate(team2_players):
        name = f"👑 {p['ign']}" if p['id'] == mvp else p['ign']
        draw.text((650, 80 + i*40), name, fill='white', font=font, anchor="ms")
    buffer = io.BytesIO(); card.save(buffer, 'PNG'); buffer.seek(0)
    return buffer

@bot.hybrid_command(name="score", description="Score a completed game.")
@is_admin()
async def score(ctx, game_id: int, winning_team_num: int, mvp: discord.Member):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT is_scored, is_undone FROM games WHERE game_id = %s", (game_id,))
            status = await cursor.fetchone()
            if not status or status[0]: return await ctx.send("Game not found or already scored.", ephemeral=True)
            if status[1]: return await ctx.send("This game was undone. Use `=rescore` to score it again.", ephemeral=True)
            await cursor.execute("SELECT p.discord_id, p.minecraft_ign, p.elo, gp.team_id FROM players p JOIN game_participants gp ON p.discord_id = gp.player_id WHERE gp.game_id = %s", (game_id,))
            players_data = await cursor.fetchall()
            t1, t2, elo_changes = [], [], []
            for p_id, ign, elo, team_id in players_data:
                player_info = {'id': p_id, 'ign': ign, 'elo': elo, 'rank': get_rank_from_elo(elo)}
                if team_id == 1: t1.append(player_info)
                else: t2.append(player_info)
                _, rank_data = get_rank_from_elo(elo); elo_change = 0; is_winner = (team_id == winning_team_num)
                if is_winner:
                    elo_change += rank_data['win']
                    if p_id == mvp.id: elo_change += rank_data['mvp']
                    await cursor.execute("UPDATE players SET wins = wins + 1, win_streak = win_streak + 1, elo = elo + %s, last_game_played_at = NOW() WHERE discord_id = %s", (elo_change, p_id))
                else:
                    elo_change += rank_data['loss']
                    await cursor.execute("UPDATE players SET losses = losses + 1, win_streak = 0, elo = elo + %s, last_game_played_at = NOW() WHERE discord_id = %s", (elo_change, p_id))
                elo_changes.append((elo_change, game_id, p_id))
            await cursor.executemany("UPDATE game_participants SET elo_change = %s WHERE game_id = %s AND player_id = %s", elo_changes)
            await cursor.execute("UPDATE games SET is_scored = TRUE, is_undone = FALSE, winning_team = %s, mvp_discord_id = %s, scored_at = NOW(), scored_by_id = %s WHERE game_id = %s", (winning_team_num, mvp.id, ctx.author.id, game_id))
    results_channel = get(ctx.guild.channels, id=bot.config.get('games_results_channel_id'))
    if results_channel:
        img_buffer = await create_results_image(t1, t2, winning_team_num, mvp.id)
        await results_channel.send(file=discord.File(img_buffer, f"game_{game_id}_results.png"))
    await ctx.send(f"Game {game_id} has been scored.")

@bot.hybrid_command(name="undo", description="Undo a scored game, reverting all stats.")
@is_admin()
async def undo(ctx, game_id: int, *, reason: str):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT is_scored, is_undone, winning_team, mvp_discord_id FROM games WHERE game_id = %s", (game_id,))
            status = await cursor.fetchone()
            if not status or not status[0]: return await ctx.send("Game not found or was not scored.", ephemeral=True)
            if status[1]: return await ctx.send("This game has already been undone.", ephemeral=True)
            is_scored, is_undone, winning_team, mvp_id = status
            await cursor.execute("SELECT player_id, team_id, elo_change FROM game_participants WHERE game_id = %s", (game_id,))
            participants = await cursor.fetchall()
            for p_id, team_id, elo_change in participants:
                is_winner = (team_id == winning_team)
                if is_winner:
                    await cursor.execute("UPDATE players SET wins = wins - 1, win_streak = win_streak - 1, elo = elo - %s WHERE discord_id = %s", (elo_change, p_id))
                else:
                    await cursor.execute("UPDATE players SET losses = losses - 1, elo = elo - %s WHERE discord_id = %s", (elo_change, p_id))
            await cursor.execute("UPDATE games SET is_scored = FALSE, is_undone = TRUE WHERE game_id = %s", (game_id,))
    await ctx.send(embed=create_embed("Game Undone", f"Game `{game_id}` has been undone. Reason: {reason}", discord.Color.orange()))

@bot.hybrid_command(name="rescore", description="Rescore a game with a new outcome.")
@is_admin()
async def rescore(ctx, game_id: int, winning_team_num: int, mvp: discord.Member):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT is_scored FROM games WHERE game_id = %s", (game_id,))
            status = await cursor.fetchone()
            if status and status[0]: await undo.callback(ctx, game_id=game_id, reason="Rescoring")
    await score.callback(ctx, game_id=game_id, winning_team_num=winning_team_num, mvp=mvp)
    await ctx.send(f"Game `{game_id}` has been rescored.")

@bot.hybrid_command(name="ticket", description="Create a new support ticket.")
@discord.app_commands.describe(ticket_type="The type of ticket to create.")
async def ticket(ctx, ticket_type: str = None):
    valid_types = ['general', 'appeal', 'store', 'ssappeal']
    if not ticket_type or ticket_type.lower() not in valid_types:
        return await ctx.send(embed=create_embed("Error", f"Invalid type. Use: `{', '.join(valid_types)}`", discord.Color.red()), ephemeral=True)
    category = get(ctx.guild.categories, id=bot.config.get('ticket_category_id'))
    if not category: return await ctx.send(embed=create_embed("Error", "Ticket category not configured.", discord.Color.red()), ephemeral=True)
    overwrites = {ctx.guild.default_role: discord.PermissionOverwrite(read_messages=False), ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True), ctx.guild.me: discord.PermissionOverwrite(read_messages=True)}
    if role := get(ctx.guild.roles, id=bot.config.get('staff_role_id')): overwrites[role] = discord.PermissionOverwrite(read_messages=True)
    channel = await ctx.guild.create_text_channel(f"{ticket_type}-{ctx.author.name}", category=category, overwrites=overwrites)
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("INSERT INTO tickets (channel_id, creator_id, type) VALUES (%s, %s, %s)", (channel.id, ctx.author.id, ticket_type.lower()))
    embed = create_embed(f"Ticket Created", f"Welcome, {ctx.author.mention}. Staff will be with you shortly."); await channel.send(embed=embed)
    await ctx.send(f"Ticket created: {channel.mention}", ephemeral=True, delete_after=10)

async def is_ticket_channel(ctx):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor: await cursor.execute("SELECT 1 FROM tickets WHERE channel_id = %s", (ctx.channel.id,)); return await cursor.fetchone() is not None

@bot.hybrid_command(name="add", description="Add a user or role to a ticket.")
@commands.check(is_ticket_channel)
async def add_to_ticket(ctx, target: Union[discord.Member, discord.Role]): await ctx.channel.set_permissions(target, read_messages=True, send_messages=True); await ctx.send(embed=create_embed("Added", f"{target.mention} added to ticket.", discord.Color.green()))

@bot.hybrid_command(name="remove", description="Remove a user or role from a ticket.")
@commands.check(is_ticket_channel)
async def remove_from_ticket(ctx, target: Union[discord.Member, discord.Role]): await ctx.channel.set_permissions(target, overwrite=None); await ctx.send(embed=create_embed("Removed", f"{target.mention} removed from ticket.", discord.Color.orange()))

@bot.hybrid_command(name="close", description="Close the current ticket.")
async def close_ticket(ctx, *, reason: str = "No reason provided."):
    is_ss_ticket = ctx.channel.name.startswith('ss-')
    is_normal_ticket = await is_ticket_channel(ctx)
    if not is_ss_ticket and not is_normal_ticket: return await ctx.send("This is not a ticket channel.", ephemeral=True)
    await ctx.send(embed=create_embed("Ticket Closing", "This ticket will be logged and closed in 10 seconds...", discord.Color.orange()))
    transcript = await generate_html_transcript(ctx.channel)
    await asyncio.sleep(10)
    await ctx.channel.delete(reason=f"Closed by {ctx.author} for: {reason}")
    log_channel = get(ctx.guild.channels, id=bot.config.get('ticket_log_channel_id'))
    if log_channel:
        embed = create_embed("Ticket Closed", f"Ticket `#{ctx.channel.name}` was closed by {ctx.author.mention}.")
        embed.add_field(name="Reason", value=reason)
        await log_channel.send(embed=embed, file=discord.File(transcript, f"transcript-{ctx.channel.name}.html"))

@bot.hybrid_command(name="ss", description="Request a screenshare for a user.")
@discord.app_commands.describe(target="The user to screenshare.", reason="The reason for the request.", proof="Image proof of cheating.")
async def ss(ctx, target: discord.Member, reason: str, proof: discord.Attachment):
    frozen_role = get(ctx.guild.roles, id=bot.config.get('frozen_role_id'))
    ss_staff_role = get(ctx.guild.roles, id=bot.config.get('screenshare_staff_role_id'))
    category = get(ctx.guild.categories, id=bot.config.get('screenshare_category_id', bot.config.get('ticket_category_id')))
    if not all([frozen_role, ss_staff_role, category]): return await ctx.send(embed=create_embed("Error", "Screenshare system is not fully configured.", discord.Color.red()), ephemeral=True)
    if frozen_role in target.roles: return await ctx.send(embed=create_embed("Error", f"{target.mention} is already frozen.", discord.Color.orange()), ephemeral=True)
    await target.add_roles(frozen_role, reason=f"SS Request by {ctx.author}")
    overwrites = {ctx.guild.default_role: discord.PermissionOverwrite(read_messages=False), ctx.author: discord.PermissionOverwrite(read_messages=True), target: discord.PermissionOverwrite(read_messages=True), ss_staff_role: discord.PermissionOverwrite(read_messages=True)}
    channel = await category.create_text_channel(f"ss-{target.name}", overwrites=overwrites)
    embed = create_embed("Screenshare Request", f"A request has been opened by {ctx.author.mention} for {target.mention}.")
    embed.add_field(name="Reason", value=reason, inline=False).set_image(url=proof.url)
    msg = await channel.send(content=f"{ss_staff_role.mention}, a new SS request requires attention.", embed=embed, view=SSTicketView())
    bot.active_ss_tickets[channel.id] = {'message_id': msg.id, 'target_id': target.id, 'requester_id': ctx.author.id, 'created_at': datetime.utcnow()}
    await ctx.send(embed=create_embed("Request Created", f"Screenshare ticket has been created in {channel.mention}", discord.Color.green()), ephemeral=True)

@bot.hybrid_command(name="ssclose", description="Close a screenshare ticket.")
@is_staff()
async def ss_close(ctx, *, reason: str = "No reason provided."):
    await close_ticket.callback(ctx, reason=reason)

@bot.hybrid_command(name="poll", description="Start a new PPP poll for a user.")
@is_ppp_manager()
async def poll(ctx, kind: str, member: discord.Member):
    pups_role = get(ctx.guild.roles, id=bot.config.get('pups_role_id'))
    pugs_role = get(ctx.guild.roles, id=bot.config.get('pugs_role_id'))
    premium_role = get(ctx.guild.roles, id=bot.config.get('premium_role_id'))
    voting_channel = get(ctx.guild.channels, id=bot.config.get('ppp_voting_channel_id'))
    if not all([pups_role, pugs_role, premium_role, voting_channel]):
        return await ctx.send(embed=create_embed("Error", "PPP system not fully configured. Ensure pups_role_id, pugs_role_id, premium_role_id, and ppp_voting_channel_id are set.", discord.Color.red()), ephemeral=True)

    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute("INSERT INTO ppp_polls (user_id, kind) VALUES (%s, %s)", (member.id, kind))
                poll_id = cursor.lastrowid
            except aiomysql.IntegrityError:
                return await ctx.send(embed=create_embed("Error", "A poll of this kind already exists for this user.", discord.Color.red()), ephemeral=True)
    embed = create_embed(f"PPP Poll: {kind.capitalize()}", f"A poll has been started for {member.mention}.")
    embed.add_field(name="Status", value="OPEN ✅")
    msg = await voting_channel.send(embed=embed)
    await msg.add_reaction("👍"); await msg.add_reaction("👎")
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("UPDATE ppp_polls SET message_id = %s WHERE poll_id = %s", (msg.id, poll_id))
    await ctx.send(f"Poll for {member.mention} created in {voting_channel.mention}")

@bot.hybrid_command(name="pollclose", description="Close an active PPP poll.")
@is_ppp_manager()
async def pollclose(ctx, kind: str, member: discord.Member):
    voting_channel = get(ctx.guild.channels, id=bot.config.get('ppp_voting_channel_id'))
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT poll_id, message_id FROM ppp_polls WHERE user_id = %s AND kind = %s AND is_open = TRUE", (member.id, kind))
            poll_data = await cursor.fetchone()
            if not poll_data: return await ctx.send("No active poll of this kind found for the user.", ephemeral=True)
            poll_id, msg_id = poll_data
            await cursor.execute("UPDATE ppp_polls SET is_open = FALSE, closed_at = NOW() WHERE poll_id = %s", (poll_id,))
    try:
        msg = await voting_channel.fetch_message(msg_id)
        embed = msg.embeds[0]
        embed.fields[0].value = "CLOSED ❌"
        embed.color = discord.Color.red()
        upvotes = get(msg.reactions, emoji="👍").count - 1
        downvotes = get(msg.reactions, emoji="👎").count - 1
        embed.add_field(name="Final Results", value=f"👍 {upvotes} - 👎 {downvotes}")
        await msg.edit(embed=embed)
    except discord.NotFound: pass
    await ctx.send(f"Poll for {member.mention} has been closed.")

@bot.hybrid_command(name="mypoll", description="Check the status of your PPP poll.")
async def mypoll(ctx, kind: str):
    async with bot.db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT is_open, message_id FROM ppp_polls WHERE user_id = %s AND kind = %s", (ctx.author.id, kind))
            poll_data = await cursor.fetchone()
    if not poll_data: return await ctx.send("You do not have a poll of this kind.", ephemeral=True)
    is_open, msg_id = poll_data
    status = "OPEN ✅" if is_open else "CLOSED ❌"
    await ctx.send(f"Your `{kind}` poll is currently **{status}**. Link: https://discord.com/channels/{ctx.guild.id}/{bot.config.get('ppp_voting_channel_id')}/{msg_id}")

# --- Run ---
if __name__ == "__main__":
    if DISCORD_TOKEN:
        bot.run(DISCORD_TOKEN, log_handler=None)
    else:
        logger.error("ERROR: DISCORD_TOKEN not found in .env file.")
