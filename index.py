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

# --- Helper Functions ---
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

# --- UI Views ---
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
        party_season = self.bot.config.get('party_season', 0)
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
        async with self.bot.db_pool.acquire() as conn:
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
        async with self.bot.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                t1_data = [(self.game_id, p.id, 1) for p in self.team1]
                t2_data = [(self.game_id, p.id, 2) for p in self.team2]
                await cursor.executemany("INSERT INTO game_participants (game_id, player_id, team_id) VALUES (%s, %s, %s)", t1_data + t2_data)
        embed = create_embed(f"Game #{self.game_id} - Teams are Set!", f"Use `=score {self.game_id} <winning_team_num> <mvp>` to score.")
        embed.add_field(name="Team 1", value=' '.join([p.mention for p in self.team1]), inline=False)
        embed.add_field(name="Team 2", value=' '.join([p.mention for p in self.team2]), inline=False)
        if interaction: await interaction.response.edit_message(embed=embed, view=None)
        else: await self.text_channel.send(embed=embed)
        del self.bot.active_games[self.text_channel.id]

class SSTicketView(discord.ui.View):
    def __init__(self, bot_instance):
        super().__init__(timeout=None)
        self.bot = bot_instance
    async def handle_ticket_close(self, i: discord.Interaction, accepted: bool):
        for item in self.children: item.disabled = True
        await i.message.edit(view=self)
        info = self.bot.active_ss_tickets.pop(i.channel.id, None)
        if not info: return
        member = i.guild.get_member(info['target_id']); role = get(i.guild.roles, id=self.bot.config.get('frozen_role_id'))
        if not accepted:
            if member and role and role in member.roles: await member.remove_roles(role, reason="SS Request Declined/Timed Out")
            reason = "Request declined." if accepted is False else "Request timed out."
            await i.channel.send(embed=create_embed("Ticket Closed", reason, discord.Color.orange()))
            await asyncio.sleep(5); await i.channel.delete()
        else: await i.channel.send(embed=create_embed("Ticket Accepted", f"{i.user.mention} has accepted the request.", discord.Color.green()))
    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success, custom_id="ss_accept")
    async def accept(self, i: discord.Interaction, button: discord.ui.Button):
        if not get(i.guild.roles, id=self.bot.config.get('screenshare_staff_role_id')) in i.user.roles: return await i.response.send_message("No permission.", ephemeral=True)
        await self.handle_ticket_close(i, accepted=True); await i.response.defer()
    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger, custom_id="ss_decline")
    async def decline(self, i: discord.Interaction, button: discord.ui.Button):
        if not get(i.guild.roles, id=self.bot.config.get('screenshare_staff_role_id')) in i.user.roles: return await i.response.send_message("No permission.", ephemeral=True)
        await self.handle_ticket_close(i, accepted=False); await i.response.defer()

class MainTicketView(discord.ui.View):
    def __init__(self, bot_instance):
        super().__init__(timeout=None)
        self.bot = bot_instance

    async def create_ticket_from_button(self, interaction: discord.Interaction, ticket_type: str):
        await interaction.response.defer(ephemeral=True)
        category = get(interaction.guild.categories, id=self.bot.config.get('ticket_category_id'))
        staff_role = get(interaction.guild.roles, id=self.bot.config.get('staff_role_id'))
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
        async with self.bot.db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("INSERT INTO tickets (channel_id, creator_id, type) VALUES (%s, %s, %s)", (channel.id, interaction.user.id, ticket_type.lower()))
        embed = create_embed(f"Ticket Created: {ticket_type.capitalize()}", f"Welcome, {interaction.user.mention}. Staff will be with you shortly.")
        await channel.send(content=staff_role.mention, embed=embed)
        await interaction.followup.send(f"Your ticket has been created: {channel.mention}", ephemeral=True)

    @discord.ui.button(label="General", emoji="☑️", custom_id="ticket_general", style=discord.ButtonStyle.secondary)
    async def general_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.create_ticket_from_button(interaction, "General")
        
    @discord.ui.button(label="Appeal", emoji="⚖️", custom_id="ticket_appeal", style=discord.ButtonStyle.secondary)
    async def appeal_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.create_ticket_from_button(interaction, "Appeal")

    @discord.ui.button(label="Store", emoji="🛒", custom_id="ticket_store", style=discord.ButtonStyle.secondary)
    async def store_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.create_ticket_from_button(interaction, "Store")

    @discord.ui.button(label="Partnership", emoji="🤝", custom_id="ticket_partnership", style=discord.ButtonStyle.secondary)
    async def partnership_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.create_ticket_from_button(interaction, "Partnership")


# --- Custom Checks ---
class NotInRegisterChannel(commands.CheckFailure):
    pass

def is_ppp_manager():
    async def pred(ctx):
        if r_id := bot.config.get('ppp_manager_role_id'): return get(ctx.guild.roles, id=r_id) in ctx.author.roles or ctx.author.guild_permissions.administrator
        return ctx.author.guild_permissions.administrator
    return commands.check(pred)

def in_strike_request_channel():
    async def pred(ctx):
        if ctx.channel.id != bot.config.get('strike_request_channel_id'):
            await ctx.send(f"This command can only be used in <#{bot.config.get('strike_request_channel_id')}>.", ephemeral=True)
            return False
        return True
    return commands.check(pred)

def in_register_channel():
    async def pred(ctx):
        if ctx.channel.id != bot.config.get('register_channel_id'):
            raise NotInRegisterChannel()
        return True
    return commands.check(pred)

def is_privileged_for_nick():
    async def pred(ctx):
        ss_staff_role_id = bot.config.get('screenshare_staff_role_id')
        staff_role_id = bot.config.get('staff_role_id')
        
        is_staff_member = False
        if staff_role_id and get(ctx.guild.roles, id=staff_role_id) in ctx.author.roles:
            is_staff_member = True
        
        is_ss_staff = False
        if ss_staff_role_id and get(ctx.guild.roles, id=ss_staff_role_id) in ctx.author.roles:
            is_ss_staff = True

        return is_staff_member or is_ss_staff or ctx.author.premium_since is not None
    return commands.check(pred)


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

# --- The rest of the file remains unchanged. ---

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
