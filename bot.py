import discord
from discord.ext import commands, tasks
from discord import app_commands
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import asyncio
import time
import json
import os
import logging
import re
import sys
import requests # Requires: pip install requests
from datetime import datetime, timedelta
import pytz
from dateutil import parser
from dotenv import load_dotenv
from collections import defaultdict

# --- CONFIGURATION ---
UPDATE_URL = "https://raw.githubusercontent.com/effionx/jeffbot/refs/heads/main/bot.py"
BOT_VERSION = "v0.07"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot_debug.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
SHEET_NAME = os.getenv('SHEET_NAME')
PINNED_CHANNEL_ID = int(os.getenv('PINNED_CHANNEL_ID'))
LOG_CHANNEL_ID = 1442000828901883986
DEMO_FORUM_ID = 1441988043404869693 

# --- CONSTANTS ---
TAB_DASHBOARD = "Dashboard"      
TAB_DISCORD = "DISCORD UPDATES"
TAB_FORM = "FORM UPDATES"
TAB_OLD = "OLD DATA"
GB_TZ = pytz.timezone('Europe/London')
STATE_FILE = 'bot_state.json'
START_TIME = int(time.time())

HEADER_FIN = "**🏦 JEFF BANK**"
HEADER_TIMER = "**📊 STATUS BOARD**"

PLAYER_IDS = [109217807555649536, 147518750562713600]
PLAYER_MAP = {
    109217807555649536: "Effion",
    147518750562713600: "Jero"
}

STANDARD_DEFAULTS = {
    "cows": "8h45m",
    "lnorth": "3d4h50m",
    "lsouth": "1d12h",
    "rice": "4h",
    "pigs": "6h"
}
INSTANCED_COMMANDS = ["seedbed", "kq"]

# --- BOT SETUP ---
intents = discord.Intents.default()
intents.message_content = True 
intents.reactions = True
intents.members = True 

class MyBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents, help_command=None)
    async def setup_hook(self): pass

bot = MyBot()

# --- HELPERS ---
def get_gb_time(): return datetime.now(GB_TZ)

def parse_duration_string(time_str):
    regex = re.compile(r'((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?')
    parts = regex.match(time_str)
    if not parts: return None
    data = parts.groupdict()
    params = {name: int(param) for name, param in data.items() if param}
    if not params: return None
    return timedelta(**params)

def parse_sheet_timestamp(ts_str):
    try:
        dt = parser.parse(ts_str)
        return GB_TZ.localize(dt) if dt.tzinfo is None else dt.astimezone(GB_TZ)
    except: return None

def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
        return gspread.authorize(creds)
    except: return None

async def log_to_channel(title, description, color=discord.Color.light_grey):
    channel = bot.get_channel(LOG_CHANNEL_ID)
    if not channel: return
    try:
        embed = discord.Embed(title=title, description=description, color=color, timestamp=datetime.now())
        await channel.send(embed=embed)
    except Exception as e: logger.error(f"Failed to log: {e}")

# --- STATE MANAGEMENT ---
def load_state():
    defaults = {
        "timers": {}, "custom_cmds": {}, "standard_overrides": {}, 
        "motd": "", "last_motd_date": "", "last_form_row": 1, "vacation": [] 
    }
    if not os.path.exists(STATE_FILE): return defaults
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            for k, v in defaults.items(): 
                if k not in data: data[k] = v
            return data
    except Exception as e:
        logger.error(f"State corrupted: {e}")
        return defaults

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def get_ping_string():
    state = load_state()
    vacationers = state.get("vacation", [])
    pings = []
    for uid in PLAYER_IDS:
        if uid not in vacationers: pings.append(f"<@{uid}>")
    return " ".join(pings) if pings else "*(No active users)*"

# --- FINANCIAL LOGIC ---
def get_financial_detailed():
    client = get_gspread_client()
    if not client: return None
    stats = {
        "gbank_val": "Error", "today": {"in": 0, "out": 0, "net": 0},
        "week": {"in": 0, "out": 0, "net": 0}, "month": {"in": 0, "out": 0, "net": 0},
        "top_categories": "None", "breakdown": defaultdict(int), "last_5": []     
    }
    try:
        wb = client.open(SHEET_NAME)
        try: stats["gbank_val"] = wb.worksheet(TAB_DASHBOARD).acell('B2').value
        except: pass
        now = get_gb_time()
        today_date = now.date()
        start_week = (now - timedelta(days=now.weekday())).date()
        start_month = now.replace(day=1).date()
        all_entries = []
        category_tracker = defaultdict(int) 
        total_income_all_time = 0

        def parse_tab(tab_name):
            try:
                rows = wb.worksheet(tab_name).get_all_values()[1:]
                for r in rows:
                    if len(r) < 4: continue
                    ts = parse_sheet_timestamp(r[0])
                    if not ts: continue
                    try:
                        gold = int(str(r[3]).lower().replace('g','').replace(',','').strip())
                        t_key = r[2] or "Unknown"
                        all_entries.append({"ts": ts, "player": r[1], "type": t_key, "gold": gold, "desc": r[4] if len(r)>4 else ""})
                    except: continue
            except: pass

        parse_tab(TAB_DISCORD)
        parse_tab(TAB_FORM)
        parse_tab(TAB_OLD)
        all_entries.sort(key=lambda x: x['ts'], reverse=True)
        stats["last_5"] = all_entries[:5]

        for e in all_entries:
            d = e['ts'].date(); val = e['gold']; is_in = val > 0
            if is_in: category_tracker[e['type']] += val; total_income_all_time += val
            if d == today_date:
                if is_in: stats["today"]["in"] += val
                else: stats["today"]["out"] += val
                stats["breakdown"][e['type']] += val
            if d >= start_week:
                if is_in: stats["week"]["in"] += val
                else: stats["week"]["out"] += val
            if d >= start_month:
                if is_in: stats["month"]["in"] += val
                else: stats["month"]["out"] += val

        stats["today"]["net"] = stats["today"]["in"] + stats["today"]["out"]
        stats["week"]["net"] = stats["week"]["in"] + stats["week"]["out"]
        stats["month"]["net"] = stats["month"]["in"] + stats["month"]["out"]
        if category_tracker and total_income_all_time > 0:
            sorted_cats = sorted(category_tracker.items(), key=lambda item: item[1], reverse=True)
            stats["top_categories"] = " | ".join([f"{n} ({(v/total_income_all_time)*100:.1f}%)" for n,v in sorted_cats[:3]])
        else: stats["top_categories"] = "None"
    except Exception as e: logger.error(f"Fin stats error: {e}")
    return stats

# --- TIMER LOGIC ---
def make_standard_command(name):
    async def wrapper(ctx):
        state = load_state()
        overrides = state.get("standard_overrides", {})
        dur_str = overrides.get(name, STANDARD_DEFAULTS.get(name))
        dur = parse_duration_string(dur_str)
        if not dur: return await ctx.send("❌ Timer config error.")
        await handle_timer_request(ctx, name, dur)
    return commands.Command(wrapper, name=name)

def make_instanced_command(name):
    async def wrapper(ctx, duration: str = None):
        if not duration: return await ctx.send(f"❌ Usage: `!{name} [duration]`")
        dur = parse_duration_string(duration)
        if not dur: return await ctx.send(f"❌ Invalid time.")
        state = load_state()
        count = 1
        while f"{name}{count}" in state.get("timers", {}): count += 1
        await start_timer_execution(ctx, f"{name}{count}", dur, f"{name.capitalize()} #{count}")
    return commands.Command(wrapper, name=name)

def make_custom_command(name, duration_str):
    async def wrapper(ctx):
        dur = parse_duration_string(duration_str)
        if not dur: return await ctx.send(f"❌ Invalid duration.")
        await handle_timer_request(ctx, name, dur)
    return commands.Command(wrapper, name=name)

def register_commands():
    for cmd in INSTANCED_COMMANDS:
        if cmd in bot.all_commands: bot.remove_command(cmd)
        bot.add_command(make_instanced_command(cmd))
    for cmd in STANDARD_DEFAULTS:
        if cmd in bot.all_commands: bot.remove_command(cmd)
        bot.add_command(make_standard_command(cmd))
    state = load_state()
    for name, dur in state.get("custom_cmds", {}).items():
        if name in bot.all_commands: bot.remove_command(name)
        bot.add_command(make_custom_command(name, dur))

class ConfirmationView(discord.ui.View):
    def __init__(self, user_id, name, duration):
        super().__init__(timeout=30)
        self.user_id = user_id; self.name = name; self.duration = duration
    @discord.ui.button(label="✅ Start", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id: return
        await interaction.response.defer()
        await start_timer_execution(interaction, self.name, self.duration)
        for c in self.children: c.disabled=True
        await interaction.followup.edit_message(message_id=interaction.message.id, view=self)
        await interaction.followup.send(f"Started **{self.name}**!", ephemeral=True)
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id: return
        await interaction.message.delete()

async def handle_timer_request(ctx, name, duration):
    state = load_state()
    current = state['timers'].get(name)
    now = int(time.time())
    if current and current['status'] == 'running' and now < current['end_time']:
        return await ctx.send(f"⏳ **{name}** running! <t:{current['end_time']}:R>")
    view = ConfirmationView(ctx.author.id, name, duration)
    await ctx.send(f"❓ Start **{name}** ({duration})?", view=view)

async def start_timer_execution(ctx_or_int, unique_id, duration, display_name=None, hidden=False):
    state = load_state()
    end_time = int(time.time() + duration.total_seconds())
    if not display_name: display_name = unique_id.capitalize()
    state['timers'][unique_id] = {
        "end_time": end_time, "channel_id": PINNED_CHANNEL_ID, "status": "running", "display": display_name, "hidden": hidden
    }
    save_state(state)
    await update_dashboards()
    user_name = ctx_or_int.user.display_name if hasattr(ctx_or_int, "user") else ctx_or_int.author.display_name
    await log_to_channel("Timer Started", f"**{display_name}** started by {user_name}\nEnds: <t:{end_time}:f>", discord.Color.green())

# --- COMMANDS ---
@bot.command(name="update")
async def manual_update_check(ctx):
    """Force check GitHub for updates"""
    if not UPDATE_URL: return await ctx.send("❌ No Update URL configured.")
    msg = await ctx.send("🔄 Checking GitHub for updates...")
    try:
        # Add timestamp to bypass local caching
        r = requests.get(UPDATE_URL + f"?t={int(time.time())}")
        if r.status_code == 200:
            new_code = r.text
            current_code = ""
            with open(__file__, 'r', encoding='utf-8') as f: current_code = f.read()
            
            # Sanitize comparison (remove trailing whitespace/newlines)
            if current_code.strip() != new_code.strip():
                await msg.edit(content="✅ Update found! Overwriting and restarting...")
                await log_to_channel("Manual Update", f"Update triggered by {ctx.author.name}", discord.Color.purple())
                with open(__file__, 'w', encoding='utf-8') as f: f.write(new_code)
                os.execv(sys.executable, ['python'] + sys.argv)
            else:
                await msg.edit(content=f"✅ System is up to date.\nLocal Lines: {len(current_code.splitlines())}\nRemote Lines: {len(new_code.splitlines())}")
        else:
            await msg.edit(content=f"❌ GitHub returned status: {r.status_code}")
    except Exception as e:
        await msg.edit(content=f"❌ Error: {e}")

@bot.command(name="lt")
async def list_timers(ctx):
    state = load_state()
    lines = ["**📜 Available Timer Configurations**", "\n**Standard:**"]
    overrides = state.get("standard_overrides", {})
    for k, v in STANDARD_DEFAULTS.items():
        lines.append(f"`!{k}` : {overrides.get(k, v)}")
    lines.append("\n**Custom:**")
    customs = state.get("custom_cmds", {})
    if customs:
        for k, v in customs.items():
            lines.append(f"`!{k}` : {v}")
    else:
        lines.append("_None_")
    lines.append("\n**Instanced (Variable):**")
    for k in INSTANCED_COMMANDS:
        lines.append(f"`!{k} [time]`")
    await ctx.send("\n".join(lines))

@bot.command(name="tt")
async def temp_timer(ctx, name: str=None, duration: str=None):
    if not name or not duration: return await ctx.send("❌ Usage: `!tt [name] [duration]`")
    dur = parse_duration_string(duration)
    if not dur: return await ctx.send("❌ Invalid time.")
    
    # Prefix "tt_" for internal identification
    unique_id = f"tt_{name.lower()}"
    await start_timer_execution(ctx, unique_id, dur, display_name=name)

@bot.command(name="ct")
async def create_timer(ctx, name: str=None, duration: str=None):
    if not name or not duration: return await ctx.send("❌ Usage: `!ct [name] [duration]`")
    name = name.lower()
    if parse_duration_string(duration) is None: return await ctx.send("❌ Invalid time.")
    state = load_state()
    state["custom_cmds"][name] = duration
    save_state(state)
    if name in bot.all_commands: bot.remove_command(name)
    bot.add_command(make_custom_command(name, duration))
    await ctx.send(f"✅ Created **!{name}** ({duration})")
    await log_to_channel("Command Created", f"**!{name}** created with duration {duration} by {ctx.author.name}", discord.Color.blue())

@bot.command(name="dt")
async def delete_timer(ctx, name: str=None):
    if not name: return await ctx.send("❌ Usage: `!dt [name]`")
    name = name.lower()
    state = load_state()
    deleted = False
    if name in state["custom_cmds"]:
        del state["custom_cmds"][name]; deleted=True
        if name in bot.all_commands: bot.remove_command(name)
    to_del = [k for k in state["timers"] if k == name]
    for k in to_del: del state["timers"][k]; deleted=True
    if deleted:
        save_state(state)
        await update_dashboards()
        await ctx.send(f"🗑️ Deleted **!{name}**")
        await log_to_channel("Timer Deleted", f"**{name}** deleted by {ctx.author.name}", discord.Color.red())
    else: await ctx.send("❌ Not found.")

@bot.command(name="rt")
async def reset_timer(ctx, name: str=None):
    if not name: return await ctx.send("❌ Usage: `!rt [name]`")
    name = name.lower()
    state = load_state()
    if name in state["timers"]:
        del state["timers"][name]
        save_state(state)
        await update_dashboards()
        await ctx.send(f"🔄 Timer **{name}** reset.")
        await log_to_channel("Timer Reset", f"**{name}** reset manually by {ctx.author.name}", discord.Color.orange())
    else: await ctx.send(f"❌ Active/Done timer **{name}** not found.")

@bot.command(name="et")
async def edit_timer(ctx, name: str=None, duration: str=None):
    if not name or not duration: return await ctx.send("❌ Usage: `!et [name] [time]`")
    name = name.lower()
    if parse_duration_string(duration) is None: return await ctx.send("❌ Invalid time.")
    state = load_state()
    if name in STANDARD_DEFAULTS:
        state["standard_overrides"][name] = duration
        save_state(state)
        if name in bot.all_commands: bot.remove_command(name)
        bot.add_command(make_standard_command(name))
        await ctx.send(f"✏️ Updated standard **!{name}**")
    elif name in state["custom_cmds"]:
        state["custom_cmds"][name] = duration
        save_state(state)
        if name in bot.all_commands: bot.remove_command(name)
        bot.add_command(make_custom_command(name, duration))
        await ctx.send(f"✏️ Updated custom **!{name}**")
    else: await ctx.send("❌ Not found.")
    await log_to_channel("Command Edited", f"**!{name}** duration changed to {duration} by {ctx.author.name}", discord.Color.blue())

@bot.command(name="setrow")
async def set_row(ctx, row: int=None):
    if not row: return await ctx.send("❌ Usage: `!setrow [number]`")
    state = load_state()
    old_row = state.get("last_form_row", 1)
    state["last_form_row"] = row
    save_state(state)
    await ctx.send(f"🛠 Row count changed from `{old_row}` to `{row}`.")
    await log_to_channel("Row Updated", f"Changed from {old_row} to {row} by {ctx.author.name}", discord.Color.orange())

# --- SLASH COMMANDS ---
@bot.tree.command(name="v", description="Toggle Vacation Mode")
async def vacation(interaction: discord.Interaction):
    user_id = interaction.user.id
    state = load_state()
    vacationers = state.get("vacation", [])
    if user_id in vacationers:
        vacationers.remove(user_id)
        status = "OFF"
        msg = "Welcome back! You will now be tagged."
    else:
        vacationers.append(user_id)
        status = "ON"
        msg = "Enjoy your break! You will no longer be tagged."
    state["vacation"] = vacationers
    save_state(state)
    await interaction.response.send_message(f"🌴 Vacation Mode: **{status}**. {msg}", ephemeral=True)
    await log_to_channel("Vacation Toggle", f"{interaction.user.name} toggled vacation to {status}", discord.Color.teal())

@bot.tree.command(name="admin", description="Show Admin commands")
async def slash_admin(interaction: discord.Interaction):
    await interaction.response.send_message("Use `/help` for list, or `!ct`, `!dt`, `!et` to manage timers. Use `!setrow` to fix sheet index.", ephemeral=True)

@bot.tree.command(name="help", description="Show Bot Commands")
async def slash_help(interaction: discord.Interaction):
    state = load_state()
    customs = state.get("custom_cmds", {})
    embed = discord.Embed(title="🤖 JEFBot Commands", color=discord.Color.gold())
    embed.add_field(name="⏱ Presets", value=", ".join([f"`!{k}`" for k in STANDARD_DEFAULTS]), inline=False)
    embed.add_field(name="🌱 Instanced", value="`!seedbed [time]`, `!kq [time]`", inline=False)
    if customs:
        embed.add_field(name="⚡ Custom", value=", ".join([f"`!{k}` ({v})" for k, v in customs.items()]), inline=False)
    embed.add_field(name="🛠 Admin", value="`!ct`, `!et`, `!dt`, `!rt`, `!setrow`, `!tt`, `/createdemo`, `/prune`, `!lt`, `!update`", inline=False)
    embed.add_field(name="💰 Bank", value="`/bank`, `/deposit`, `/withdraw`", inline=False)
    embed.add_field(name="🌴 Misc", value="`/v` (Toggle Vacation)", inline=False)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="bank", description="Show detailed financial stats")
async def bank(interaction: discord.Interaction):
    await interaction.response.defer()
    stats = get_financial_detailed()
    if not stats: return await interaction.followup.send("❌ Error fetching data.")
    embed = discord.Embed(title="🏦 JEFBank Financials", color=discord.Color.gold())
    embed.add_field(name="💰 Gbank Value", value=f"**{stats['gbank_val']}**", inline=False)
    today_str = (f"📥 In: {stats['today']['in']}g\n📤 Out: {stats['today']['out']}g\n📈 Net: {stats['today']['net']}g")
    embed.add_field(name="📅 Today's Activity", value=today_str, inline=True)
    break_str = "\n".join([f"• {k}: {v}g" for k,v in stats['breakdown'].items()]) or "No activity"
    embed.add_field(name="📊 Today's Breakdown", value=break_str, inline=True)
    hist_str = ""
    for e in stats['last_5']:
        desc_str = f" - *{e['desc']}*" if e.get('desc') else ""
        hist_str += f"`{e['ts'].strftime('%d/%m %H:%M')}` **{e['player']}**: {e['type']} ({e['gold']}g){desc_str}\n"
    embed.add_field(name="📜 Last 5 Transactions", value=hist_str or "None", inline=False)
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="createdemo", description="Schedule a demo")
@app_commands.describe(location="Location Name", datetime_str="Format: 25.11.2025 00:30 (GB Time)")
async def createdemo(interaction: discord.Interaction, location: str, datetime_str: str):
    await interaction.response.defer()
    try:
        dt = GB_TZ.localize(datetime.strptime(datetime_str, "%d.%m.%Y %H:%M"))
    except ValueError:
        return await interaction.followup.send("❌ Invalid format. Use `DD.MM.YYYY HH:MM`")
    t_3h, t_1h, t_10m = dt-timedelta(hours=3), dt-timedelta(hours=1), dt-timedelta(minutes=10)
    now = get_gb_time()
    forum = bot.get_channel(DEMO_FORUM_ID)
    if forum and isinstance(forum, discord.ForumChannel):
        await forum.create_thread(name=f"{location} - {dt.strftime('%d/%m')}", content=(f"**Demo Scheduled**\n📍 **Location:** {location}\n📅 **Time:** <t:{int(dt.timestamp())}:F>\n{get_ping_string()}"))
    state = load_state()
    if dt > now:
        state['timers'][f"demo_{location}_main"] = {
            "end_time": int(dt.timestamp()), "channel_id": PINNED_CHANNEL_ID, "status": "running", "display": f"Demo {location}", "hidden": False
        }
    for lbl, obj in [("3h", t_3h), ("1h", t_1h), ("10m", t_10m)]:
        if obj > now:
            state['timers'][f"demo_{location}_{lbl}"] = {
                "end_time": int(obj.timestamp()), "channel_id": PINNED_CHANNEL_ID, "status": "running", "display": f"Demo Alert {location} {lbl}", "hidden": True
            }
    save_state(state)
    await update_dashboards()
    await interaction.followup.send(f"✅ Demo set for {location} at <t:{int(dt.timestamp())}:f>.")
    await log_to_channel("Demo Created", f"Demo at {location} for {datetime_str} created by {interaction.user.name}", discord.Color.purple())

@bot.tree.command(name="prune", description="Delete messages (Protected)")
async def prune(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("❌ No permission.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await interaction.channel.purge(limit=None, check=lambda m: not m.pinned)
    await interaction.followup.send("🧹 Channel pruned (Pins protected).", ephemeral=True)

@bot.tree.command(name="deposit", description="Log deposit")
@app_commands.choices(type=[app_commands.Choice(name=k, value=k) for k in ["Larders", "Dungeon", "Crafting", "Donation", "Traderun", "Loyalty", "Other"]])
async def deposit(interaction: discord.Interaction, type: app_commands.Choice[str], gold: int, description: str = ""):
    await handle_transaction(interaction, type.value, gold, description)

@bot.tree.command(name="withdraw", description="Log withdrawal")
@app_commands.choices(category=[app_commands.Choice(name="General", value="General"), app_commands.Choice(name="Regrades", value="Regrades")])
async def withdraw(interaction: discord.Interaction, category: app_commands.Choice[str], gold: int, description: str):
    await handle_transaction(interaction, "Regrades" if category.value == "Regrades" else "Withdraw", -abs(gold), description)

def get_mapped_name(user: discord.User): return PLAYER_MAP.get(user.id, user.display_name)

async def handle_transaction(interaction, type_str, gold_amt, desc_str):
    await interaction.response.defer()
    client = get_gspread_client()
    if not client: return await interaction.followup.send("❌ DB Error")
    try:
        ts = get_gb_time().strftime("%Y-%m-%d %H:%M:%S")
        sheet = client.open(SHEET_NAME).worksheet(TAB_DISCORD)
        player = get_mapped_name(interaction.user)
        sheet.append_row([ts, player, type_str, gold_amt, desc_str])
        color = discord.Color.green() if gold_amt > 0 else discord.Color.red()
        embed = discord.Embed(title=f"💰 {type_str}", color=color)
        embed.add_field(name="Player", value=player)
        embed.add_field(name="Gold", value=str(gold_amt))
        embed.add_field(name="Desc", value=desc_str)
        await interaction.followup.send(embed=embed)
        await update_dashboards()
        await log_to_channel("Transaction", f"{player} logged {type_str}: {gold_amt}g", color)
    except Exception as e:
        logger.error(f"Tx Error: {e}")
        await interaction.followup.send(f"Error: {e}")

# --- TASKS ---
@tasks.loop(minutes=1)
async def scheduler_task():
    now = get_gb_time()
    today_str = now.strftime("%Y-%m-%d")
    state = load_state()
    is_time = (now.hour == 4 and now.minute == 30)
    is_late = (now.hour >= 4 and (now.hour > 4 or now.minute > 30)) and (state.get("last_motd_date") != today_str)
    
    if is_time or is_late:
        weekday = now.weekday()
        msg = None
        if weekday == 4: msg = "Pick DS quest AT today"
        elif weekday == 5: msg = "Fish AT today\nDGS AT today\nLib AT today"
        elif weekday == 6: msg = "Anth AT today"
        
        # LOGIC FIX: Always update MOTD. If no msg, clear it.
        if msg:
            state["motd"] = msg
        else:
            state["motd"] = ""
            
        state["last_motd_date"] = today_str
        save_state(state)
        
        if is_time and msg:
            chan = bot.get_channel(PINNED_CHANNEL_ID)
            if chan: await chan.send(f"📢 **DAILY REMINDER**\n{msg}")
        
        await update_dashboards()

@bot.tree.command(name="refresh", description="Force update")
async def refresh(interaction: discord.Interaction):
    await interaction.response.defer()
    await run_sheet_check(True)
    await update_dashboards()
    await interaction.followup.send("Updated.")

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    register_commands()
    try:
        guild = bot.get_channel(PINNED_CHANNEL_ID).guild
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
    except: pass
    if not background_sheet_check.is_running(): background_sheet_check.start()
    if not timer_monitor.is_running(): timer_monitor.start()
    if not update_pinned_message.is_running(): update_pinned_message.start()
    if not scheduler_task.is_running(): scheduler_task.start()
    if not hourly_state_backup.is_running(): hourly_state_backup.start()
    if not channel_wiper.is_running(): channel_wiper.start()
    if not github_monitor.is_running(): github_monitor.start()
    
    chan = bot.get_channel(PINNED_CHANNEL_ID)
    if chan: await chan.send(f"🤖 **JEFFBANK IS AWAKE** ({BOT_VERSION})")

@tasks.loop(minutes=60)
async def background_sheet_check(): await run_sheet_check(False)

# UPDATED WIPER LOGIC: 12 PM Threshold
@tasks.loop(minutes=2)
async def channel_wiper():
    try:
        channel = bot.get_channel(PINNED_CHANNEL_ID)
        if not channel: return
        
        now = get_gb_time()
        # Midnight today (e.g., 00:00:00 on Tuesday)
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Determine the cutoff time based on current hour
        if now.hour >= 12:
            # It's after noon. Cutoff is midnight today.
            # (Delete everything from yesterday and before)
            cutoff = today_midnight
        else:
            # It's before noon. Cutoff is midnight yesterday.
            # (Keep yesterday, delete older)
            cutoff = today_midnight - timedelta(days=1)

        def should_delete(m):
            if m.pinned: return False
            # Check if message is older than cutoff
            # Note: m.created_at is UTC. Convert to GB_TZ to compare.
            msg_time = m.created_at.astimezone(GB_TZ)
            return msg_time < cutoff

        await channel.purge(limit=500, check=should_delete)
    except Exception as e:
        logger.error(f"Wipe error: {e}")

@tasks.loop(minutes=5)
async def github_monitor():
    """Checks for updates from GitHub Raw Link and restarts if found"""
    if not UPDATE_URL: return
    try:
        r = requests.get(UPDATE_URL + f"?t={int(time.time())}")
        if r.status_code == 200:
            new_code = r.text
            current_code = ""
            with open(__file__, 'r', encoding='utf-8') as f: current_code = f.read()
            if current_code.strip() != new_code.strip():
                logger.info("Update detected from GitHub. Overwriting and restarting...")
                await log_to_channel("System Update", "New code detected on GitHub. Overwriting and restarting...", discord.Color.purple())
                with open(__file__, 'w', encoding='utf-8') as f: f.write(new_code)
                os.execv(sys.executable, ['python'] + sys.argv)
    except Exception as e: logger.error(f"GitHub Monitor Error: {e}")

@tasks.loop(seconds=1)
async def timer_monitor():
    state = load_state()
    timers = state.get("timers", {})
    dirty = False
    now = int(time.time())
    
    for name, data in list(timers.items()):
        
        # 1. Check for Running -> Expired
        if data['status'] == 'running' and now >= data['end_time']:
            channel = bot.get_channel(PINNED_CHANNEL_ID)
            if channel:
                try:
                    d_name = data.get('display', name.capitalize())
                    ping = get_ping_string()
                    if "demo" in name and "hidden" in data and data['hidden']:
                        msg = await channel.send(f"⚠️ **ALERT:** {d_name} is coming up! {ping}")
                    else:
                        msg = await channel.send(f"⏰ **{d_name} IS UP!** {ping}")
                        await log_to_channel("Timer Expired", f"{d_name} expired", discord.Color.gold())
                    if "hidden" in data and data['hidden']:
                        del timers[name]
                    else:
                        data['status'] = 'expired'
                        data['msg_id'] = msg.id 
                    dirty = True
                except: pass
        
        # 2. Check for Expired -> Auto-Delete
        elif data['status'] == 'expired':
            
            # A) Fast cleanup (1 hour) for seedbed/kq
            if any(name.startswith(p) for p in ["seedbed", "kq"]):
                if now > (data['end_time'] + 3600): # 1 hr
                    del timers[name]
                    dirty = True
                    
            # B) Slow cleanup (24 hours) for demo/tt
            elif any(name.startswith(p) for p in ["demo", "tt_"]):
                if now > (data['end_time'] + 86400): # 24 hr
                    del timers[name]
                    dirty = True

    if dirty:
        save_state(state)
        await update_dashboards()

@tasks.loop(minutes=10)
async def update_pinned_message(): await update_dashboards()

@tasks.loop(hours=1)
async def hourly_state_backup():
    state = load_state()
    state_str = json.dumps(state, indent=2)
    if len(state_str) > 1900: state_str = state_str[:1900] + "\n...[TRUNCATED]"
    await log_to_channel("Hourly State Backup", f"```json\n{state_str}\n```", discord.Color.dark_grey())

async def run_sheet_check(manual):
    client = get_gspread_client()
    if not client: return
    try:
        sheet = client.open(SHEET_NAME).worksheet(TAB_FORM)
        all_rows = sheet.get_all_values()
        current = len(all_rows)
        state = load_state()
        last = max(state.get("last_form_row", 1), 1)
        count = 0
        if current > last:
            chan = bot.get_channel(PINNED_CHANNEL_ID)
            for i in range(last, current):
                r = all_rows[i]
                if not any(r): continue
                while len(r) < 5: r.append("")
                embed = discord.Embed(title="💸 Form Update", color=discord.Color.blue())
                embed.add_field(name="Player", value=r[1])
                embed.add_field(name="Gold", value=r[3])
                embed.add_field(name="Type", value=r[2])
                embed.set_footer(text=r[0])
                if chan: await chan.send(embed=embed)
                count += 1
            state["last_form_row"] = current
            save_state(state)
        if not manual: await log_to_channel("Sheet Check", f"Checked Form. Current Row: {current}. New Entries: {count}", discord.Color.light_gray())
    except Exception as e: await log_to_channel("Sheet Check Error", str(e), discord.Color.red())

async def update_dashboards():
    channel = bot.get_channel(PINNED_CHANNEL_ID)
    if not channel: return
    state = load_state()
    stats = get_financial_detailed()
    timers = state.get("timers", {})
    now_gb = get_gb_time()
    now_ts = int(time.time())
    fin_lines = [HEADER_FIN]
    if stats:
        fin_lines.extend([
            f"Last Restart: <t:{START_TIME}:f>",
            f"Current Gbank: **{stats['gbank_val']}**",
            f"Top Contributions: **{stats['top_categories']}**",
            f"Last Refresh: <t:{int(now_gb.timestamp())}:f>",
            "---",
            f"**Today:** In {stats['today']['in']} | Out {stats['today']['out']} | Net {stats['today']['net']}",
            f"**Week:** In {stats['week']['in']} | Out {stats['week']['out']} | Net {stats['week']['net']}",
            f"**Month:** In {stats['month']['in']} | Out {stats['month']['out']} | Net {stats['month']['net']}",
            "---"
        ])
    timer_lines = [HEADER_TIMER]
    if state.get("motd"):
        timer_lines.append(f"\n📢 **TODAY:**\n{state['motd']}\n")
    list_today = []
    list_later = []
    list_done = []
    for name, data in sorted(timers.items(), key=lambda x: x[1].get('end_time', 0)):
        if data.get('hidden'): continue
        d_name = data.get('display', name.capitalize())
        if data['status'] == 'running':
            t_str = f"• **{d_name}**: <t:{data['end_time']}:R>"
            if (data['end_time'] - now_ts) > 86400:
                list_later.append(t_str)
            else:
                list_today.append(t_str)
        elif data['status'] == 'expired':
            list_done.append(f"• **{d_name}** (<t:{data['end_time']}:R>)")
    timer_lines.append("**Timers (Today)**")
    timer_lines.extend(list_today if list_today else ["_None_"])
    timer_lines.append("\n**Timers (1d+)**")
    timer_lines.extend(list_later if list_later else ["_None_"])
    timer_lines.append("\n**Timers (DONE)**")
    timer_lines.extend(list_done if list_done else ["_None_"])
    try:
        history = await channel.pins()
        msg_fin = next((m for m in history if m.author == bot.user and HEADER_FIN in m.content), None)
        if msg_fin:
            await msg_fin.edit(content="\n".join(fin_lines))
        else:
            n = await channel.send("\n".join(fin_lines))
            await n.pin()
        msg_tim = next((m for m in history if m.author == bot.user and HEADER_TIMER in m.content), None)
        if msg_tim:
            await msg_tim.edit(content="\n".join(timer_lines))
        else:
            n = await channel.send("\n".join(timer_lines))
            await n.pin()
    except: pass

bot.run(TOKEN)
