import asyncio
import io
import logging
import math
import pprint
import random
from collections import Counter, defaultdict
from urllib import request

import asyncpg
import discord
import googletrans
import numpy as np
import youtube_dl
from discord.ext import commands, tasks
from async_timeout import timeout

from cogs.utils import db

from .utils import checks
from .videos import Videos
import itertools

FFMPEG_BEFORE_OPTS = '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5'
"""
Command line options to pass to `ffmpeg` before the `-i`.
See https://stackoverflow.com/questions/43218292/youtubedl-read-error-with-discord-py/44490434#44490434 for more information.
Also, https://ffmpeg.org/ffmpeg-protocols.html for command line option reference.
"""


class VoiceConnectionError(commands.CommandError):
    """Custom Exception class for connection errors."""


class InvalidVoiceChannel(VoiceConnectionError):
    """Exception for cases of invalid Voice Channels."""

async def audio_playing(ctx):
    """Checks that audio is currently playing before continuing."""
    client = ctx.guild.voice_client
    if client and client.channel and client.source:
        return True
    else:
        raise commands.CommandError("Not currently playing any audio.")


async def in_voice_channel(ctx):
    """Checks that the command sender is in the same voice channel as the bot."""
    voice = ctx.author.voice
    bot_voice = ctx.guild.voice_client
    if voice and bot_voice and voice.channel and bot_voice.channel and voice.channel == bot_voice.channel:
        return True
    else:
        raise commands.CommandError(
            "You need to be in a channel to do that.")


async def is_audio_requester(ctx):
    """Checks that the command sender is the song requester."""
    permissions = ctx.channel.permissions_for(ctx.author)
    if permissions.administrator:
        return True
    else:
        raise commands.CommandError(
            "You need to be an admin to do that.")

class MusicTable(db.Table, table_name="playlists"):
    id = db.PrimaryKeyColumn()

    name = db.Column(db.String, index=True)
    author_id = db.Column(db.Integer(big=True), index=True)
    url = db.Column(db.String, index=True)
    title = db.Column(db.String)
    uploader = db.Column(db.String)

    @classmethod
    def create_table(cls, *, exists_ok=True):
        statement = super().create_table(exists_ok=exists_ok)

        # create the unique indexes
        sql = "CREATE UNIQUE INDEX IF NOT EXISTS playlists_uniq_idx ON playlists (name, author_id, url);"
        return statement + '\n' + sql
    


class Music(commands.Cog):
    """Bot commands to help play music."""

    def __init__(self, bot):
        self.bot = bot
        self.max_vol = bot.max_volume
        self.vote_skip = bot.vote_skip
        self.vote_skip_ratio = bot.vote_skip_ratio
        self.states = {}
        self.bot.add_listener(self.on_reaction_add, "on_reaction_add")
        # database mutex access
        self._batch_of_data = []
        self._batch_lock = asyncio.Lock(loop=bot.loop)
        self.bulk_insert.add_exception_type(asyncpg.PostgresConnectionError)
        self.bulk_insert.start()

    def cog_unload(self):
        self.bulk_insert.stop()

    async def cog_command_error(self, ctx, error):
       if isinstance(error, commands.BadArgument):
            await ctx.send(error)

    def get_player(self, ctx):
        """Retrieve the guild player, or generate one."""
        try:
            player = self.states[ctx.guild.id]
        except KeyError:
            player = GuildState(ctx)
            self.states[ctx.guild.id] = player

        return player

    async def cleanup(self, guild):
        try:
            await guild.voice_client.disconnect()
        except AttributeError:
            pass

        try:
            del self.states[guild.id]
        except KeyError:
            pass

    @tasks.loop(seconds=60.0)
    async def bulk_insert(self):
        query = """INSERT INTO playlists (name, author_id, url, title, uploader)
                   SELECT x.name, x.author, x.url, x.title, x.uploader
                   FROM jsonb_to_recordset($1::jsonb) AS x(name TEXT, author BIGINT, url TEXT, title TEXT, uploader TEXT)
                """
        async with self._batch_lock:
            try:
                await self.bot.pool.execute(query, self._batch_of_data)
            except Exception as e:
                print(e)
            self._batch_of_data.clear()

    async def on_reaction_add(self, reaction, user):
        """Responds to reactions added to the bot's messages, allowing reactions to control playback."""
        message = reaction.message
        if user != self.bot.user and message.author == self.bot.user:
            await message.remove_reaction(reaction, user)
            if message.guild and message.guild.voice_client:
                user_in_channel = user.voice and user.voice.channel and user.voice.channel == message.guild.voice_client.channel
                permissions = message.channel.permissions_for(user)
                if permissions.administrator:
                    client = message.guild.voice_client
                    if reaction.emoji == "⏯":
                        # pause audio
                        self._pause_audio(client)
                    elif reaction.emoji == "⏭":
                        # skip audio
                        client.stop()  
                    # doesn't do anything atm   
                    elif reaction.emoji == "⏮":
                        pass
                elif reaction.emoji == "⏭" and self.vote_skip and user_in_channel and message.guild.voice_client and message.guild.voice_client.channel:
                    # ensure that skip was pressed, that vote skipping is
                    # enabled, the user is in the channel, and that the bot is
                    # in a voice channel
                    voice_channel = message.guild.voice_client.channel
                    self._vote_skip(voice_channel, user)
                    # announce vote
                    channel = message.channel
                    users_in_channel = len([
                        member for member in voice_channel.members
                        if not member.bot
                    ])  # don't count bots
                    required_votes = math.ceil(
                        self.vote_skip_ratio * users_in_channel)
                    await channel.send(
                        f"{user.mention} voted to skip ({len(state.skip_votes)}/{required_votes} votes)"
                    )

    
    def _pause_audio(self, client):
        if client.is_paused():
            client.resume()
        else:
            client.pause()


    async def get_playlist(self, ctx, playlist_name="default"):
        query = """SELECT *
                   FROM playlists
                   WHERE author_id=$1 AND name=$2 
                   ORDER BY id
                   LIMIT 20;
                """
        return await ctx.db.fetch(query, ctx.author.id, playlist_name)


    # TODO: add reaction control support
    # TODO: loading in complete playlist
    async def register_video(self, ctx, playlist_name):
        '''
        use this in 'add_to_playlist <playlist_name>' command
        '''
        if ctx.command is None:
            return

        if ctx.guild is None:
            return

        state = self.get_player(ctx)
        async with self._batch_lock:
            self._batch_of_data.append({
                'name': playlist_name,
                'author': ctx.author.id,
                'url': state.now_playing["webpage_url"],
                'title': state.now_playing['title'],
                'uploader': state.now_playing['uploader'],
            })

    def get_state(self, ctx):
        """Gets the state for `guild`, creating it if it does not exist."""
        if ctx.guild.id in self.states:
            return self.states[ctx.guild.id]
        else:
            self.states[ctx.guild.id] = GuildState(ctx)
            return self.states[ctx.guild.id]

    def _vote_skip(self, ctx, member):
        """Register a vote for `member` to skip the song playing."""
        logging.info(f"{member.name} votes to skip")
        state = self.get_state(ctx)
        state.skip_votes.add(member)
        users_in_channel = len([
            member for member in ctx.channel.members if not member.bot
        ])  # don't count bots
        if (float(len(state.skip_votes)) /
                users_in_channel) >= self.vote_skip_ratio:
            # enough members have voted to skip, so skip the song
            logging.info(f"Enough votes, skipping...")
            ctx.channel.guild.voice_client.stop()

    def _play_song(self, client, state, song):
        state.now_playing = song
        state.skip_votes = set()  # clear skip votes
        source = discord.PCMVolumeTransformer(
            discord.FFmpegPCMAudio(song["formats"][0]["url"], before_options=FFMPEG_BEFORE_OPTS), volume=state.volume)

        def after_playing(err):
            if len(state.queue) > 0:
                next_song = state.queue.pop(0)
                self._play_song(client, state, next_song)
            else:
                state.now_playing = None
                asyncio.run_coroutine_threadsafe(client.disconnect(),
                                                 self.bot.loop)
                

        client.play(source, after=after_playing)

    def _queue_text(self, queue):
        """Returns a block of text describing a given song queue."""
        if len(queue) > 0:
            message = [f"{len(queue)} songs in queue:"]
            message += [
                
                f"  {index+1}. **{song['title']}** (requested by **{song['requested_by'].name}**)"
                for (index, song) in enumerate(queue)
            ]  # add individual songs
            return "\n".join(message)
        else:
            return "The play queue is empty."

    def empty_queue(q: asyncio.Queue):
        for _ in range(q.qsize()):
            # Depending on your program, you may want to
            # catch QueueEmpty
            q.get_nowait()
            q.task_done()

    async def delete_from_playlist(self, ctx, id, playlist_name):
        query = """DELETE
                   FROM playlists
                   WHERE playlists.author_id=$1 AND playlists.id=$2 AND playlists.name=$3;
                """
        try:
            await self.bot.pool.execute(query, ctx.author.id, int(id), playlist_name)
        except Exception as e:
            await ctx.send(e)

    @commands.command(aliases=["plremove"])
    @commands.guild_only()
    async def playlist_remove(self, ctx, id,*, playlistname = 'default'):
        """
            Delete a song from your playlist by ID and playlistname
            Playlist defaults to 'default'.
        """
        try:
            await self.delete_from_playlist(ctx, id, playlistname)
            message = f"{id} was deleted from {playlistname}."
            await ctx.send(message)
        except:
            raise commands.CommandError("Something went wrong while loading your playlist.")

    @commands.command(aliases=["pllist"])
    @commands.guild_only()
    async def playlist_list(self, ctx, *, playlistname = 'default'):
        """
            List your playlist
            Playlist defaults to 'default'.
        """
        try:
            count = 0
            songs = await self.get_playlist(ctx, playlistname)
            message = f"```ini\n{playlistname}:\n"
            for song in songs:
                count += 1
                message += f"\t[ID]: {song['id']} - [Title]: {song['title']}\n"
                if count > 10:
                    message += f"... ({len(songs)} total).\n"
                    break
            message += "```"
            await ctx.send(message)
        except:
            raise commands.CommandError("Something went wrong while loading your playlist.")

    @commands.command(aliases=["plplay","playplaylist"])
    @commands.guild_only()
    async def playlist_play(self, ctx, *, playlistname = 'default'):
        """
            Play your playlist
            Playlist defaults to 'default'.
        """
        try:
            songs = await self.get_playlist(ctx, playlistname)
            for song in songs:
                await ctx.invoke(self.bot.get_command('play'), url=song['url'])
        except:
            raise commands.CommandError("Something went wrong while loading your playlist.")
    
    @commands.command(aliases=["pladd"])
    @commands.guild_only()
    async def playlist_add(self, ctx, playlistname = 'default'):
        """
            Adds the currently playing song in this guild to your specified playlist.
            Playlist defaults to 'default'.
            Add prompt when making new playlist.
        """
        try:
            await self.register_video(ctx, playlistname)
            await ctx.send(f"Succesfully added *{self.get_player(ctx).now_playing['title']}* to *{playlistname}*.")
        except:
            raise commands.CommandError("Something went wrong while adding the song to your playlist.")

    @commands.command(aliases=["stop"])
    @commands.guild_only()
    async def leave(self, ctx):
        """Leaves the voice channel, if currently in one."""
        client = ctx.guild.voice_client
        state = self.get_player(ctx)
        if client and client.channel:
            await client.disconnect()
            await self.cleanup(ctx.guild)
        else:
            raise commands.CommandError("Not in a voice channel.")

    @commands.command(aliases=["p"])
    @commands.guild_only()
    @commands.check(audio_playing)
    @commands.check(in_voice_channel)
    @commands.check(is_audio_requester)
    async def pause(self, ctx):
        """Pauses any currently playing audio."""
        vc = ctx.voice_client

        if vc.is_paused():
            return

        vc.pause()
        await ctx.send(f'**`{ctx.author}`**: Paused the song!')
    

    @commands.guild_only()
    @commands.check(audio_playing)
    @commands.check(in_voice_channel)
    @commands.check(is_audio_requester)
    @commands.command(name='resume')
    async def resume_(self, ctx):
        """Resume the currently paused song."""
        vc = ctx.voice_client

        if not vc.is_paused():
            return

        vc.resume()
        await ctx.send(f'**`{ctx.author}`**: Resumed the song!')

    @commands.command(aliases=["vol", "v"])
    @commands.guild_only()
    @commands.check(audio_playing)
    @commands.check(in_voice_channel)
    @commands.check(is_audio_requester)
    async def volume(self, ctx, volume: int):
        """Change the volume of currently playing audio (values 0-250)."""
        state = self.get_player(ctx)

        # make sure volume is nonnegative
        if volume < 0:
            volume = 0

        max_vol = self.max_vol
        if max_vol > -1:  # check if max volume is set
            # clamp volume to [0, max_vol]
            if volume > max_vol:
                volume = max_vol

        client = ctx.guild.voice_client

        state.volume = float(volume) / 100.0
        client.source.volume = state.volume  # update the AudioSource's volume to match

     # TODO: fix new version
    @commands.command()
    @commands.guild_only()
    @commands.check(audio_playing)
    @commands.check(in_voice_channel)
    async def skip(self, ctx):
        """Skips the currently playing song, or votes to skip it."""
        vc = ctx.voice_client
        vc.stop()
        await ctx.send(f'**`{ctx.author}`**: Skipped the song!')



     # TODO: fix new version
     # No longer necessary with new version
    @commands.command(aliases=["np"], disabled=True)
    @commands.guild_only()
    @commands.check(audio_playing)
    async def nowplaying(self, ctx):
        """Displays information about the current song."""
        state = self.get_state(ctx)
        await state.player.delete()
        message = await ctx.send("", embed=Videos.get_embed(video=state.now_playing))
        await self._add_reaction_controls(message)
        state.player = message

    @commands.command()
    @commands.guild_only()
    @commands.check(audio_playing)
    async def shuffle(self, ctx):
        """Shuffles the current playlist"""
        state = self.get_state(ctx)
        order = list(range(0,(len(state.queue))))
        random.shuffle(order)
        new_playlist = []
        for i in order:
            new_playlist.append(state.queue[i])
        state.queue = new_playlist
        if len(state.queue) > 0:
            await ctx.send(f"Playlist is shuffled. Next song is **{state.queue[0]['title']}**")
        else:
            await ctx.send("No songs in the queue to shuffle.")

    @commands.command(aliases=["q", "playlist"])
    @commands.guild_only()
    @commands.check(audio_playing)
    async def queue(self, ctx):
        """Display the current play queue."""
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently connected to voice!', delete_after=20)

        player = self.get_player(ctx)
        if len(player.queue) == 0:
            return await ctx.send('There are currently no more queued songs.')

        # Grab up to 10 entries from the queue...
        upcoming = list(itertools.islice(player.queue, 0, 10))

        fmt = '\n'.join(f'**`{_["title"]}`**' for _ in upcoming)
        embed = discord.Embed(title=f'Upcoming - Next {len(upcoming)}', description=fmt)

        await ctx.send(embed=embed)

    @commands.command(aliases=["cq"])
    @commands.guild_only()
    @commands.check(audio_playing)
    @commands.has_permissions(administrator=True)
    async def clearqueue(self, ctx):
        """Clears the play queue without leaving the channel."""
        state = self.get_player(ctx)
        await self.clearqueue(state.queue)


    @commands.command(aliases=["jq"])
    @commands.guild_only()
    @commands.check(audio_playing)
    @commands.has_permissions(administrator=True)
    async def jumpqueue(self, ctx, song: int, new_index: int):
        """Moves song at an index to `new_index` in queue."""
        state = self.get_player(ctx)  # get state for this guild
        if 1 <= song <= len(state.queue) and 1 <= new_index:
            song = state.queue.pop(song - 1)  # take song at index...
            state.queue.insert(new_index - 1, song)  # and insert it.

            await ctx.send(self._queue_text(state.queue))
        else:
            raise commands.CommandError("You must use a valid index.")

    @commands.command(brief="Plays audio from <url>.")
    @commands.guild_only()
    async def play(self, ctx, *, url):
        """
            Plays audio hosted at <url> (or performs a search for <url> and plays the first result).
            Takes maximum 20 songs. First 20 in a given playlist.
        """
        
        await ctx.trigger_typing()

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)

        player = self.get_player(ctx)

        # If download is False, source will be a dict which will be used later to regather the stream.
        # If download is True, source will be a discord.FFmpegPCMAudio with a VolumeTransformer.
        try:
            videos = Videos(url, ctx.author)
        except youtube_dl.DownloadError as e:
            await ctx.send(
                "There was an error downloading your video(s), sorry.")
            return
        for v in videos:
            player.queue.append(v)
        await ctx.send(f"Added {len(videos)} songs to the queue.")

    @commands.command(name='connect', aliases=['c'])
    async def connect_(self, ctx, *, channel: discord.VoiceChannel=None):
        """Connect to voice.
        Parameters
        ------------
        channel: discord.VoiceChannel [Optional]
            The channel to connect to. If a channel is not specified, an attempt to join the voice channel you are in
            will be made.
        This command also handles moving the bot to different channels.
        """
        if not channel:
            try:
                channel = ctx.author.voice.channel
            except AttributeError:
                raise InvalidVoiceChannel('No channel to join. Please either specify a valid channel or join one.')

        vc = ctx.voice_client

        if vc:
            if vc.channel.id == channel.id:
                return
            try:
                await vc.move_to(channel)
            except asyncio.TimeoutError:
                raise VoiceConnectionError(f'Moving to channel: <{channel}> timed out.')
        else:
            try:
                await channel.connect()
            except asyncio.TimeoutError:
                raise VoiceConnectionError(f'Connecting to channel: <{channel}> timed out.')

        await ctx.send(f'Connected to: **{channel}**', delete_after=20)


    async def _add_reaction_controls(self, message):
        """Adds a 'control-panel' of reactions to a message that can be used to control the bot."""
        CONTROLS = ["⏮", "⏯", "⏭"]
        for control in CONTROLS:
            await message.add_reaction(control)

class GuildState:
    def __init__(self, ctx):
        self.bot = ctx.bot
        self._guild = ctx.guild
        self._channel = ctx.channel
        self._cog = ctx.cog

        self.skip_votes = set()

        self.queue = []
        self.event = asyncio.Event()

        self.player = None  # Now playing message
        self.volume = .5
        self.now_playing = None

        ctx.bot.loop.create_task(self.player_loop())

    async def player_loop(self):
        """Our main player loop."""
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            self.event.clear()

            try:
                # Wait for the next song. If we timeout cancel the player and disconnect...  # 5 minutes...
                source = self.queue.pop(0)
            except:
                return self.destroy(self._guild)
            self.now_playing = source
            play_source = discord.PCMVolumeTransformer(
            discord.FFmpegPCMAudio(source["formats"][0]["url"], before_options=FFMPEG_BEFORE_OPTS), volume=self.volume)
            self._guild.voice_client.play(play_source, after=lambda _ : self.bot.loop.call_soon_threadsafe(self.event.set))
            
            self.player = await self._channel.send(embed=Videos.get_embed(self.now_playing))
            await self._add_reaction_controls(self.player)
            await self.event.wait()
            # Make sure the FFmpeg process is cleaned up.
            play_source.cleanup()
            self.now_playing = None
            try:
                # We are no longer playing this song...
                await self.player.delete()
            except discord.HTTPException:
                pass

    def destroy(self, guild):
        """Disconnect and cleanup the player."""
        return self.bot.loop.create_task(self._cog.cleanup(guild))

    async def _add_reaction_controls(self, message):
        """Adds a 'control-panel' of reactions to a message that can be used to control the bot."""
        CONTROLS = ["⏮", "⏯", "⏭"]
        for control in CONTROLS:
            await message.add_reaction(control)

def setup(bot):
    bot.add_cog(Music(bot))
