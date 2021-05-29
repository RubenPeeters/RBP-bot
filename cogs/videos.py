import youtube_dl as ytdl
import discord
import pprint

YTDL_OPTS = {
    "default_search": "ytsearch",
    "format": "bestaudio/best",
    "quiet": True,
    "extract_flat": "in_playlist"
}


class Videos(list):
    """Class containing information about a batch of videos"""

    def __init__(self, url_or_search=None, requested_by:discord.User=None):
        """Plays audio from (or searches for) a URL."""
        with ytdl.YoutubeDL(YTDL_OPTS) as ydl:
            if requested_by: self.requested_by = requested_by
            if url_or_search: self._get_info(url_or_search)            

    def _get_info(self, video_url):
        N = 20
        with ytdl.YoutubeDL(YTDL_OPTS) as ydl:
            info = ydl.extract_info(video_url, download=False)
            if "_type" in info and info["_type"] == "playlist":
                count = 0
                for entry in info["entries"]:
                    self._get_info(entry["url"])
                    count += 1
                    if count >= N:
                        break
            else:
                info["requested_by"] = self.requested_by
                self.append(info)

    def get_embed(video):
        """Makes an embed out of this Video's information."""
        embed = discord.Embed(
            title=video["title"], description=video["uploader"] if "uploader" in video else "", url=video["webpage_url"])
        embed.set_footer(
            text=f"Requested by {video['requested_by'].name}",
            icon_url=video['requested_by'].avatar_url)
        thumbnail = video["thumbnail"] if "thumbnail" in video else None
        if thumbnail:
            embed.set_thumbnail(url=thumbnail)
        return embed