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
    """Class containing information about a batch of videos all requested by the same person."""
    # TODO: We want the videos to always be a list (size=1 when only one video).
    #       We then use this to alter current implementation.

    def __init__(self, url_or_search, requested_by:discord.User):
        """Plays audio from (or searches for) a URL."""
        with ytdl.YoutubeDL(YTDL_OPTS) as ydl:
            self.requested_by = requested_by
            self._get_info(url_or_search)
            # video_format = video["formats"][0]
            # self.stream_url = video_format["url"]
            # self.video_url = video["webpage_url"]
            # self.title = video["title"]
            # self.uploader = video["uploader"] if "uploader" in video else ""
            # self.thumbnail = video[
            #     "thumbnail"] if "thumbnail" in video else None
            


    def _get_info(self, video_url):
        with ytdl.YoutubeDL(YTDL_OPTS) as ydl:
            info = ydl.extract_info(video_url, download=False)
            # TODO: make iterative so that every video gets loaded in (max = N).
            if "_type" in info and info["_type"] == "playlist":
                for entry in info["entries"]:
                    # print(entry)
                    self._get_info(entry["url"])

                # return self._get_info(
                #     info["entries"][0]["url"])  # get info for first video
            else:
                info["requested_by"] = self.requested_by
                self.append(info)
                # print(info)
                # print("=========================", len(self))

    def get_embed(self, video):
        """Makes an embed out of this Video's information."""
        embed = discord.Embed(
            title=video["title"], description=video["uploader"] if "uploader" in video else "", url=video["webpage_url"])
        embed.set_footer(
            text=f"Requested by {self.requested_by.name}",
            icon_url=self.requested_by.avatar_url)
        thumbnail = video["thumbnail"] if "thumbnail" in video else None
        if thumbnail:
            embed.set_thumbnail(url=thumbnail)
        return embed