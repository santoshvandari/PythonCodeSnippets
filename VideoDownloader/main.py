import yt_dlp
import os
class VideoDownloader:
    def __init__(self, save_path: str):
        self.save_path = save_path

    def download_video(self, url: str) -> str:
        """
        Downloads a video using yt_dlp and returns the absolute path.
        Raises DownloadError or Exception on failure.
        """
        outtmpl = os.path.join(self.save_path, "%(id)s.%(ext)s")
        opts = {
            "outtmpl": outtmpl,
            "format": "best",
            "noplaylist": True,
            "quiet": True,
        }

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return ydl.prepare_filename(info)
        
if __name__ == "__main__":
    downloader = VideoDownloader(save_path="./downloads")
    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # Example URL
    try:
        video_path = downloader.download_video(video_url)
        print(f"Video downloaded to: {video_path}")
    except Exception as e:
        print(f"An error occurred: {e}")