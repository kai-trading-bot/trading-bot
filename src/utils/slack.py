import tenacity

from typing import *

from slack import WebClient
from slack.errors import SlackApiError

from src.config import SLACK_API_TOKEN
from src.utils.logger import logger

BLUE = "#3498db"
GREEN = "#27ae60"
RED = "#e74c3c"
SILVER = "#bdc3c7"


def build_attachment(title: str, color: str, text: str = "", **kwargs: Any) -> Dict:
    fields = [{'title': k, 'value': str(v), 'short': len(str(v)) < 15} for k, v in kwargs.items() if v]
    attachment = {'title': title, 'color': color, 'text': text, 'fields': fields}
    return attachment


class Slack:

    def __init__(self,
                 channel: str,
                 run_async: bool = True,
                 icon_emoji: Optional[str] = None,
                 username: Optional[str] = None) -> None:
        self.client: WebClient = WebClient(token=SLACK_API_TOKEN, run_async=run_async)
        self.channel = channel
        self.icon_emoji = icon_emoji  # the icon emoji of the slack bot. Example :alpaca:
        self.username = username  # the display username of the Slack bot.

    def __str__(self) -> str:
        return f'Slack(channel={self.channel}, username={self.username})'

    def __repr__(self) -> str:
        return self.__str__()

    async def send_text(self, text: str, **kwargs: Any) -> Optional[Dict]:
        return await self.post_message(text=text, **kwargs)

    # TODO: Deprecate this.
    async def text(self, text: str = "", **kwargs) -> Dict:
        return await self.send_text(text, **kwargs)

    async def success(self, title: str, text: str = "", **kwargs: Any) -> Dict:
        return await self.send(title, text, color=GREEN, **kwargs)

    async def info(self, title: str, text: str = "", **kwargs: Any) -> Dict:
        return await self.send(title, text, color=BLUE, **kwargs)

    async def error(self, title: str, text: str = "", **kwargs) -> Dict:
        return await self.send(title, text, color=RED, **kwargs)

    async def send_attachment(self, attachment: Dict, **kwargs: Any) -> Optional[Dict]:
        return await self.post_message(attachemnts=[attachment], **kwargs)

    async def send_image(self, filename: str, **kwargs: Any) -> Optional[Dict]:
        return await self.upload(filename, **kwargs)

    async def send(self, title: str, text: str = "", success: bool = True, color: Optional[str] = None,
                   **kwargs: Any) -> Dict:
        """ Send a Slack message with attachment (color).
        E.g.
            send("Execution Report", success=True, Date='2021-01-01', Notional=10000, ...)
        """
        color = color if color is not None else GREEN if success else RED
        attachment = build_attachment(title=title, text=text, color=color, **kwargs)
        return await self.post_message(attachments=[attachment])

    @tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def post_message(self, **kwargs: Any) -> Optional[Dict]:
        try:
            response = await self.client.chat_postMessage(
                channel=self.channel, icon_emoji=self.icon_emoji, username=self.username, **kwargs)
            assert response["ok"]
            return response
        except SlackApiError as e:
            logger.error(f"Slack API Error: {e}")

    @tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def upload(self, file: str, **kwargs: Any) -> Optional[Dict]:
        try:
            filename = file.split('/')[-1]
            logger.info(f'Uploading Slack {file}. Filename={filename}')
            return await self.client.files_upload(channels=self.channel, file=file, filename=filename, **kwargs)
        except SlackApiError as e:
            logger.error(f'Slack upload file error: {e}')
