import os
import random
import smtplib
import ssl
import pandas as pd

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import make_msgid
from typing import *

from src.config import *
from src.utils.logger import logger

load_dotenv()
random.seed(42)
RECIPIENTS = ['kqureshi@mit.edu']
CSS = """\
<style>
body {
  padding-right: 15px;
  padding-left: 15px;
  margin-right: auto;
  margin-left: auto;
  font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji","Segoe UI Symbol";
}
@media (min-width: 768px) {
  body {
    width: 750px;
  }
}
@media (min-width: 992px) {
  body {
    width: 970px;
  }
}
@media (min-width: 1200px) {
  body {
    width: 1170px;
  }
}
h2 {
  text-align: left;
  font-size: 150%;
}
table { 
  margin-left: 0;
  margin-right: auto;
  margin-bottom: 3px;
}
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}
th, td {
  padding: 5px;
  text-align: center;
  font-size: 100%;
}
table tbody tr:hover {
  background-color: #dddddd;
}
p {
  font-size: 100%
}
.wide {
  width: 90%; 
}
</style>
"""


class Email:

    def __init__(self,
                 subject: str = '',
                 sender: str = EMAIL_USER,
                 pw: str = EMAIL_PW,
                 recipients: Optional[List[str]] = None) -> None:
        self.sender = sender
        self.pw = pw
        self.recipients = recipients or RECIPIENTS
        self.message = MIMEMultipart()
        self.message['Subject'] = subject
        self.message['From'] = self.sender
        self.message['To'] = ', '.join(self.recipients)

    def add_title(self, title: str) -> None:
        html = f"""\
        <html>
          <head>{CSS}</head>
          <body>
            <h1>{title}</h1>
          </body>
        </html>
        """
        self.message.attach(MIMEText(html, 'html'))

    def add_text(self, text: str) -> None:
        html = f"""\
        <html>
          <head>{CSS}</head>
          <body>
            <p>{text}</p>
          </body>
        </html>
        """
        self.message.attach(MIMEText(html, 'html'))

    def add_details(self, caption: str = '', **kwargs: Any) -> None:
        elements = [f"""
        <tr>
            <th style="text-align: left;">{k}</th>
            <td style="text-align: left;">{v}</td>
        </tr>
        """ for k, v in kwargs.items()]
        html = f"""\
        <html>
          <head>{CSS}</head>
          <body>
            <h2>{caption}</h2>
            <table>{''.join(elements)}</table>
          </body>
        </html>
        """
        self.message.attach(MIMEText(html, 'html'))

    def add_dataframe(self, df: pd.DataFrame, caption: str = '') -> None:
        if df is None or df.empty:
            return
        df = df.copy()
        df.index.name = None
        html = f"""\
        <html>
          <head>{CSS}</head>
          <body>
            <h2>{caption}</h2>
            {df.to_html()}
          </body>
        </html>
        """
        self.message.attach(MIMEText(html, 'html'))

    def add_image(self, filename: str, caption: str = '', subtype='png') -> None:
        cid = make_msgid()
        # Note cid here needs to strip the `<` and `>`.
        self.message.attach(MIMEText(f"""\
        <html>
          <head>{CSS}</head>
            <body>
              <h2>{caption}</h2>
              <img src="cid:{cid[1:-1]}">
            </body>
        </html>
        """, 'html'))
        with open(filename, "rb") as f:
            image = MIMEImage(f.read(), _subtype=subtype)
        image.add_header('Content-ID', cid)
        self.message.attach(image)

    def send(self) -> None:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
            server.login(self.sender, self.pw)
            for recipient in self.recipients:
                server.sendmail(self.sender, recipient, self.message.as_string())
        logger.info(f'Email sent successfully')
