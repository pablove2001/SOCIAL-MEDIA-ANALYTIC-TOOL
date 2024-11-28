from datetime import datetime

class User:
    def __init__(self, username:str, password:str):
        self.username = username
        self.password = password
        self.creation_date = datetime.now()
        self.notifications = "on" # on of of
        self.language = "ENG" # ENG or ESP

    def to_dict(self):
        return {
            "username": self.username,
            "password": self.password,
            "creation_date": self.creation_date,
            "notifications": self.notifications,
            "language": self.language,
        }

class Post:
    def __init__(self, title:str, text:str, username:str):
        self.title = title
        self.text = text
        self.username = username
        self.creation_date = datetime.now()

    def to_dict(self):
        return {
            "title": self.title,
            "text": self.text,
            "username": self.username,
            "creation_date": self.creation_date
        }