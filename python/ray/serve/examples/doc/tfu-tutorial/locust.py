import time
from locust import HttpUser, task, between

class QuickstartUser(HttpUser):
    wait_time = between(0.5, 1)

    @task
    def call_summarizer(self):
        self.client.get("/serve/summarize/?type=wiki&url=https://en.wikipedia.org/wiki/Oreo")
        
