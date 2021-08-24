import sys
import requests
import os
import json
import ray
import time

from transformers import pipeline
from fastapi import FastAPI
from ray import serve

ray.client("anyscale://tfu-tutorial").cluster_env("tfu-tutorial-cluster-env").namespace("summarizer").job_name("tfu-test").connect()
# ray.init(address="auto", namespace="summarizer")
serve.start(detached=True)

app = FastAPI()
bearer_token = os.environ.get("BEARER_TOKEN")

def bearer_oauth(r):
	r.headers["Authorization"] = f"Bearer {bearer_token}"
	return r

def summarize_text(text):
	summarizer = pipeline("summarization")
	summary_text = summarizer(text)[0]["summary_text"]
	print("Summary: " + summary_text)
	# print("--- %s seconds ---" % (time.time() - start_time))
	return summary_text

def fetch_wiki_text(wiki_title):
	split_url = wiki_title.split("/")
	wiki_title = split_url[4]
	response = requests.get('https://en.wikipedia.org/w/api.php', 
		params={
			'action': 'query',
			'format': 'json',
			'titles':  wiki_title,
			'prop': 'extracts',
			'exintro': True,
			# 'exsectionformat': 'plain',
			'explaintext': True,

		}
	).json()
	page = next(iter(response['query']['pages'].values()))
	print("Wiki Text: " + page['extract'])
	return page['extract']

def fetch_tweet_text(url):
	# get tweet id
	split_url = url.split("/")
	tweet_id = split_url[5]

	# make api url 
	api_url = "https://api.twitter.com/2/tweets/" + tweet_id
	
	# get tweet text 
	response = requests.get(api_url, auth=bearer_oauth)
	response_json = response.json()
	text = response_json['data']['text']
	
	print("Tweet text: " + text)
	return text

@serve.deployment(route_prefix="/summarize", num_replicas=16)
@serve.ingress(app)
class SummarizeURLDeployment:
	@app.get("/")
	def summarize_wiki_or_twitter(self, type: str, url: str):
		start_time = time.time()
		if type == "wiki":
			text = fetch_wiki_text(url)
		elif type == "twitter":
			text = fetch_tweet_text(url)
		return summarize_text(text)

SummarizeURLDeployment.deploy()

#curl -X GET localhost:8000/summarize/\?type\=wiki\&url\=https://en.wikipedia.org/wiki/Oreo
#curl -X GET localhost:8000/summarize/\?type\=twitter\&url\=https://twitter.com/StarbucksNews/status/1422935361857064969

#curl -X GET https://session-z7rwazgpsahewnaawvf7e4cr.i.anyscaleuserdata-staging.com/serve/summarize/\?type\=wiki\&url\=https://en.wikipedia.org/wiki/Oreo
#curl -X GET https://session-z7rwazgpsahewnaawvf7e4cr.i.anyscaleuserdata-staging.com/serve/summarize/\?type\=twitter\&url\=https://twitter.com/StarbucksNews/status/1422935361857064969