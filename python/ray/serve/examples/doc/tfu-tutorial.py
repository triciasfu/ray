import sys
import requests
import os
import json

from transformers import pipeline

bearer_token = os.environ.get("BEARER_TOKEN")

def main():

	### example command for wikipedia python serve.py wiki https://en.wikipedia.org/wiki/Oreo
	### example command for twitter python serve.py twitter https://twitter.com/ChrisBHaynes/status/1422327596826587140
	
	# get passed in url
	url = str(sys.argv[2])
	print("URL: " + url)

	wiki_or_tweet = str(sys.argv[1])

	if wiki_or_tweet == "wiki":
		text = fetch_wiki_text(url)
	elif wiki_or_tweet == "twitter":
		text = fetch_tweet_text(url)

	summarize_text(text)


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

def summarize_text(text):
	summarizer = pipeline("summarization")
	summary_text = summarizer(text)[0]["summary_text"]
	print("Summary: " + summary_text)
	return summary_text


def bearer_oauth(r):
	r.headers["Authorization"] = f"Bearer {bearer_token}"
	return r


def fetch_wiki_text(url):
	split_url = url.split("/")
	wiki_title = split_url[4]
	response = requests.get('https://en.wikipedia.org/w/api.php', 
		params={
			'action': 'query',
			'format': 'json',
			'titles':  wiki_title,
			'prop': 'extracts',
			'exintro': True,
			'explaintext': True,
		}
	).json()
	page = next(iter(response['query']['pages'].values()))
	print("Wiki Text: " + page['extract'])
	return page['extract']


if __name__ == "__main__":
	main()


