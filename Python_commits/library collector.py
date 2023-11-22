import requests
from datetime import datetime, timedelta

# Calculate the date 3 weeks ago from today
three_weeks_ago = datetime.now() - timedelta(weeks=10)
date_str = three_weeks_ago.strftime('%Y-%m-%d')

# Corrected URL
url = f"https://api.github.com/search/repositories?q=stars:<20+created:>{date_str}+language:python&sort=stars&order=desc"

# Make the request to the GitHub API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    repos = response.json()

    # Print the names and details of the repositories
    for repo in repos['items']:
        print(f"Name: {repo['name']}, URL: {repo['html_url']}, Stars: {repo['stargazers_count']}")
else:
    print("Error fetching repositories:", response.status_code)
