import requests
from datetime import datetime, timedelta
import logging
import configparser

parser =configparser.ConfigParser(interpolation=None)
parser.read('api.conf')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

TOKEN = parser.get('API Conf', 'token')
headers = {'Authorization': f'token {TOKEN}'}

def fetch_repos():
    six_months_ago = datetime.now() - timedelta(weeks=50)
    date_str = six_months_ago.strftime('%Y-%m-%d')


    url = f"https://api.github.com/search/repositories?q=created:>{date_str}+language:python&sort=stars&order=desc"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()['items']
    else:
        print("Error fetching repositories", response.status_code)
        return []

def fetch_issues(repo):
    issues_url = repo['issues_url'].replace('{/number}', '?state=open')
    response = requests.get(issues_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching issues for {repo['name']}: {response.status_code}")
        return []

def main():
    repos = fetch_repos()
    for repo in repos:
        issues = fetch_issues(repo)
        for issue in issues:
            if 'good first issue' in [label['name'] for label in issue['labels']]:
                print(f"Repository: {repo['name']}, Issue: {issue['title']}, URL: {issue['html_url']}")

if __name__ == '__main__':
    main()