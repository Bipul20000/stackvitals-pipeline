import requests
import pandas as pd
from datetime import datetime

def fetch_npm_registry(package_name):
    url = f"https://registry.npmjs.org/{package_name}/latest"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        repo_url = data.get("repository", {}).get("url", "")
        
        return {
            "Package_Name": data.get("name"),
            "Version": data.get("version"),
            "License": data.get("license"),
            "Dependencies_Count": len(data.get("dependencies", {})),
            "Repository_URL": repo_url
        }
    return {}

def fetch_bundlephobia(package_name):
    url = f"https://bundlephobia.com/api/size?package={package_name}"
    response = requests.get(url, headers={'User-Agent': 'StackVitals-Bot/1.0'})
    
    if response.status_code == 200:
        data = response.json()
        return {
            "Minified_Size_Bytes": data.get("size", 0),
            "Gzipped_Size_Bytes": data.get("gzip", 0),
            "Download_Time_3G_ms": data.get("dependencySizes", [{"approximateSize": 0}])[0].get("approximateSize", 0)
        }
    return {}

def fetch_github_health(repo_url):
    if not repo_url or "github.com" not in repo_url:
        return {}
        
    clean_url = repo_url.replace("git+", "").replace("git://", "https://").replace(".git", "")
    parts = clean_url.split("github.com/")[-1].split("/")
    
    if len(parts) >= 2:
        owner = parts[0]
        repo = parts[1]
        api_url = f"https://api.github.com/repos/{owner}/{repo}"
        
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            return {
                "Open_Issues": data.get("open_issues_count"),
                "Stars": data.get("stargazers_count"),
                "Forks": data.get("forks_count"),
                "Last_Commit_Date": data.get("updated_at")
            }
    return {}

def execute_pipeline():
    target_packages = ["lodash", "moment", "express", "react"]
    master_dataset = []

    for pkg in target_packages:
        print(f"Extracting data for: {pkg}")
        
        npm_data = fetch_npm_registry(pkg)
        size_data = fetch_bundlephobia(pkg)
        
        repo_link = npm_data.get("Repository_URL", "")
        github_data = fetch_github_health(repo_link)
        
        combined_record = {**npm_data, **size_data, **github_data}
        master_dataset.append(combined_record)

    df = pd.DataFrame(master_dataset)
    df.to_csv("stackvitals_raw_data.csv", index=False)
    print("\nExtraction Complete. Preview of Data:")
    print(df.head())

if __name__ == "__main__":
    execute_pipeline()