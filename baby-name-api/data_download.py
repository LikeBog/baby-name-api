import urllib.request
import zipfile
import os

def download_ssa_data():
    """Download SSA baby names data"""
    url = "https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-national-data"
    print("Please download the data from:", url)
    print("Extract it to the 'babynames' folder")

if __name__ == "__main__":
    download_ssa_data()
