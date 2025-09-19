from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import urllib.request
import urllib.error
import threading
import logging
import hashlib
import shutil
import time
import json
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Thread %(threadName)s - %(message)s')

def get_expected_hashes_from_api(repo_owner, repo_name, release_tag):
	"""Fetch expected SHA256 hashes directly from GitHub API release assets."""
	api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/releases/tags/{release_tag}"
	try:
		with urllib.request.urlopen(api_url, timeout=30) as response:
			release_info = json.loads(response.read().decode())
		
		expected_hashes = {}
		for asset in release_info.get('assets', []):
			filename = asset.get('name', '')
			if filename.endswith('.rpm'):
				digest = asset.get('digest', '')
				if digest.startswith('sha256:'):
					hash_value = digest[7:].lower()
					expected_hashes[filename] = hash_value
					logging.info(f"Fetched hash for {filename}: {hash_value[:16]}...")
				else:
					logging.warning(f"No valid SHA256 digest found for {filename}: {digest}")
		
		if not expected_hashes:
			logging.warning("No RPM assets with SHA256 digests found in release.")
			logging.info(f"Available assets: {[a['name'] for a in release_info.get('assets', [])]}")
		else:
			logging.info(f"Fetched {len(expected_hashes)} expected hashes from API.")
		return expected_hashes
	except urllib.error.URLError as e:
		logging.error(f"Failed to fetch release assets from API: {e}")
		raise
	except json.JSONDecodeError as e:
		logging.error(f"Failed to parse API JSON: {e}")
		raise
	except Exception as e:
		logging.error(f"Unexpected error fetching hashes from API: {e}")
		raise

def compute_sha256(file_path):
	"""Compute SHA256 hash of a file."""
	sha256 = hashlib.sha256()
	try:
		with open(file_path, 'rb') as file:
			while True:
				chunk = file.read(8192)
				if not chunk:
					break
				sha256.update(chunk)
		return sha256.hexdigest().lower()
	except IOError as e:
		logging.error(f"Failed to compute hash for {file_path}: {e}")
		return None

def download_chunk(url, save_path, start, end, total_size, progress_bar, lock):
	try:
		req = urllib.request.Request(url)
		req.add_header('Range', f'bytes={start}-{end}')
		with urllib.request.urlopen(req, timeout=30) as response:
			downloaded = 0
			chunk_size = 8192
			while True:
				chunk = response.read(chunk_size)
				if not chunk:
					break
				downloaded += len(chunk)
				with lock:
					with open(save_path, 'r+b') as file:
						file.seek(start + downloaded - len(chunk))
						file.write(chunk)
					progress_bar.update(len(chunk))
		logging.info(f"Completed range {start}-{end}")
	except urllib.error.HTTPError as http_err:
		logging.error(f"HTTP error in range {start}-{end}: {http_err}")
	except urllib.error.URLError as url_err:
		logging.error(f"URL error in range {start}-{end}: {url_err}")
	except IOError as io_err:
		logging.error(f"File error in range {start}-{end}: {io_err}")
	except Exception as e:
		logging.error(f"Unexpected error in range {start}-{end}: {e}")

def download_file(url, save_path, expected_hashes, num_threads=4):
	try:
		with urllib.request.urlopen(url, timeout=30) as response:
			total_size = int(response.getheader('Content-Length', 0))
			if total_size == 0:
				raise ValueError("Could not determine file size")
		logging.info(f"Total file size: {total_size} bytes")

		os.makedirs(os.path.dirname(save_path), exist_ok=True)
		with open(save_path, 'wb') as file:
			file.truncate(total_size)

		progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(save_path), mininterval=0.1)
		lock = threading.Lock()

		chunk_size = total_size // num_threads
		ranges = [(i * chunk_size, (i + 1) * chunk_size - 1 if i < num_threads - 1 else total_size - 1)
				  for i in range(num_threads)]

		start_time = time.time()
		with ThreadPoolExecutor(max_workers=num_threads) as executor:
			futures = [executor.submit(download_chunk, url, save_path, start, end, total_size, progress_bar, lock)
					   for start, end in ranges]
			for future in futures:
				future.result()
		end_time = time.time()

		progress_bar.close()
		print(f"File downloaded successfully and saved to {save_path}")
		logging.info(f"Download completed in {end_time - start_time:.2f} seconds")

		filename = os.path.basename(save_path)
		if filename not in expected_hashes:
			raise Exception(f"No expected hash found for {filename} in API response. Available: {list(expected_hashes.keys())}")
		expected_hash = expected_hashes[filename]
		computed_hash = compute_sha256(save_path)
		if computed_hash is None:
			raise Exception(f"Hash computation failed for {save_path}")
		if computed_hash != expected_hash:
			raise Exception(f"Hash mismatch for {save_path}: expected {expected_hash}, got {computed_hash}")
		print(f"Hash verified for {filename}: {computed_hash}")

	except urllib.error.HTTPError as http_err:
		logging.error(f"HTTP error occurred: {http_err}")
		raise
	except urllib.error.URLError as url_err:
		logging.error(f"URL error occurred: {url_err}")
		raise
	except IOError as io_err:
		logging.error(f"File error occurred while saving: {io_err}")
		raise
	except Exception as e:
		logging.error(f"An unexpected error occurred: {e}")
		raise

repo_owner = "Reyher-VDI"
repo_name = "Citrix-RPM-Repository"
release_tag = "TEST"

expected_hashes = get_expected_hashes_from_api(repo_owner, repo_name, release_tag)
if not expected_hashes:
	raise Exception("Failed to auto-fetch expected hashes from GitHub API. Check logs for details.")

for rpm_file in ["/temp/packages/ctxusb.rpm", "/temp/packages/ICAClient.rpm"]:
	if os.path.exists(rpm_file):
		os.remove(rpm_file)

download_file("https://github.com/Reyher-VDI/Citrix-RPM-Repository/releases/download/TEST/ctxusb.rpm", "/temp/packages/ctxusb.rpm", expected_hashes)
download_file("https://github.com/Reyher-VDI/Citrix-RPM-Repository/releases/download/TEST/ICAClient.rpm", "/temp/packages/ICAClient.rpm", expected_hashes)

os.makedirs("/var/local/citrix-repo")

logging.info("Copying ctxusb.rpm...")
shutil.copyfile("/temp/packages/ctxusb.rpm", "/var/local/citrix-repo")
logging.info("Finished copying ICAClient.rpm")
logging.info("Copying ICAClient.rpm...")
shutil.copyfile("/temp/packages/ICAClient.rpm", "/var/local/citrix-repo")
logging.info("Finished copying ICAClient.rpm")

os.system("createrepo_c /var/local/citrix-repo")

shutil.copyfile("./local-citrix-repo.repo", "/etc/yum.repos.d/")

print("All files downloaded and verified successfully!")
