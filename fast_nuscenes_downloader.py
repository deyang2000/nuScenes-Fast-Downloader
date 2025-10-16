import os, json, math, hashlib, tarfile, gzip, shutil, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

OUTPUT_DIR = "/mnt/data1/nuscenes-download"
REGION = os.environ.get("NUSC_REGION", "asia")   # 'us' or 'asia'
MAX_FILE_CONCURRENCY = 3                         
RANGE_THREADS = 8                                 
CHUNK_SIZE = 8 * 1024 * 1024                      
TIMEOUT = 30
VERIFY_SSL = True

DOWNLOAD_FILES = {
    "v1.0-test_meta.tgz":"b0263f5c41b780a5a10ede2da99539eb",
    "v1.0-test_blobs.tgz":"e065445b6019ecc15c70ad9d99c47b33",
    "v1.0-trainval01_blobs.tgz":"cbf32d2ea6996fc599b32f724e7ce8f2",
    "v1.0-trainval02_blobs.tgz":"aeecea4878ec3831d316b382bb2f72da",
    "v1.0-trainval03_blobs.tgz":"595c29528351060f94c935e3aaf7b995",
    "v1.0-trainval04_blobs.tgz":"b55eae9b4aa786b478858a3fc92fb72d",
    "v1.0-trainval05_blobs.tgz":"1c815ed607a11be7446dcd4ba0e71ed0",
    "v1.0-trainval06_blobs.tgz":"7273eeea36e712be290472859063a678",
    "v1.0-trainval07_blobs.tgz":"46674d2b2b852b7a857d2c9a87fc755f",
    "v1.0-trainval08_blobs.tgz":"37524bd4edee2ab99678909334313adf",
    "v1.0-trainval09_blobs.tgz":"a7fcd6d9c0934e4052005aa0b84615c0",
    "v1.0-trainval10_blobs.tgz":"31e795f2c13f62533c727119b822d739",
    "v1.0-trainval_meta.tgz":"537d3954ec34e5bcb89a35d4f6fb0d4a",
}

COGNITO_CLIENT_ID = "7fq5jvs5ffs1c50hd3toobb3b9"

def get_env_cred():
    email = os.environ.get("NUSC_EMAIL")
    password = os.environ.get("NUSC_PASS")

    if not email or not password:
        print("Please export NUSC_EMAIL=... ; export NUSC_PASS=...")
        sys.exit(1)
    return email, password

def requests_session():
    sess = requests.Session()
    retries = Retry(
        total=8, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET","POST"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_maxsize=MAX_FILE_CONCURRENCY*RANGE_THREADS)
    sess.mount("https://", adapter); sess.mount("http://", adapter)
    sess.headers.update({"User-Agent":"nuScenes-fastdownloader/1.0"})
    return sess

def login(sess, username, password):
    url = "https://cognito-idp.us-east-1.amazonaws.com/"
    headers = {
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
    }
    data = {
        "AuthFlow": "USER_PASSWORD_AUTH",
        "ClientId": COGNITO_CLIENT_ID,
        "AuthParameters": {"USERNAME": username, "PASSWORD": password},
        "ClientMetadata": {}
    }
    r = sess.post(url, headers=headers, data=json.dumps(data), timeout=TIMEOUT, verify=VERIFY_SSL)
    r.raise_for_status()
    j = r.json()
    return j["AuthenticationResult"]["IdToken"]

def get_urls(sess, token):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type':'application/json'}
    result = {}
    for filename, md5 in DOWNLOAD_FILES.items():
        api = f'https://o9k5xn5546.execute-api.us-east-1.amazonaws.com/v1/archives/v1.0/{filename}?region={REGION}&project=nuScenes'
        rr = sess.get(api, headers=headers, timeout=TIMEOUT, verify=VERIFY_SSL)
        rr.raise_for_status()
        result[filename] = (rr.json()['url'], md5)
    return result

def has_aria2c():
    return shutil.which("aria2c") is not None

def aria2c_download(url, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    cmd = [
        "aria2c", "-x16", "-s16", "-k1M", "-c",
        "-o", os.path.basename(out_path),
        "-d", os.path.dirname(out_path),
        url
    ]
    print("[aria2c]", " ".join(cmd))
    subprocess.run(cmd, check=True)

def get_content_length(sess, url):
    rr = sess.head(url, timeout=TIMEOUT, allow_redirects=True, verify=VERIFY_SSL)
    if rr.status_code >= 400:
        rr = sess.get(url, headers={"Range":"bytes=0-0"}, timeout=TIMEOUT, stream=True, verify=VERIFY_SSL)
    rr.raise_for_status()
    cl = rr.headers.get("Content-Length")
    if (not cl) or cl == "1":
        rr2 = sess.get(url, stream=True, timeout=TIMEOUT, verify=VERIFY_SSL)
        rr2.raise_for_status()
        cl = rr2.headers.get("Content-Length")
        rr2.close()
    return int(cl) if cl else None

def part_path(save_path, idx):
    return f"{save_path}.part{idx:03d}"

def download_range_part(sess, url, start, end, pth):
    headers = {"Range": f"bytes={start}-{end}"}
    existing = 0
    if os.path.exists(pth):
        existing = os.path.getsize(pth)
        seg_len = end - start + 1
        if existing >= seg_len:
            return  
        headers["Range"] = f"bytes={start+existing}-{end}"
    with sess.get(url, headers=headers, stream=True, timeout=TIMEOUT, verify=VERIFY_SSL) as r:
        r.raise_for_status()
        mode = "ab" if existing > 0 else "wb"
        with open(pth, mode, buffering=CHUNK_SIZE) as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)

def merge_parts(save_path, total_parts):
    with open(save_path, "wb", buffering=CHUNK_SIZE) as out:
        for i in range(total_parts):
            p = part_path(save_path, i)
            with open(p, "rb", buffering=CHUNK_SIZE) as f:
                shutil.copyfileobj(f, out, length=CHUNK_SIZE)

    for i in range(total_parts):
        pp = part_path(save_path, i)
        if os.path.exists(pp):
            os.remove(pp)

def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb", buffering=CHUNK_SIZE) as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()

def fast_extract_tgz(tgz_path, out_dir):

    if shutil.which("tar") and shutil.which("pigz"):
        print(f"[extract] tar -I pigz -xf {tgz_path} -C {out_dir}")
        subprocess.run(["tar","-I","pigz","-xf",tgz_path,"-C",out_dir], check=True)
        return

    if shutil.which("tar"):
        print(f"[extract] tar -xf {tgz_path} -C {out_dir}")
        subprocess.run(["tar","-xf",tgz_path,"-C",out_dir], check=True)
        return

    print("[extract] (python) extracting...", tgz_path)
    with gzip.open(tgz_path, 'rb') as f_in:
        with tarfile.open(fileobj=f_in, mode='r:*') as tar:
            tar.extractall(out_dir)

def fast_extract_tar(tar_path, out_dir):
    if shutil.which("tar"):
        subprocess.run(["tar","-xf",tar_path,"-C",out_dir], check=True)
        return
    with tarfile.open(tar_path, 'r:*') as tar:
        tar.extractall(out_dir)

def download_one(sess, url, save_path, md5_expected):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    if has_aria2c():
        aria2c_download(url, save_path)
    else:
        size = get_content_length(sess, url)
        if size is None:

            with sess.get(url, stream=True, timeout=TIMEOUT, verify=VERIFY_SSL) as r:
                r.raise_for_status()
                with open(save_path, "wb", buffering=CHUNK_SIZE) as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk: f.write(chunk)
        else:

            part_len = max(size // RANGE_THREADS, 1)
            ranges = []
            for i in range(RANGE_THREADS):
                start = i * part_len
                end = (size - 1) if i == RANGE_THREADS - 1 else (start + part_len - 1)
                ranges.append((i, start, end))
            with ThreadPoolExecutor(max_workers=RANGE_THREADS) as ex:
                futs = []
                for i, s, e in ranges:
                    pth = part_path(save_path, i)
                    futs.append(ex.submit(download_range_part, sess, url, s, e, pth))
                for fu in as_completed(futs):
                    fu.result()
            merge_parts(save_path, RANGE_THREADS)


    h = md5sum(save_path)
    if h != md5_expected:
        raise RuntimeError(f"MD5 mismatch for {os.path.basename(save_path)}: got {h}, expect {md5_expected}")
    print(f"[OK] {os.path.basename(save_path)} MD5 verified.")

def main():
    email, password = get_env_cred()
    sess = requests_session()
    print("Login…")
    token = login(sess, email, password)
    print("Fetch URLs…")
    url_map = get_urls(sess, token)

    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_FILE_CONCURRENCY) as pool:
        for fname, (url, md5exp) in url_map.items():
            save_path = os.path.join(OUTPUT_DIR, fname)
            tasks.append(pool.submit(download_one, sess, url, save_path, md5exp))
        for t in as_completed(tasks):
            t.result()
    print("All downloads done.")


    print("Extracting…")
    for fname in url_map.keys():
        fpath = os.path.join(OUTPUT_DIR, fname)
        if fname.endswith(".tgz"):
            fast_extract_tgz(fpath, os.path.dirname(fpath))
        elif fname.endswith(".tar"):
            fast_extract_tar(fpath, os.path.dirname(fpath))
        else:
            print("Skip (unknown type):", fname)

    print("Done.")

if __name__ == "__main__":
    main()
