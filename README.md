# nuScenes Fast Downloader

High-speed, resumable downloader for the **nuScenes dataset**.  
Supports concurrent range downloads, automatic MD5 verification, and multi-core extraction.

---

## 🚀 How to Use

### 1. Register an Account
First, register an account at the official [nuScenes website](https://www.nuscenes.org/), and agree to the terms of use.

### 2. Install Required Libraries
Make sure you have Python ≥3.8 and then install:

pip install requests



### 3. Before running, ensure you have sufficient space to save files. Then, export your nuScenes credentials as environment variables.
export NUSC_EMAIL="your_mail"

export NUSC_PASS="your_password"

export NUSC_REGION="asia"   # or "us"

python fast_nuscenes_downloader.py




