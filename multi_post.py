import concurrent.futures
from tiktokautouploader import upload_tiktok
from tiktokautouploader.function import FORCE_STOP, stop_all_uploads

# List of 5 videos to upload at the same time
videos_to_upload = [
    {"video": r'C:\Users\GML_BENSER\Downloads\generated_video (16).mp4', "desc": "Parallel Upload 1 #fun"},
    {"video": r'C:\Users\GML_BENSER\Downloads\generated_video (30).mp4', "desc": "Parallel Upload 2 #viral"},
    {"video": r'C:\Users\GML_BENSER\Downloads\generated_video (10).mp4', "desc": "Parallel Upload 3 #tiktok"},
    {"video": r'C:\Users\GML_BENSER\Downloads\generated_video (12).mp4', "desc": "Parallel Upload 4 #awesome"},
    {"video": r'C:\Users\GML_BENSER\Downloads\generated_video (8).mp4', "desc": "Parallel Upload 5 #trending"},
]

def run_upload(video_data):
    if FORCE_STOP:
        return f"⏹️ SKIPPED: {video_data['desc']} | User stopped session."
    try:
        # Each call launches a separate browser instance
        print(f"📂 Starting browser for: {video_data['desc']}")
        upload_tiktok(
            video=video_data["video"],
            description=video_data["desc"],
            accountname='jbnsrr.xx',
            hashtags=['#fun', '#viral'],
            headless=False,
            stealth=True, 
            schedule='03:10', day=20,
            sound_name='random',
            search_mode='favorites'
        )
        return f"✅ SUCCESS: {video_data['desc']}"
    except Exception as e:
        return f"❌ FAILED: {video_data['desc']} | Error: {str(e)}"

if __name__ == "__main__":
    print(f"🚀 Starting parallel upload of {len(videos_to_upload)} videos...")
    
    # Use 5 workers to run them literally at the same time
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(run_upload, videos_to_upload))
        
    print("\n--- Summary ---")
    for res in results:
        print(res)
