import os
import yaml
import sys
local_path = os.environ["APP_PATH"]
sys.path.append(local_path.strip() + "/src")

with open(local_path + "/config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

from messenger import WhatsappMessenger
from database import AppLogger
import json

app_logger = AppLogger()
messenger = WhatsappMessenger(config, app_logger)

asha_video_path = os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'], 'videos/asha.mp4')
anm_video_path = os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'], 'videos/anm.mp4')

asha_video_id = messenger.upload_video(asha_video_path)['id']
anm_video_id = messenger.upload_video(anm_video_path)['id']

print(asha_video_id, anm_video_id)

video_json = {
    'asha': asha_video_id,
    'anm': anm_video_id
}

with open(os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'], 'videos/video_ids.json'), 'w') as f:
    f.write(json.dumps(video_json))