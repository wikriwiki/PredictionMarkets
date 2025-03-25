import yt_dlp
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# 다운로드 디렉토리 설정
download_dir = "vimeo_videos"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# 다운로드 옵션 설정
ydl_opts = {
    'format': 'bestvideo+bestaudio/best',  # 최고 품질
    'outtmpl': f'{download_dir}/%(title)s_%(id)s.%(ext)s',  # 파일명 형식
    'verbose': True,  # 자세한 로그
    'merge_output_format': 'mp4',  # 병합 후 MP4로 출력
    'noplaylist': True,  # 플레이리스트 무시
    'quiet': False,  # 로그 출력 유지
}

# titles_videos.csv에서 URL 읽기
video_urls = []
with open('titles_videos.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        video_urls.append(row['Video Link'])
video_urls = video_urls[:5]  # 테스트용 5개

# 다운로드 함수 정의
def download_video(video_url):
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        return f"다운로드 완료: {video_url}"
    except Exception as e:
        return f"다운로드 실패: {video_url}, 오류: {e}"

# 병렬 다운로드 실행
max_workers = min(4, os.cpu_count() or 1)  # CPU 코어 수나 4 중 작은 값
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # 작업 제출
    future_to_url = {executor.submit(download_video, url): url for url in video_urls}
    # 결과 처리
    for future in as_completed(future_to_url):
        result = future.result()
        print(result)

print(f"총 {len(video_urls)}개 영상 처리 완료")