import json
import requests
import time

API_KEY = "AIzaSyDcickPwHuReLq0IjELaWjmbcIAc60PxmY"

URL = (
    "https://generativelanguage.googleapis.com/v1beta/"
    f"models/gemini-1.5-flash-latest:generateContent?key={API_KEY}"
)

with open("job_list.json", "r", encoding="utf-8") as f:
    jobs = json.load(f)

results = []

for idx, job_title in enumerate(jobs, start=1):

    prompt = f"""
You are an HR and industry expert.
List the most relevant technical and professional skills for the job below.
Return ONLY a valid JSON array of strings.
No explanations. No markdown.

Job title: {job_title}
"""

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ]
    }

    try:
        response = requests.post(URL, json=payload, timeout=30)
        response.raise_for_status()

        text = response.json()["candidates"][0]["content"]["parts"][0]["text"]
        clean = text.strip().replace("```json", "").replace("```", "")
        skills = json.loads(clean)

    except Exception as e:
        print(f"❌ Xato job: {job_title} → {e}")
        skills = []

    results.append({
        "job_id": idx,
        "job_title": job_title,
        "skills": skills
    })

    print(f"✅ {job_title} → {len(skills)} skills")
    time.sleep(1)

with open("job_with_skills.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print("\n🔥 HAMMASI ISHLADI — job_with_skills.json tayyor")
