# pip install accelerate
from transformers import AutoProcessor, Gemma3ForConditionalGeneration
from PIL import Image
import requests
import torch
import os
import json
import pandas as pd

# 모델 로드
model_id = "google/gemma-3-4b-it"
model = Gemma3ForConditionalGeneration.from_pretrained(model_id, device_map="auto").eval()
processor = AutoProcessor.from_pretrained(model_id)


# 결과 저장용 리스트
results = []

# 프롬프트 디렉토리 설정
prompt_dir = "./prompts"
prompt_files = sorted([f for f in os.listdir(prompt_dir) if f.endswith(".json")])

# 반복 처리
for idx, fname in enumerate(prompt_files):

    print(f"[{idx+1}/{len(prompt_files)}] Processing: {fname}")

    with open(os.path.join(prompt_dir, fname), "r") as f:
        messages = json.load(f)

    inputs = processor.apply_chat_template(
        messages,
        add_generation_prompt=True,
        tokenize=True,
        return_dict=True,
        return_tensors="pt"
    )
    

    input_len = inputs["input_ids"].shape[-1]

    with torch.inference_mode():
        inputs = {k: v.to('cuda') for k, v in inputs.items()}
        print(f"Processing {fname}: input_ids shape = {inputs['input_ids'].shape}")
        if inputs['input_ids'].shape[1] > 2000:
            continue
        generation = model.generate(**inputs, max_new_tokens=8, do_sample=False)
        generation = generation[0][input_len:]

    decoded = processor.decode(generation, skip_special_tokens=True)
    print(f"{idx}: {decoded}")

    results.append({
        "file": fname,
        "output": decoded
    })

    # 중간 결과 저장
    pd.DataFrame(results).to_csv("generation_results.csv", index=False)

print("완료: generation_results.csv 저장됨")