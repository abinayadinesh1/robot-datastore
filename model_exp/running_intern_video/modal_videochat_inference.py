# modal_videochat_inference.py

import base64
import io
import tempfile
import os
from typing import Any

import modal

# --- Image setup ---
videochat_image = (
    modal.Image.from_registry("nvidia/cuda:12.1.0-cudnn8-devel-ubuntu22.04", add_python="3.10")
    .entrypoint([])
    .apt_install("git", "ffmpeg", "libgl1", "libglib2.0-0", "gcc", "g++", "clang")
    .pip_install("wheel", "packaging", "ninja")
    .pip_install(
        "torch==2.1.2",
        "torchvision==0.16.2",
    )
    .run_commands(
        "pip install flash-attn==2.5.9.post1 --no-build-isolation "
        "--find-links https://github.com/Dao-AILab/flash-attention/releases/expanded_assets/v2.5.9.post1"
    )
    .pip_install(
        "transformers==4.40.1",
        "accelerate",
        "av",
        "imageio",
        "decord",
        "opencv-python-headless",
        "fastapi",
        "uvicorn[standard]",
        "python-multipart",
        "einops",
        "timm",
    )
    .run_commands("pip install 'numpy==1.26.4'")
)


MODEL_NAME = "OpenGVLab/VideoChat-Flash-Qwen2_5-7B_InternVideo2-1B"

hf_cache_vol = modal.Volume.from_name("huggingface-cache", create_if_missing=True)

app = modal.App("videochat-flash-inference")

MINUTES = 60
API_PORT = 8000


@app.cls(
    image=videochat_image,
    gpu="H100",
    scaledown_window=15 * MINUTES,
    timeout=10 * MINUTES,
    volumes={"/root/.cache/huggingface": hf_cache_vol},
)
@modal.concurrent(max_inputs=4)  # video inference is heavy, keep this low
class VideoChatModel:
    @modal.enter()
    def load_model(self):
        import torch
        from transformers import AutoModel, AutoTokenizer

        print(f"Loading model {MODEL_NAME}...")
        self.tokenizer = AutoTokenizer.from_pretrained(
            MODEL_NAME, trust_remote_code=True
        )
        self.model = (
            AutoModel.from_pretrained(
                MODEL_NAME,
                trust_remote_code=True,
            )
            .to(torch.bfloat16)
            .cuda()
        )
        self.generation_config = dict(
            do_sample=False,
            temperature=0.0,
            max_new_tokens=1024,
            top_p=0.1,
            num_beams=1,
            top_k=0,  # disables top_k sampling since do_sample=False
        )

        self.image_processor = self.model.get_vision_tower().image_processor
        print("Model loaded.")

    @modal.method()
    def infer(
        self,
        video_bytes: bytes,
        question: str,
        max_num_frames: int = 128,
    ) -> str:
        # Write bytes to temp file — model.chat() expects a file path
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            f.write(video_bytes)
            tmp_path = f.name

        try:
            output, _ = self.model.chat(
                video_path=tmp_path,
                tokenizer=self.tokenizer,
                user_prompt=question,
                return_history=True,
                max_num_frames=max_num_frames,
                generation_config=self.generation_config,
            )
        finally:
            os.unlink(tmp_path)

        return output

# --- FastAPI web endpoint (inline, no separate file needed) ---
@app.function(
    image=videochat_image,
    gpu="H100",
    scaledown_window=15 * MINUTES,
    timeout=10 * MINUTES,
    volumes={"/root/.cache/huggingface": hf_cache_vol},
)
@modal.concurrent(max_inputs=4)
@modal.web_server(port=API_PORT, startup_timeout=10 * MINUTES)
def serve():
    import subprocess
    # Write the FastAPI app inline to a temp file
    app_code = '''
import os, tempfile
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
import torch
from transformers import AutoModel, AutoTokenizer

MODEL_NAME = "OpenGVLab/VideoChat-Flash-Qwen2_5-7B_InternVideo2-1B"
app = FastAPI()

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
model = AutoModel.from_pretrained(MODEL_NAME, trust_remote_code=True).to(torch.bfloat16).cuda()
model.config.mm_llm_compress = False
generation_config = dict(do_sample=False, temperature=0.0, max_new_tokens=1024, top_p=0.1, num_beams=1)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/v1/video/chat")
async def video_chat(
    file: UploadFile = File(...),
    question: str = Form(default="Describe this video in detail."),
    max_num_frames: int = Form(default=128),
):
    video_bytes = await file.read()
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        f.write(video_bytes)
        tmp_path = f.name
    try:
        output, _ = model.chat(
            video_path=tmp_path,
            tokenizer=tokenizer,
            user_prompt=question,
            return_history=True,
            max_num_frames=max_num_frames,
            generation_config=generation_config,
        )
    finally:
        os.unlink(tmp_path)
    return JSONResponse({"response": output})
'''
    with open("/tmp/videochat_app.py", "w") as f:
        f.write(app_code)

    subprocess.Popen(
        "uvicorn videochat_app:app --host 0.0.0.0 --port 8000",
        shell=True,
        cwd="/tmp",
    )


@app.local_entrypoint()
def test(video_path: str = "./nnnn.mp4", question: str = "Describe this video."):
    with open(video_path, "rb") as f:
        video_bytes = f.read()
    
    model = VideoChatModel()
    result = model.infer.remote(
        video_bytes=video_bytes,
        question=question,
        max_num_frames=128,
    )
    print(result)
