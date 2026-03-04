# videochat_app.py  (runs inside the container)

import base64
import io
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse

app = FastAPI(title="VideoChat-Flash API")

# lazy-loaded model
_model = None

def get_model():
    global _model
    if _model is None:
        import torch
        from transformers import AutoModel, AutoTokenizer
        MODEL_NAME = "OpenGVLab/VideoChat-Flash-Qwen2_5-7B_InternVideo2-1B"
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
        model = AutoModel.from_pretrained(MODEL_NAME, trust_remote_code=True).to(torch.bfloat16).cuda()
        _model = (model, tokenizer)
    return _model


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/v1/video/chat")
async def video_chat(
    file: UploadFile = File(...),
    question: str = Form(default="Describe this video in detail."),
    num_frames: int = Form(default=16),
):
    """
    POST a video file + question, get back a text response.
    """
    import numpy as np
    import os, tempfile, torch
    import torchvision.transforms as T
    from decord import VideoReader, cpu
    from PIL import Image
    from torchvision.transforms.functional import InterpolationMode

    model, tokenizer = get_model()
    image_processor = model.get_vision_tower().image_processor

    MEAN = (0.485, 0.456, 0.406)
    STD  = (0.229, 0.224, 0.225)
    sz   = image_processor.crop_size["height"]
    transform = T.Compose([
        T.Lambda(lambda img: img.convert("RGB") if img.mode != "RGB" else img),
        T.Resize((sz, sz), interpolation=InterpolationMode.BICUBIC),
        T.ToTensor(),
        T.Normalize(mean=MEAN, std=STD),
    ])

    video_bytes = await file.read()
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        f.write(video_bytes)
        tmp = f.name

    try:
        vr = VideoReader(tmp, ctx=cpu(0))
        indices = np.linspace(0, len(vr) - 1, num_frames, dtype=int)
        frames = vr.get_batch(indices).asnumpy()
        pixel_values = torch.stack([
            transform(Image.fromarray(fr)) for fr in frames
        ]).to(torch.bfloat16).cuda()
    finally:
        os.unlink(tmp)

    prompt = f"<video>\n{question}"
    with torch.no_grad():
        response = model.chat(
            tokenizer,
            pixel_values,
            num_frames,
            prompt,
            dict(max_new_tokens=512, do_sample=False),
            img_context_token_id=tokenizer.convert_tokens_to_ids("<video>"),
        )

    return JSONResponse({"response": response, "model": "VideoChat-Flash-Qwen2_5-7B_InternVideo2-1B"})