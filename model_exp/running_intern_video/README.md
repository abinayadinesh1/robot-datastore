# to start
pip install modal 
modal setup 
modal run modal_videochat_inference.py
# to deploy, run 
modal deploy modal_videochat_inference.py

# to infer, run 
curl -X POST "https://abinayadinesh1--videochat-flash-inference-serve.modal.run/v1/video/chat" 
    -F "file=@nnnn.mp4"   
    -F "question=Give a detailed description of what goes on in this video."   
    -F "num_frames=128"


### all done! 