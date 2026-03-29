from rlm import RLM
from rlm.video import create_video_sub_agent_spec, create_image_sub_agent_spec

video_spec = create_video_sub_agent_spec(
    robot_id="reachy-003",
    modal_image_url="http://your-image-modal-endpoint",
)
image_spec = create_image_sub_agent_spec(modal_image_url="http://your-image-modal-endpoint")

rlm = RLM(
    backend="openai",
    backend_kwargs={"model_name": "gpt-4o"},
    sub_agent_specs={"video": video_spec, "image": image_spec},
)

result = rlm.completion(
    prompt=video_metadata_dict,
    root_prompt="When does the robot pick up the red block?",
)
