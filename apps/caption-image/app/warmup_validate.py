from PIL import Image
import requests
from transformers import AutoProcessor, BlipForConditionalGeneration


# This serves as a warmup for the model to load into memory
# It also validates that the model is working correctly
processor = AutoProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

url = "http://images.cocodataset.org/val2017/000000039769.jpg"
image = Image.open(requests.get(url, stream=True).raw)
text = "A picture of"

inputs = processor(images=image, text=text, return_tensors="pt")

output = model.generate(**inputs)
caption = processor.decode(output[0], skip_special_tokens=True)
print(caption)

assert "cat" in caption.lower(), f"{caption} does not contain 'cat'"
