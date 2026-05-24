from PIL import Image
from transformers import AutoProcessor, BlipForConditionalGeneration

# This serves as a warmup for the model to load into memory
# It also validates that the model is working correctly
processor = AutoProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

# Use a dummy image instead of fetching from external network
image = Image.new("RGB", (224, 224), color="red")
text = "A picture of"

inputs = processor(images=image, text=text, return_tensors="pt")

output = model.generate(**inputs)
caption = processor.decode(output[0], skip_special_tokens=True)
print("Warmup complete. Dummy image caption:", caption)
