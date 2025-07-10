import os
import clip
import torch
from facenet_pytorch import MTCNN, InceptionResnetV1


# Load the model
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load('ViT-B/16', device)

mtcnn = MTCNN(image_size=96, margin=0, device=device)

# Create an inception resnet (in eval mode):
resnet = InceptionResnetV1(pretrained='vggface2', device=device).eval()
