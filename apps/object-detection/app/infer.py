import pickle
import numpy as np
import cv2

import torch
from torchvision.models import detection

class BboxDetector():

    def __init__(self, model_name="frcnn-mobilenet",
                 model_path=None,
                 labels_path="coco_classes.pickle",
                 confidence=0.5):

        # choices=["frcnn-resnet", "frcnn-mobilenet", "retinanet"]
        self.model_name = model_name
        self.model_path = model_path
        self.labels_path = labels_path
        self.confidence = confidence

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # initialize a dictionary containing model name and its corresponding
        # torchvision function call
        models = {
            "frcnn-resnet": detection.fasterrcnn_resnet50_fpn,
            "frcnn-mobilenet": detection.fasterrcnn_mobilenet_v3_large_320_fpn,
            "retinanet": detection.retinanet_resnet50_fpn
        }

        # set the device we will be using to run the model
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # load the list of categories in the COCO dataset and then generate a
        # set of bounding box colors for each class
        try:
            self.classes = pickle.loads(open(self.labels_path, "rb").read())
        except:
            print("FATAL ERROR: Could not load labels file.", flush=True)
            exit(-1)

        np.random.seed(42) # To keep the colors consistent
        self.colors = np.random.uniform(0, 255, size=(len(self.classes), 3))

        # load the model and set it to evaluation mode
        self.model = models[self.model_name](pretrained=True, progress=True,
            num_classes=len(self.classes), pretrained_backbone=True).to(self.device)
        self.model.eval()


    def infer(self, image):
        # load the image from disk

        orig = image.copy()
        # convert the image from BGR to RGB channel ordering and change the
        # image from channels last to channels first ordering
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        image = image.transpose((2, 0, 1))
        # add the batch dimension, scale the raw pixel intensities to the
        # range [0, 1], and convert the image to a floating point tensor
        image = np.expand_dims(image, axis=0)
        image = image / 255.0
        image = torch.FloatTensor(image)
        # send the input to the device and pass the it through the network to
        # get the detections and predictions
        image = image.to(self.device)
        detections = self.model(image)[0]

        # loop over the detections
        for i in range(0, len(detections["boxes"])):
            # extract the confidence (i.e., probability) associated with the
            # prediction
            confidence = detections["scores"][i]
            # filter out weak detections by ensuring the confidence is
            # greater than the minimum confidence
            if confidence > self.confidence:
                # extract the index of the class label from the detections,
                # then compute the (x, y)-coordinates of the bounding box
                # for the object
                idx = int(detections["labels"][i])
                box = detections["boxes"][i].detach().cpu().numpy()
                (startX, startY, endX, endY) = box.astype("int")
                # display the prediction to our terminal
                label = "{}: {:.2f}%".format(self.classes[idx], confidence * 100)
                # print("[INFO] {}".format(label))
                # draw the bounding box and label on the image
                cv2.rectangle(orig, (startX, startY), (endX, endY),
                    self.colors[idx], 2)
                y = startY - 15 if startY - 15 > 15 else startY + 15
                cv2.putText(orig, label, (startX, y),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, self.colors[idx], 2)

        self.last_infer_img = orig

        self.detections = detections

        return detections

    def get_last_infer_img(self):
        return self.last_infer_img

    def save_last_infer_img(self, path):
        cv2.imwrite(path, self.last_infer_img)
