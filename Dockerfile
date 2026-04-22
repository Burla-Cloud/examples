FROM pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime

RUN pip install --no-cache-dir \
      sentence-transformers==3.0.1 \
      datasets==2.21.0 \
      huggingface-hub==0.24.6

# Bake the model weights into the image so GPU workers don't race on
# HuggingFace downloads at job start.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-large-en-v1.5')"
