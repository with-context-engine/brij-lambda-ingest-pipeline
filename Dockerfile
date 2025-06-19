# 1) Use AWS’s official Python Lambda base
FROM public.ecr.aws/lambda/python:3.11

# 2) Install Poppler for pdf2image
RUN yum install -y poppler-utils

# 3) Switch into Lambda’s working dir
WORKDIR ${LAMBDA_TASK_ROOT}

# 4) Copy in your code from src/ingest_pipeline and the requirements
COPY src/ingest_pipeline/main.py   ./main.py
COPY requirements.txt              .

# 5) Install Python deps into the image
RUN pip install --no-cache-dir -r requirements.txt

# 6) Tell Lambda which handler to use (module.function)
CMD ["main.lambda_handler"]
