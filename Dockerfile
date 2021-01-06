FROM python:3.7-slim 
COPY . /app 
WORKDIR /app 
RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install -r requirements.txt
EXPOSE 80
CMD python3 main.py 
