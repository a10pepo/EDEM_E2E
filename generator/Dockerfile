FROM python:3.8-slim-buster

RUN mkdir main_folder

COPY requirements.txt /main_folder/
COPY main.py /main_folder/

WORKDIR /main_folder

RUN pip install -r requirements.txt

CMD ["python", "main.py"]


