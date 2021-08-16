#Create a base image.
#FROM python:3.8 AS base
FROM continuumio/miniconda3

#SHELL [ "/bin/bash", "--login", "-c" ]

#Set work directory
WORKDIR /usr/src

#Install PIP first
#RUN set -xe \
#    && apt-get update \
#    && apt-get install python-pip

#Update pip
RUN pip install --upgrade pip

#Install dependencies using a requirements file
COPY requirements.txt environment.yml .
RUN pip install --no-cache-dir -r requirements.txt

#COPY environment.yml .
RUN conda env create -f environment.yml

COPY . .
