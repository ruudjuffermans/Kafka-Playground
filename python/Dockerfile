FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install virtualenv
RUN pip install --no-cache-dir virtualenv

# Create and activate virtual environment
# Using /env to avoid conflicts with any existing directories
RUN virtualenv /env
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

# Install any needed packages specified in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /usr/src/app
COPY app/ .

# Make port available to the world outside this container
# EXPOSE 5000

# Define environment variable
ENV NAME World
