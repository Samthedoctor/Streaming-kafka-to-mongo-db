# consumer.Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file for the consumer
COPY requirements.txt .

# Install any needed packages specified in requirements-consumer.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer script into the container at /app
COPY consumer.py .

# Run consumer.py when the container launches
CMD ["python", "consumer.py"]