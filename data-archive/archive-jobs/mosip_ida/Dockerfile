# Use an official Python runtime as the base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

COPY mosip_archive_ida.py /app

# Set the entrypoint command to run the Python script with the environment variable
CMD ["python3", "mosip_archive_ida.py", "--config", "/app/mosip_archive_ida.ini"]
