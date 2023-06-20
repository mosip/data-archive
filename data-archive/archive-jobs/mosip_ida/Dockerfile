# Use a official Python runtime as the base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script and configuration files to the container
COPY mosip_archive_ida.py mosip_archive_ida.ini ./

# Run the Python script when the container launches
CMD ["python", "mosip_archive_ida.py"]
