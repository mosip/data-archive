# Use an official Python runtime as the base image
FROM python:3.9
# Set the working directory in the container
WORKDIR /app
# Copy the requirements file to the working directory
COPY requirements.txt .
# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip
# Create a virtual environment
RUN python -m venv /venv
# Activate the virtual environment
ENV PATH="/venv/bin:$PATH"
# Install the Python dependencies within the virtual environment
RUN pip install --no-cache-dir -r requirements.txt
# Copy the script and the configuration file to the container
COPY mosip_archive_ida.py /app/mosip_archive_ida.py
COPY mosip_archive_ida.ini /app/mosip_archive_ida.ini

# Make the script executable
RUN chmod +x mosip_archive_ida.py

# Run the script when the container launches
CMD ["python3", "mosip_archive_ida.py"]

